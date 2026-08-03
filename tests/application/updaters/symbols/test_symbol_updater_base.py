"""Tests for symbol updater base class."""

import json
from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import Symbol as _Sym
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import _repository_cache
from snapper.data.repository import close_and_insert_sync
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import SymbolAliasUpdateData


def _lookup_spid(session: Session, native_symbol: str) -> str:
    """Look up symbol public_id by native_symbol."""
    return session.execute(
        select(_Sym.public_id).where(_Sym.symbol_public_id == native_symbol)
    ).scalar_one()


@dataclass
class DummySettings:
    """Mock settings for symbol updater testing."""

    db_url: str
    zmq_broker_xsub: str
    zmq_broker_xpub: str
    master_password: str


class DummyExchangeClient:
    """Mock exchange client for symbol subscription."""

    def __init__(self, symbols: list[dict[str, Any]]):
        """Initialize the instance."""
        self._symbols = symbols
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        """Mark client as connected."""
        self.connected = True

    async def disconnect(self) -> None:
        """Mark client as disconnected."""
        self.disconnected = True

    async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
        """Yield configured symbol payloads."""
        for payload in self._symbols:
            yield payload


class DummySymbolUpdater(SymbolUpdaterService[Any]):
    """Concrete implementation of SymbolUpdaterService for testing."""

    def __init__(self, update_threshold_hours: int, *, force: bool = False) -> None:
        """Initialize the instance."""
        super().__init__(update_threshold_hours, force=force)
        self._client_symbols: list[dict[str, Any]] = []
        self.updated_payloads: list[list[dict[str, Any]]] = []

    def set_client_symbols(self, symbols: list[dict[str, Any]]) -> None:
        """Configure symbols to return from mock client."""
        self._client_symbols = symbols

    def _create_exchange_client(self) -> DummyExchangeClient:
        return DummyExchangeClient(self._client_symbols)

    def _get_setting_key(self) -> str:
        return "test_symbols_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        self.updated_payloads.append(symbols)

    def get_last_update_timestamp_public(self) -> datetime | None:
        """Expose _get_last_update_timestamp for testing."""
        return self._get_last_update_timestamp()

    async def set_last_update_timestamp_public(self, timestamp: datetime) -> None:
        """Expose _set_last_update_timestamp for testing."""
        await self._set_last_update_timestamp(timestamp)

    async def fetch_symbols_public(self, client: DummyExchangeClient) -> list[dict[str, Any]]:
        """Expose _fetch_symbols for testing."""
        return await self._fetch_symbols(client)

    def setup_zmq_public(self) -> None:
        """Expose _setup_zmq for testing."""
        self._setup_zmq()

    def cleanup_zmq_public(self) -> None:
        """Expose _cleanup_zmq for testing."""
        self._cleanup_zmq()


class StubSocket:
    """Mock ZMQ socket for testing pub/sub."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.sent_multipart: list[tuple[str, bytes]] = []
        self.closed = False
        self.connected_to: str | None = None

    async def send_multipart(self, topic: str, payload: bytes) -> None:
        """Record sent multipart message."""
        self.sent_multipart.append((topic, payload))

    def close(self) -> None:
        """Mark socket as closed."""
        self.closed = True

    def setsockopt(self, _option: int, _value: int) -> None:
        """No-op socket option setting."""
        pass

    def connect(self, endpoint: str) -> None:
        """Record connection endpoint."""
        self.connected_to = endpoint


class StubContext:
    """Mock ZMQ context for testing."""

    def __init__(self, socket: StubSocket) -> None:
        """Initialize the instance."""
        self.socket_instance = socket
        self.terminated = False
        self.socket_calls: list[int] = []

    def socket(self, socket_type: int) -> StubSocket:
        """Create and return socket instance."""
        self.socket_calls.append(socket_type)
        return self.socket_instance

    def term(self) -> None:
        """Mark context as terminated."""
        self.terminated = True


@pytest.fixture
def updater_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Callable[[int, bool], DummySymbolUpdater]:
    """Provide factory function for creating test updater instances."""
    created_repositories: list[DatabaseRepository] = []
    created_updaters: list[DummySymbolUpdater] = []

    def factory(update_threshold_hours: int, force: bool = False) -> DummySymbolUpdater:
        db_path = tmp_path / f"symbols_{len(created_repositories)}.sqlite"
        sync_url = f"sqlite:///{db_path}"
        async_url = f"sqlite+aiosqlite:///{db_path}"
        settings = DummySettings(
            db_url=async_url,
            zmq_broker_xsub="inproc://xsub",
            zmq_broker_xpub="inproc://xpub",
            master_password="master",
        )
        monkeypatch.setattr(
            "snapper.application.updaters.symbols.base.get_settings",
            lambda settings=settings: settings,
        )
        updater = DummySymbolUpdater(update_threshold_hours, force=force)
        repository = DatabaseRepository(sync_url)
        repository.create_all()
        updater.repository = repository
        created_updaters.append(updater)
        created_repositories.append(repository)
        return updater

    def cleanup() -> None:
        for updater in created_updaters:
            SymbolUpdaterService._cleanup_zmq(updater)
        for repository in created_repositories:
            repository.engine.dispose()
        _repository_cache.clear()

    request.addfinalizer(cleanup)
    return factory


def test_get_last_update_timestamp_returns_none_when_missing(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify get_last_update_timestamp returns None when no setting.

    Given: Updater with empty settings database,
    When: get_last_update_timestamp called,
    Then: None returned.
    """
    updater = updater_factory(3, False)
    assert updater.get_last_update_timestamp_public() is None


def test_get_last_update_timestamp_returns_none_without_repository(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify an uninitialized repository makes the timestamp unavailable.

    Given: Updater whose repository has not been initialized,
    When: get_last_update_timestamp is called,
    Then: None is returned.
    """
    updater = updater_factory(3, False)
    updater.repository = None
    assert updater.get_last_update_timestamp_public() is None


@pytest.mark.asyncio
async def test_set_last_update_timestamp_creates_setting(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify set_last_update_timestamp creates new setting.

    Given: Updater with empty settings database,
    When: set_last_update_timestamp called with timestamp,
    Then: Setting created with ISO format value.
    """
    updater = updater_factory(3, False)
    timestamp = datetime(2024, 1, 2, tzinfo=UTC)
    await updater.set_last_update_timestamp_public(timestamp)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        setting = session.execute(
            select(Setting).where(Setting.key == "test_symbols_last_update")
        ).scalar_one()
    assert setting.value == timestamp.isoformat()
    assert setting.timestamp == timestamp


def test_get_last_update_timestamp_handles_invalid_value(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify get_last_update_timestamp returns None for invalid value.

    Given: Setting with non-timestamp string value,
    When: get_last_update_timestamp called,
    Then: None returned.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Setting(
                key="test_symbols_last_update",
                value="not-a-timestamp",
                category="system",
                description="Invalid value for testing",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    assert updater.get_last_update_timestamp_public() is None


def test_should_update_returns_true_when_force_enabled(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns True when force enabled.

    Given: Updater with force=True,
    When: should_update called,
    Then: True returned regardless of timestamp.
    """
    updater = updater_factory(3, True)
    assert updater.should_update() is True


@pytest.mark.asyncio
async def test_should_update_returns_false_when_recently_updated(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns False when recently updated.

    Given: Updater with recent timestamp within threshold,
    When: should_update called,
    Then: False returned.
    """
    updater = updater_factory(5, False)
    recent = datetime.now(UTC)
    await updater.set_last_update_timestamp_public(recent)
    assert updater.should_update() is False


@pytest.mark.asyncio
async def test_should_update_returns_true_when_threshold_elapsed(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns True when threshold elapsed.

    Given: Updater with timestamp older than threshold,
    When: should_update called,
    Then: True returned.
    """
    updater = updater_factory(1, False)
    outdated = datetime.now(UTC) - timedelta(hours=2)
    await updater.set_last_update_timestamp_public(outdated)
    assert updater.should_update() is True


def test_should_update_returns_true_when_never_updated(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns True when never updated.

    Given: Updater with no prior update timestamp,
    When: should_update called,
    Then: True returned.
    """
    updater = updater_factory(3, False)
    assert updater.should_update() is True


@pytest.mark.asyncio
async def test_fetch_symbols_collects_async_generator(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _fetch_symbols collects all items from async generator.

    Given: Exchange client yielding two symbols,
    When: _fetch_symbols called,
    Then: List with both symbols returned.
    """
    updater = updater_factory(1, False)
    client = DummyExchangeClient(
        [
            {"symbol": "BTC-USD"},
            {"symbol": "ETH-USD"},
        ]
    )
    symbols = await updater.fetch_symbols_public(client)
    assert symbols == [{"symbol": "BTC-USD"}, {"symbol": "ETH-USD"}]


@pytest.mark.asyncio
async def test_setup_and_cleanup_zmq_manage_resources(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify ZMQ setup creates context/socket and cleanup releases them.

    Given: Updater with mocked ZMQ context,
    When: _setup_zmq then _cleanup_zmq called,
    Then: Socket connected, then closed and context terminated.
    """
    updater = updater_factory(2, False)
    socket = StubSocket()

    class ContextFactory:
        def __init__(self) -> None:
            self.created: StubContext | None = None

        def __call__(self) -> StubContext:
            self.created = StubContext(socket)
            return self.created

    factory = ContextFactory()
    monkeypatch.setattr("snapper.application.updaters.symbols.base.zmq.asyncio.Context", factory)
    updater.setup_zmq_public()
    assert factory.created is not None
    assert updater.context is not None
    assert updater.msg_publisher is not None
    assert socket.connected_to == updater.settings.zmq_broker_xsub
    updater.cleanup_zmq_public()
    assert socket.closed is True
    assert factory.created.terminated is True
    assert updater.msg_publisher is None
    assert updater.context is None


@pytest.mark.asyncio
async def test_broadcast_cache_invalidation_uses_socket(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify broadcast_cache_invalidation sends message via socket.

    Given: Updater with mocked publisher socket,
    When: broadcast_cache_invalidation called,
    Then: Message sent with topic and invalidation payload.
    """
    updater = updater_factory(2, False)
    mock_msg_pub = MagicMock()
    mock_msg_pub.send = AsyncMock()
    mock_msg_pub.tracker = SequenceTracker()
    mock_msg_pub.session_id = mock_msg_pub.tracker.session_id

    def fake_setup() -> None:
        updater.msg_publisher = mock_msg_pub

    monkeypatch.setattr(updater, "_setup_zmq", fake_setup)
    await updater.broadcast_cache_invalidation()
    mock_msg_pub.send.assert_called_once()
    called_data = mock_msg_pub.send.call_args.args[1]
    assert called_data.event == "symbol_aliases_updated"
    assert called_data.action == "clear_cache"
    assert called_data.timestamp is not None


@pytest.mark.asyncio
async def test_broadcast_cache_invalidation_skips_setup_when_publisher_exists(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify broadcast_cache_invalidation skips setup when publisher exists.

    Given: Updater with existing publisher socket,
    When: broadcast_cache_invalidation called,
    Then: Setup not called, message still sent.
    """
    updater = updater_factory(1, False)
    mock_msg_pub = MagicMock()
    mock_msg_pub.send = AsyncMock()
    mock_msg_pub.tracker = SequenceTracker()
    mock_msg_pub.session_id = mock_msg_pub.tracker.session_id
    setup_called = False

    def fake_setup() -> None:
        nonlocal setup_called
        setup_called = True

    updater.msg_publisher = mock_msg_pub
    monkeypatch.setattr(updater, "_setup_zmq", fake_setup)
    await updater.broadcast_cache_invalidation()
    assert setup_called is False
    mock_msg_pub.send.assert_called_once()
    called_data = mock_msg_pub.send.call_args.args[1]
    assert called_data.event == "symbol_aliases_updated"


@pytest.mark.asyncio
async def test_broadcast_cache_invalidation_stamped_envelope(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify broadcast_cache_invalidation publishes a fully-stamped ZMQ envelope.

    Given: Updater with a real MessagePublisher backed by a mock ValidatedPublisher,
    When: broadcast_cache_invalidation called,
    Then: Payload sent over ZMQ carries non-empty session_id, sequence_id >= 1,
          and correct SymbolAliasUpdateData fields.
    """
    updater = updater_factory(1, False)
    mock_vp = MagicMock()
    mock_vp.send_multipart = AsyncMock()
    tracker = SequenceTracker()
    updater.msg_publisher = MessagePublisher(mock_vp, tracker)
    await updater.broadcast_cache_invalidation()
    mock_vp.send_multipart.assert_called_once()
    call_args = mock_vp.send_multipart.call_args
    topic: str = call_args.args[0]
    raw: bytes = call_args.args[1]
    assert topic == "system.symbol_aliases"
    data = json.loads(raw.decode())
    assert data["type"] == "symbol_alias_update"
    assert data["event"] == "symbol_aliases_updated"
    assert data["session_id"] == tracker.session_id
    assert data["session_id"] != ""
    assert data["sequence_id"] >= 1
    stamped = SymbolAliasUpdateData.model_validate_json(raw)
    assert isinstance(stamped, SymbolAliasUpdateData)


@pytest.mark.asyncio
async def test_cleanup_safe_when_no_resources(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _cleanup_zmq is safe when no resources allocated.

    Given: Updater with no ZMQ resources,
    When: _cleanup_zmq called,
    Then: No error and attributes remain None.
    """
    updater = updater_factory(1, False)
    updater.cleanup_zmq_public()
    assert updater.context is None
    assert updater.msg_publisher is None


@pytest.mark.asyncio
async def test_start_skips_update_when_not_needed(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start skips update when threshold not elapsed.

    Given: Updater with recent timestamp,
    When: start called,
    Then: No update performed.
    """
    updater = updater_factory(5, False)

    async def mock_get_settings_service(*args: Any) -> Any:
        return None

    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_service",
        mock_get_settings_service,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_with_service",
        lambda _: updater.settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.set_log_context",
        lambda _: None,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.DatabaseRepository",
        lambda *args: updater.repository,
    )
    await updater.set_last_update_timestamp_public(datetime.now(UTC))
    await updater.start()
    assert len(updater.updated_payloads) == 0


@pytest.mark.asyncio
async def test_start_performs_full_update_workflow(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start performs complete update workflow.

    Given: Updater with force=True and test symbols,
    When: start called,
    Then: ZMQ setup, fetch, update, broadcast, cleanup all executed.
    """
    updater = updater_factory(1, True)
    test_symbols = [{"symbol": "BTC-USD"}, {"symbol": "ETH-USD"}]
    updater.set_client_symbols(test_symbols)
    zmq_setup_called = False
    zmq_cleanup_called = False

    def mock_setup_zmq() -> None:
        nonlocal zmq_setup_called
        zmq_setup_called = True

    def mock_cleanup_zmq() -> None:
        nonlocal zmq_cleanup_called
        zmq_cleanup_called = True

    async def mock_get_settings_service(*args: Any) -> Any:
        return None

    repo_ref: DatabaseRepository | None = None

    def mock_repository_factory(*args: Any) -> DatabaseRepository:
        nonlocal repo_ref
        repo_ref = updater.repository
        return updater.repository

    monkeypatch.setattr(updater, "_setup_zmq", mock_setup_zmq)
    monkeypatch.setattr(updater, "_cleanup_zmq", mock_cleanup_zmq)
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_service",
        mock_get_settings_service,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_with_service",
        lambda _: updater.settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.set_log_context",
        lambda _: None,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.DatabaseRepository",
        mock_repository_factory,
    )
    broadcast_called = False

    async def mock_broadcast() -> None:
        nonlocal broadcast_called
        broadcast_called = True

    monkeypatch.setattr(updater, "broadcast_cache_invalidation", mock_broadcast)
    await updater.start()
    assert zmq_setup_called, "ZMQ setup should be called"
    assert len(updater.updated_payloads) == 1, "Database update should be called once"
    assert updater.updated_payloads[0] == test_symbols
    assert broadcast_called, "Cache invalidation broadcast should be called"
    assert zmq_cleanup_called, "ZMQ cleanup should be called"
    assert repo_ref is not None
    with repo_ref.get_session() as session:
        assert isinstance(session, Session)
        setting = session.execute(
            select(Setting).where(Setting.key == "test_symbols_last_update")
        ).scalar_one_or_none()
    assert setting is not None
    last_update = datetime.fromisoformat(setting.value.replace("Z", "+00:00"))
    assert (datetime.now(UTC) - last_update).total_seconds() < 5


@pytest.mark.asyncio
async def test_start_cleans_up_on_exception(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start cleans up resources on exception.

    Given: Client that raises RuntimeError on subscribe,
    When: start called,
    Then: Exception raised, ZMQ and client cleanup still executed.
    """
    updater = updater_factory(1, True)
    zmq_cleanup_called = False
    client_disconnected = False

    def mock_cleanup_zmq() -> None:
        nonlocal zmq_cleanup_called
        zmq_cleanup_called = True

    class FailingClient:
        async def connect(self) -> None:
            """Intentionally empty async stub for testing."""
            pass

        async def disconnect(self) -> None:
            nonlocal client_disconnected
            client_disconnected = True

        async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
            raise RuntimeError("Test exception")
            yield

    def create_failing_client() -> FailingClient:
        return FailingClient()

    async def mock_get_settings_service(*args: Any) -> Any:
        return None

    monkeypatch.setattr(updater, "_create_exchange_client", create_failing_client)
    monkeypatch.setattr(updater, "_cleanup_zmq", mock_cleanup_zmq)
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_service",
        mock_get_settings_service,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_with_service",
        lambda _: updater.settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.set_log_context",
        lambda _: None,
    )
    with pytest.raises(RuntimeError, match="Test exception"):
        await updater.start()
    assert zmq_cleanup_called, "ZMQ cleanup should be called even on exception"
    assert client_disconnected, "Client disconnect should be called even on exception"


@pytest.mark.asyncio
async def test_start_handles_repository_initialization_failure(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start handles repository initialization failure.

    Given: Repository factory that raises RuntimeError,
    When: start called,
    Then: RuntimeError propagated.
    """
    updater = updater_factory(1, False)
    updater.repository = None

    async def mock_get_settings_service(*args: Any) -> Any:
        return None

    def failing_repository(*args: Any) -> DatabaseRepository:
        raise RuntimeError("init failure")

    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.DatabaseRepository",
        failing_repository,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_service",
        mock_get_settings_service,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_settings_with_service",
        lambda _: updater.settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.set_log_context",
        lambda _: None,
    )
    with pytest.raises(RuntimeError, match="init failure"):
        await updater.start()
    assert updater.repository is None


@pytest.mark.asyncio
async def test_setup_zmq_skips_when_already_setup(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _setup_zmq skips when already initialized.

    Given: Updater with existing ZMQ context,
    When: _setup_zmq called again,
    Then: Context factory not called.
    """
    updater = updater_factory(1, False)
    socket = StubSocket()

    class ContextFactory:
        def __init__(self) -> None:
            self.call_count = 0

        def __call__(self) -> StubContext:
            self.call_count += 1
            return StubContext(socket)

    factory = ContextFactory()
    monkeypatch.setattr("snapper.application.updaters.symbols.base.zmq.asyncio.Context", factory)
    updater.setup_zmq_public()
    assert factory.call_count == 1
    updater.setup_zmq_public()
    assert factory.call_count == 1, "Context should not be created again"


@pytest.mark.asyncio
async def test_broadcast_cache_invalidation_handles_missing_publisher(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify broadcast_cache_invalidation handles missing publisher.

    Given: Setup that fails to create publisher,
    When: broadcast_cache_invalidation called,
    Then: No error raised.
    """
    updater = updater_factory(1, False)

    def mock_setup_fails() -> None:
        updater.msg_publisher = None

    monkeypatch.setattr(updater, "_setup_zmq", mock_setup_fails)
    await updater.broadcast_cache_invalidation()


@pytest.mark.asyncio
async def test_set_last_update_timestamp_updates_existing(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify set_last_update_timestamp updates existing setting.

    Given: Updater with existing timestamp setting,
    When: set_last_update_timestamp called with new timestamp,
    Then: Setting updated in place, no duplicates.
    """
    updater = updater_factory(3, False)
    first_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    second_timestamp = datetime(2024, 1, 2, tzinfo=UTC)
    await updater.set_last_update_timestamp_public(first_timestamp)
    await updater.set_last_update_timestamp_public(second_timestamp)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        settings = list(
            session.execute(
                select(Setting).where(Setting.key == "test_symbols_last_update")
            ).scalars()
        )
    active_settings = [s for s in settings if s.value == second_timestamp.isoformat()]
    assert len(active_settings) == 1


@pytest.mark.asyncio
async def test_set_last_update_timestamp_raises_on_repository_error(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify set_last_update_timestamp propagates repository errors.

    Given: get_repository raises ValueError,
    When: set_last_update_timestamp called,
    Then: ValueError propagated.
    """
    updater = updater_factory(3, False)

    def broken_get_repo(_url: str) -> None:
        raise ValueError("failed")

    monkeypatch.setattr(
        "snapper.application.updaters.symbols.base.get_repository",
        broken_get_repo,
    )
    update_timestamp = datetime.now(UTC)
    with pytest.raises(ValueError, match="failed"):
        await updater.set_last_update_timestamp_public(update_timestamp)


def test_get_last_update_timestamp_returns_none_for_null_value(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify get_last_update_timestamp returns None for 'null' value.

    Given: Setting with literal 'null' string value,
    When: get_last_update_timestamp called,
    Then: None returned.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Setting(
                key="test_symbols_last_update",
                value="null",
                category="system",
                description="Null value for testing",
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    assert updater.get_last_update_timestamp_public() is None


def test_get_last_update_timestamp_returns_none_for_empty_value(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify get_last_update_timestamp returns None for empty value.

    Given: Setting with empty string value,
    When: get_last_update_timestamp called,
    Then: None returned.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Setting(
                key="test_symbols_last_update",
                value="",
                category="system",
                description="Empty value for testing",
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    assert updater.get_last_update_timestamp_public() is None


def test_upsert_symbol_creates_new_entry(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_symbol creates new Symbol and Symbol when none exists.

    Given: Empty database,
    When: _upsert_symbol called with new native_symbol,
    Then: New Symbol and Symbol rows created and True returned.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    now = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "BTC", "USD", "crypto", now, session_id="", sequence_id=0
        )
        session.commit()
    assert isinstance(result, str)
    assert len(result) == 36
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        version = session.execute(
            select(Symbol).where(Symbol.native_symbol == "BTC-USD")
        ).scalar_one()
    assert version.base == "BTC"
    assert version.quote == "USD"
    assert version.asset_type == "crypto"


def test_upsert_symbol_updates_base_currency(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_symbol creates new version via close+insert when base changes.

    Given: Existing Symbol + Symbol with base=BTC,
    When: _upsert_symbol called with base=XBT,
    Then: Old version closed, new version inserted with base=XBT.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "XBT", "USD", "crypto", update_time, session_id="", sequence_id=0
        )
        session.commit()
    assert isinstance(result, str)
    assert len(result) == 36
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        active = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == "BTC-USD",
                Symbol.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert active.base == "XBT"
    assert active.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_symbol_updates_quote_currency(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_symbol creates new version via close+insert when quote changes.

    Given: Existing Symbol + Symbol with quote=USD,
    When: _upsert_symbol called with quote=USDT,
    Then: Old version closed, new version inserted with quote=USDT.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "BTC", "USDT", "crypto", update_time, session_id="", sequence_id=0
        )
        session.commit()
    assert isinstance(result, str)
    assert len(result) == 36
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        active = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == "BTC-USD",
                Symbol.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert active.quote == "USDT"
    assert active.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_symbol_updates_asset_type(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_symbol creates new version via close+insert when asset_type changes.

    Given: Existing Symbol + Symbol with asset_type=crypto,
    When: _upsert_symbol called with asset_type=forex,
    Then: Old version closed, new version inserted with asset_type=forex.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="EUR-USD",
                base="EUR",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_symbol(
            session, "EUR-USD", "EUR", "USD", "forex", update_time, session_id="", sequence_id=0
        )
        session.commit()
    assert isinstance(result, str)
    assert len(result) == 36
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        active = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == "EUR-USD",
                Symbol.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert active.asset_type == "forex"
    assert active.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_symbol_preserves_timestamp_when_unchanged(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_symbol preserves version when no fields changed.

    Given: Existing Symbol + Symbol with identical values,
    When: _upsert_symbol called with same values,
    Then: No new version created, original timestamp preserved.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "BTC", "USD", "crypto", update_time, session_id="", sequence_id=0
        )
        session.commit()
    assert isinstance(result, str)
    assert len(result) == 36
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        active = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == "BTC-USD",
                Symbol.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert active.timestamp.replace(tzinfo=None) == original_time.replace(tzinfo=None)


def _seed_catalog(updater: DummySymbolUpdater, native_symbol: str, now: datetime) -> str:
    """Insert Symbol row and return its public_id for capability tests."""
    assert updater.repository is not None
    parts = native_symbol.split("-", maxsplit=1)
    base = parts[0]
    quote = parts[1] if len(parts) > 1 else None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        sym = Symbol(
            native_symbol=native_symbol,
            base=base,
            quote=quote,
            asset_type="crypto",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        session.add(sym)
        session.flush()
        spid = sym.public_id
        session.commit()
    return spid


def test_upsert_capability_creates_new_entry(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability creates new row when none exists.

    Given: Empty capability table with parent catalog row,
    When: _upsert_capability called,
    Then: Returns 'created' and row exists with correct values.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    now = datetime(2024, 6, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", now)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "kraken_updater",
            "Ticker list",
            now,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "created"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            )
        ).scalar_one()
    assert cap.can_market_data is True
    assert cap.can_trade is True
    assert cap.source == "kraken_updater"
    assert cap.reason == "Ticker list"


def test_upsert_capability_updates_can_trade(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability updates can_trade when changed.

    Given: Existing capability with can_trade=False,
    When: _upsert_capability called with can_trade=True,
    Then: Returns 'updated' and can_trade is True.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=_spid,
                exchange="kraken",
                can_market_data=True,
                can_trade=False,
                source="seed",
                reason=None,
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "seed",
            None,
            update_time,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "updated"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert cap.can_trade is True
    assert cap.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_capability_updates_can_market_data(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability updates can_market_data when changed.

    Given: Existing capability with can_market_data=False,
    When: _upsert_capability called with can_market_data=True,
    Then: Returns 'updated' and can_market_data is True.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "ETH-USD", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=_spid,
                exchange="kraken",
                can_market_data=False,
                can_trade=True,
                source="seed",
                reason=None,
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "seed",
            None,
            update_time,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "updated"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert cap.can_market_data is True
    assert cap.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_capability_updates_source(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability updates source when changed.

    Given: Existing capability with source='seed',
    When: _upsert_capability called with source='kraken_updater',
    Then: Returns 'updated' and source is 'kraken_updater'.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=_spid,
                exchange="kraken",
                can_market_data=True,
                can_trade=True,
                source="seed",
                reason=None,
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "kraken_updater",
            None,
            update_time,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "updated"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert cap.source == "kraken_updater"
    assert cap.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_capability_updates_reason(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability updates reason when changed.

    Given: Existing capability with reason=None,
    When: _upsert_capability called with reason='WS-only',
    Then: Returns 'updated' and reason is 'WS-only'.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=_spid,
                exchange="kraken",
                can_market_data=True,
                can_trade=True,
                source="kraken_updater",
                reason=None,
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "kraken_updater",
            "WS-only",
            update_time,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "updated"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert cap.reason == "WS-only"
    assert cap.timestamp.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_capability_unchanged(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_capability returns 'unchanged' when all fields match.

    Given: Existing capability with identical values,
    When: _upsert_capability called with same values,
    Then: Returns 'unchanged' and updated_at preserved.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=_spid,
                exchange="kraken",
                can_market_data=True,
                can_trade=True,
                source="kraken_updater",
                reason="Ticker list",
                created_at=original_time,
                timestamp=original_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "kraken_updater",
            "Ticker list",
            update_time,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result == "unchanged"
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            )
        ).scalar_one()
    assert cap.timestamp.replace(tzinfo=None) == original_time.replace(tzinfo=None)


def _seed_capability(
    updater: DummySymbolUpdater,
    native_symbol: str,
    exchange: str,
    can_md: bool,
    can_trade: bool,
    source: str,
    now: datetime,
) -> None:
    """Seed a capability row (assumes catalog row already exists)."""
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        sym = session.execute(
            select(Symbol).where(Symbol.native_symbol == native_symbol)
        ).scalar_one()
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=sym.public_id,
                exchange=exchange,
                can_market_data=can_md,
                can_trade=can_trade,
                source=source,
                reason=None,
                created_at=now,
                timestamp=now,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()


def test_deactivate_stale_capabilities_deactivates_removed(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify stale capabilities are deactivated for removed symbols.

    Given: Two active capabilities for kraken (BTC-USD, ETH-USD),
    When: _deactivate_stale_capabilities called with only BTC-USD active,
    Then: ETH-USD deactivated (can_trade=False, can_market_data=False).
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    btc_spid = _seed_catalog(updater, "BTC-USD", now)
    eth_spid = _seed_catalog(updater, "ETH-USD", now)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", now)
    _seed_capability(updater, "ETH-USD", "kraken", True, True, "kraken_updater", now)
    deactivation_time = datetime(2024, 6, 2, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._deactivate_stale_capabilities(
            session,
            "kraken",
            {btc_spid},
            "kraken_updater",
            deactivation_time,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 1
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        btc_cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == btc_spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        eth_cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == eth_spid,
                SymbolExchangeCapability.exchange == "kraken",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert btc_cap.can_trade is True
    assert btc_cap.can_market_data is True
    assert eth_cap.can_trade is False
    assert eth_cap.can_market_data is False
    assert eth_cap.reason == "Delisted: not seen in updater run"


def test_deactivate_stale_capabilities_leaves_active_untouched(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify active capabilities are not modified.

    Given: Two active capabilities for kraken (BTC-USD, ETH-USD),
    When: _deactivate_stale_capabilities called with both active,
    Then: Zero deactivated, both remain active.
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    btc_spid = _seed_catalog(updater, "BTC-USD", now)
    eth_spid = _seed_catalog(updater, "ETH-USD", now)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", now)
    _seed_capability(updater, "ETH-USD", "kraken", True, True, "kraken_updater", now)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._deactivate_stale_capabilities(
            session,
            "kraken",
            {btc_spid, eth_spid},
            "kraken_updater",
            now,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 0


def test_deactivate_stale_skips_already_inactive(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify already-inactive capabilities are not counted.

    Given: One active and one already-inactive capability,
    When: _deactivate_stale_capabilities called with only active symbol,
    Then: Zero deactivated (inactive row not touched).
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    btc_spid = _seed_catalog(updater, "BTC-USD", now)
    _seed_catalog(updater, "ETH-USD", now)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", now)
    _seed_capability(updater, "ETH-USD", "kraken", False, False, "kraken_updater", now)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._deactivate_stale_capabilities(
            session,
            "kraken",
            {btc_spid},
            "kraken_updater",
            now,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 0


def test_reconcile_capabilities_proceeds_above_threshold(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify reconciliation proceeds when active ratio is above threshold.

    Given: Two active capabilities, one symbol still active (ratio 0.5),
    When: _reconcile_capabilities called with min_active_ratio=0.5,
    Then: Stale symbol deactivated.
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    btc_spid = _seed_catalog(updater, "BTC-USD", now)
    _seed_catalog(updater, "ETH-USD", now)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", now)
    _seed_capability(updater, "ETH-USD", "kraken", True, True, "kraken_updater", now)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._reconcile_capabilities(
            session,
            "kraken",
            {btc_spid},
            "kraken_updater",
            now,
            min_active_ratio=0.5,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 1


def test_reconcile_capabilities_skips_below_threshold(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify reconciliation skipped when active ratio is below threshold.

    Given: Three active capabilities, only one symbol still active (ratio 0.33),
    When: _reconcile_capabilities called with min_active_ratio=0.5,
    Then: Zero deactivated (safety threshold prevents mass deactivation).
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    spids: dict[str, str] = {}
    for sym in ("BTC-USD", "ETH-USD", "SOL-USD"):
        spids[sym] = _seed_catalog(updater, sym, now)
        _seed_capability(updater, sym, "kraken", True, True, "kraken_updater", now)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._reconcile_capabilities(
            session,
            "kraken",
            {spids["BTC-USD"]},
            "kraken_updater",
            now,
            min_active_ratio=0.5,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 0
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        caps = (
            session.execute(
                select(SymbolExchangeCapability).where(
                    SymbolExchangeCapability.exchange == "kraken",
                )
            )
            .scalars()
            .all()
        )
    assert all(cap.can_trade is True for cap in caps)


def test_reconcile_capabilities_empty_existing(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify reconciliation handles empty existing capabilities gracefully.

    Given: No existing capabilities for the exchange,
    When: _reconcile_capabilities called,
    Then: Zero deactivated (no division by zero, no error).
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    now = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._reconcile_capabilities(
            session,
            "kraken",
            {"BTC-USD"},
            "kraken_updater",
            now,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 0


def test_deactivate_stale_different_exchange_not_touched(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify deactivation only affects the target exchange.

    Given: Capabilities on both kraken and polygon for BTC-USD,
    When: _deactivate_stale_capabilities called for kraken with empty set,
    Then: Only kraken capability deactivated, polygon untouched.
    """
    updater = updater_factory(3, False)
    now = datetime(2024, 6, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", now)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", now)
    _seed_capability(updater, "BTC-USD", "polygon", True, False, "polygon_updater", now)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        count = SymbolUpdaterService._deactivate_stale_capabilities(
            session,
            "kraken",
            set(),
            "kraken_updater",
            now,
            session_id="",
            next_sequence_fn=lambda: 0,
        )
        session.commit()
    assert count == 1
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        polygon_cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "polygon",
            )
        ).scalar_one()
    assert polygon_cap.can_market_data is True


def test_close_and_insert_sync_creates_fresh_row_when_no_existing(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify close_and_insert_sync creates a fresh row when no active row exists.

    Given: Empty SymbolExchangeCapability table with catalog FK parent,
    When: close_and_insert_sync called with new values,
    Then: New row inserted with KNOWN_TO_MAX and provided timestamp.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    now = datetime(2024, 6, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", now)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        new_row = close_and_insert_sync(
            session=session,
            model=SymbolExchangeCapability,
            match_filters=[
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            ],
            new_values={
                "symbol_public_id": _spid,
                "exchange": "kraken",
                "can_market_data": True,
                "can_trade": True,
                "source": "test",
                "reason": None,
                "created_at": now,
                "session_id": "test-session",
                "sequence_id": 1,
            },
            bus_time=now,
        )
        session.commit()
    assert new_row.known_to == KNOWN_TO_MAX
    assert new_row.timestamp == now
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        cap = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            )
        ).scalar_one()
    assert cap.can_trade is True
    assert cap.source == "test"


def test_close_and_insert_sync_closes_existing_and_inserts_new(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify close_and_insert_sync closes existing row and inserts replacement.

    Given: Existing active SymbolExchangeCapability row,
    When: close_and_insert_sync called with updated values,
    Then: Old row closed (known_to=bus_time), new row active with same public_id.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    _spid = _seed_catalog(updater, "BTC-USD", original_time)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "seed", original_time)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        original = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            )
        ).scalar_one()
        original_public_id = original.public_id
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        close_and_insert_sync(
            session=session,
            model=SymbolExchangeCapability,
            match_filters=[
                SymbolExchangeCapability.symbol_public_id == _spid,
                SymbolExchangeCapability.exchange == "kraken",
            ],
            new_values={
                "symbol_public_id": _spid,
                "exchange": "kraken",
                "can_market_data": False,
                "can_trade": False,
                "source": "updated",
                "reason": "test update",
                "created_at": original_time,
                "session_id": "test-session",
                "sequence_id": 2,
            },
            bus_time=update_time,
        )
        session.commit()
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        all_caps = (
            session.execute(
                select(SymbolExchangeCapability).where(
                    SymbolExchangeCapability.symbol_public_id == _spid,
                    SymbolExchangeCapability.exchange == "kraken",
                )
            )
            .scalars()
            .all()
        )
    assert len(all_caps) == 2
    closed = [c for c in all_caps if c.known_to != KNOWN_TO_MAX]
    active = [c for c in all_caps if c.known_to == KNOWN_TO_MAX]
    assert len(closed) == 1
    assert len(active) == 1
    assert closed[0].known_to == update_time
    assert active[0].public_id == original_public_id
    assert active[0].source == "updated"
    assert active[0].can_trade is False


def _assert_contiguous_intervals(versions: list[Any]) -> None:
    """Assert temporal versions form contiguous non-overlapping half-open intervals.

    Sorts by timestamp and verifies prev.known_to == next.timestamp for each
    consecutive pair. Also verifies the last version has known_to == KNOWN_TO_MAX.

    Args:
        versions: List of ORM model instances with timestamp and known_to fields.
    """
    sorted_versions = sorted(versions, key=lambda v: v.timestamp)
    for i in range(len(sorted_versions) - 1):
        assert sorted_versions[i].known_to == sorted_versions[i + 1].timestamp
    if sorted_versions:
        assert sorted_versions[-1].known_to == KNOWN_TO_MAX


def test_real_updater_catalog_reingest_same_payload_is_noop(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify real _upsert_symbol with same payload creates no new version.

    Given: A Symbol + Symbol (base=BTC, quote=USD, asset_type=crypto)
        created via the real _upsert_symbol method at t1,
    When: _upsert_symbol is called again at t2 with identical payload,
    Then: Still only 1 Symbol row exists and its timestamp equals t1.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "BTC", "USD", "crypto", t1, session_id="", sequence_id=0
        )
        session.commit()

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        before = (
            session.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")).scalars().all()
        )
    assert len(before) == 1
    original_ts = before[0].timestamp

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        SymbolUpdaterService._upsert_symbol(
            session, "BTC-USD", "BTC", "USD", "crypto", t2, session_id="", sequence_id=0
        )
        session.commit()

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        after = (
            session.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")).scalars().all()
        )
    assert len(after) == 1
    assert after[0].timestamp.replace(tzinfo=None) == original_ts.replace(tzinfo=None)


def test_real_updater_alias_reingest_preserves_public_id_and_timestamp(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify real _upsert_alias with same exchange_symbol preserves public_id.

    Given: A Symbol + SymbolAlias (exchange_symbol='XBT/USD') created via
        the real _upsert_alias method at t1,
    When: _upsert_alias is called again at t2 with the same exchange_symbol,
    Then: The result is 'unchanged', the same public_id and timestamp are
        preserved, and there is still only 1 SymbolAlias row.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

    _spid = _seed_catalog(updater, "BTC-USD", t1)

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result1 = SymbolUpdaterService._upsert_alias(
            session, _spid, "kraken", "ws", "XBT/USD", t1, session_id="", sequence_id=0
        )
        session.commit()
    assert result1 == "created"

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        before = (
            session.execute(
                select(SymbolAlias).where(
                    SymbolAlias.symbol_public_id == _spid,
                    SymbolAlias.exchange == "kraken",
                    SymbolAlias.channel == "ws",
                )
            )
            .scalars()
            .all()
        )
    assert len(before) == 1
    original_public_id = before[0].public_id
    original_ts = before[0].timestamp

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result2 = SymbolUpdaterService._upsert_alias(
            session, _spid, "kraken", "ws", "XBT/USD", t2, session_id="", sequence_id=0
        )
        session.commit()
    assert result2 == "unchanged"

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        after = (
            session.execute(
                select(SymbolAlias).where(
                    SymbolAlias.symbol_public_id == _spid,
                    SymbolAlias.exchange == "kraken",
                    SymbolAlias.channel == "ws",
                )
            )
            .scalars()
            .all()
        )
    assert len(after) == 1
    assert after[0].public_id == original_public_id
    assert after[0].timestamp.replace(tzinfo=None) == original_ts.replace(tzinfo=None)


def test_real_updater_capability_change_closes_old_inserts_new(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify real _upsert_capability with changed can_trade closes old and inserts new.

    Given: A Symbol + SymbolExchangeCapability (can_trade=False) created via
        the real _upsert_capability method at t1,
    When: _upsert_capability is called at t2 with can_trade=True,
    Then: Two rows exist with the same public_id and contiguous intervals.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

    _spid = _seed_catalog(updater, "BTC-USD", t1)

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result1 = SymbolUpdaterService._upsert_capability(
            session, _spid, "kraken", True, False, "seed", None, t1, session_id="", sequence_id=0
        )
        session.commit()
    assert result1 == "created"

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result2 = SymbolUpdaterService._upsert_capability(
            session,
            _spid,
            "kraken",
            True,
            True,
            "kraken_updater",
            "Promoted",
            t2,
            session_id="",
            sequence_id=0,
        )
        session.commit()
    assert result2 == "updated"

    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        all_caps = (
            session.execute(
                select(SymbolExchangeCapability).where(
                    SymbolExchangeCapability.symbol_public_id == _spid,
                    SymbolExchangeCapability.exchange == "kraken",
                )
            )
            .scalars()
            .all()
        )
    assert len(all_caps) == 2
    public_ids = {c.public_id for c in all_caps}
    assert len(public_ids) == 1
    _assert_contiguous_intervals(all_caps)


def test_ensure_instrument_identity_returns_existing() -> None:
    """Verify _ensure_instrument_identity returns existing instrument public_id.

    Given: A mock session where scalar_one_or_none returns an existing instrument,
    When: _ensure_instrument_identity is called,
    Then: It returns the existing public_id without inserting.
    """
    existing = SimpleNamespace(public_id="existing-inst-pid")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = existing
    now = datetime.now(UTC)
    result = SymbolUpdaterService._ensure_instrument_identity(
        session, "sym-pid", "kraken", now, session_id="s", sequence_id=1
    )
    assert result == "existing-inst-pid"
    session.add.assert_not_called()


def _seed_alias(
    updater: DummySymbolUpdater,
    native_symbol: str,
    exchange: str,
    channel: str,
    alias_value: str,
    now: datetime,
) -> None:
    """Seed an active alias row (assumes catalog row already exists)."""
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        sym = session.execute(
            select(Symbol).where(Symbol.native_symbol == native_symbol)
        ).scalar_one()
        session.add(
            SymbolAlias(
                symbol_public_id=sym.public_id,
                exchange=exchange,
                channel=channel,
                exchange_symbol=alias_value,
                created_at=now,
                timestamp=now,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()


def _open_aliases_count(updater: DummySymbolUpdater, symbol_public_id: str, exchange: str) -> int:
    """Count active alias rows (known_to == KNOWN_TO_MAX) for a symbol on an exchange."""
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        rows = (
            session.execute(
                select(SymbolAlias).where(
                    SymbolAlias.symbol_public_id == symbol_public_id,
                    SymbolAlias.exchange == exchange,
                    SymbolAlias.known_to == KNOWN_TO_MAX,
                )
            )
            .scalars()
            .all()
        )
        return len(rows)


def test_reconcile_aliases_closes_when_capability_deactivated(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Spec — aliases close when current capability is False/False.

    Given: Two symbols on ``kraken`` — BTC-USD with capability
        ``can_market_data=True/can_trade=True`` and active WS alias
        ``XBT/USD``; ETH-USD with capability ``False/False`` (delisted)
        and an open WS alias ``ETH/USD``,
    When: ``_reconcile_aliases`` runs at a time ≥ the seeded
        timestamps,
    Then: Exactly ONE alias row closes (ETH-USD's WS alias) and the
        BTC-USD alias stays open. The closure is the canonical cleanup
        for the architectural rule that capabilities are the
        operational gate for symbol aliases.
    """
    updater = updater_factory(3, False)
    seed_time = datetime(2024, 6, 1, tzinfo=UTC)
    btc_spid = _seed_catalog(updater, "BTC-USD", seed_time)
    eth_spid = _seed_catalog(updater, "ETH-USD", seed_time)
    _seed_capability(updater, "BTC-USD", "kraken", True, True, "kraken_updater", seed_time)
    _seed_capability(updater, "ETH-USD", "kraken", False, False, "kraken_updater", seed_time)
    _seed_alias(updater, "BTC-USD", "kraken", "ws", "XBT/USD", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken", "ws", "ETH/USD", seed_time)
    reconcile_time = datetime(2024, 6, 2, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        closed = SymbolUpdaterService._reconcile_aliases(session, "kraken", reconcile_time)
        session.commit()
    assert closed == 1
    assert _open_aliases_count(updater, btc_spid, "kraken") == 1
    assert _open_aliases_count(updater, eth_spid, "kraken") == 0


def test_reconcile_aliases_leaves_alias_when_only_one_flag_false(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Spec — aliases stay open when only one capability flag is False.

    Given: ETH-USD on ``kraken`` with capability
        ``can_market_data=True/can_trade=False`` (market-data-only,
        e.g. a venue we read from but cannot route orders to) and an
        active WS alias,
    When: ``_reconcile_aliases`` runs,
    Then: Zero aliases close — the requirement is BOTH flags False
        (truly retired). This protects market-data-only and
        trade-only configurations from accidental alias closure.
    """
    updater = updater_factory(3, False)
    seed_time = datetime(2024, 6, 1, tzinfo=UTC)
    eth_spid = _seed_catalog(updater, "ETH-USD", seed_time)
    _seed_capability(updater, "ETH-USD", "kraken", True, False, "kraken_updater", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken", "ws", "ETH/USD", seed_time)
    reconcile_time = datetime(2024, 6, 2, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        closed = SymbolUpdaterService._reconcile_aliases(session, "kraken", reconcile_time)
        session.commit()
    assert closed == 0
    assert _open_aliases_count(updater, eth_spid, "kraken") == 1


def test_reconcile_aliases_is_idempotent(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Spec — a second invocation with no new capability changes closes nothing.

    Given: A symbol whose capability is False/False and whose alias
        was just closed by an earlier reconcile invocation,
    When: ``_reconcile_aliases`` runs a second time at a later
        timestamp,
    Then: Zero additional rows close — the closed alias's
        ``known_to < now`` filter excludes it from the second
        UPDATE. Steady-state guarantee for repeated symbol updater
        runs and CLI backfill re-invocations.
    """
    updater = updater_factory(3, False)
    seed_time = datetime(2024, 6, 1, tzinfo=UTC)
    eth_spid = _seed_catalog(updater, "ETH-USD", seed_time)
    _seed_capability(updater, "ETH-USD", "kraken", False, False, "kraken_updater", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken", "ws", "ETH/USD", seed_time)
    first_run = datetime(2024, 6, 2, tzinfo=UTC)
    second_run = datetime(2024, 6, 3, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        first_closed = SymbolUpdaterService._reconcile_aliases(session, "kraken", first_run)
        session.commit()
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        second_closed = SymbolUpdaterService._reconcile_aliases(session, "kraken", second_run)
        session.commit()
    assert first_closed == 1
    assert second_closed == 0
    assert _open_aliases_count(updater, eth_spid, "kraken") == 0


def test_reconcile_aliases_closes_all_channels_for_one_symbol(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Spec — every channel's alias closes when capability flips.

    Given: A symbol with capability False/False and TWO alias rows
        — one on the ``ws`` channel and one on the ``rest`` channel
        (mirror of how the kraken updater seeds both for trading
        pairs that need REST + WS routing),
    When: ``_reconcile_aliases`` runs,
    Then: BOTH alias rows close in a single UPDATE — the helper
        operates on ``symbol_public_id IN (deactivated)`` so every
        channel's alias for that symbol is targeted at once. Tests
        the WHERE clause's channel-independence.
    """
    updater = updater_factory(3, False)
    seed_time = datetime(2024, 6, 1, tzinfo=UTC)
    eth_spid = _seed_catalog(updater, "ETH-USD", seed_time)
    _seed_capability(updater, "ETH-USD", "kraken", False, False, "kraken_updater", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken", "ws", "ETH/USD", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken", "rest", "XETHZUSD", seed_time)
    reconcile_time = datetime(2024, 6, 2, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        closed = SymbolUpdaterService._reconcile_aliases(session, "kraken", reconcile_time)
        session.commit()
    assert closed == 2
    assert _open_aliases_count(updater, eth_spid, "kraken") == 0


def test_reconcile_aliases_does_not_touch_other_exchange(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Spec — exchange scoping prevents cross-exchange interference.

    Given: ETH-USD on BOTH ``kraken`` (capability False/False, open
        alias) and ``kraken_futures`` (capability False/False, open
        alias),
    When: ``_reconcile_aliases`` runs scoped to ``kraken_futures``,
    Then: Only the kraken_futures alias closes; the kraken alias
        stays open — the WHERE clause's ``exchange == ...`` is
        respected. This is the contract that lets the CLI backfill
        scope to one exchange without spilling into others.
    """
    updater = updater_factory(3, False)
    seed_time = datetime(2024, 6, 1, tzinfo=UTC)
    eth_spid = _seed_catalog(updater, "ETH-USD", seed_time)
    _seed_capability(updater, "ETH-USD", "kraken", False, False, "kraken_updater", seed_time)
    _seed_capability(
        updater, "ETH-USD", "kraken_futures", False, False, "kraken_futures_updater", seed_time
    )
    _seed_alias(updater, "ETH-USD", "kraken", "ws", "ETH/USD", seed_time)
    _seed_alias(updater, "ETH-USD", "kraken_futures", "ws", "PF_ETHUSD", seed_time)
    reconcile_time = datetime(2024, 6, 2, tzinfo=UTC)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        closed = SymbolUpdaterService._reconcile_aliases(session, "kraken_futures", reconcile_time)
        session.commit()
    assert closed == 1
    assert _open_aliases_count(updater, eth_spid, "kraken") == 1
    assert _open_aliases_count(updater, eth_spid, "kraken_futures") == 0
