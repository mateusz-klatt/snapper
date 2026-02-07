"""Tests for symbol updater base class."""

import json
from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.data.models import Setting
from snapper.data.models import SymbolCatalog
from snapper.data.repository import DatabaseRepository


@dataclass
class DummySettings:
    """Mock settings for symbol updater testing."""

    db_url: str
    zmq_broker_xsub: str
    zmq_broker_xpub: str
    master_password: str
    encryption_salt: str


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

    def set_last_update_timestamp_public(self, timestamp: datetime) -> None:
        """Expose _set_last_update_timestamp for testing."""
        self._set_last_update_timestamp(timestamp)

    async def fetch_symbols_public(self, client: DummyExchangeClient) -> list[dict[str, Any]]:
        """Expose _fetch_symbols for testing."""
        return await self._fetch_symbols(client)

    async def setup_zmq_public(self) -> None:
        """Expose _setup_zmq for testing."""
        await self._setup_zmq()

    async def cleanup_zmq_public(self) -> None:
        """Expose _cleanup_zmq for testing."""
        await self._cleanup_zmq()


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


@pytest.fixture()
def updater_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Callable[[int, bool], DummySymbolUpdater]:
    """Provide factory function for creating test updater instances."""
    created_repositories: list[DatabaseRepository] = []

    def factory(update_threshold_hours: int, force: bool = False) -> DummySymbolUpdater:
        db_path = tmp_path / f"symbols_{len(created_repositories)}.sqlite"
        settings = DummySettings(
            db_url=f"sqlite:///{db_path}",
            zmq_broker_xsub="inproc://xsub",
            zmq_broker_xpub="inproc://xpub",
            master_password="master",
            encryption_salt="salt",
        )
        monkeypatch.setattr(
            "snapper.application.updaters.symbols.base.get_settings",
            lambda settings=settings: settings,
        )
        updater = DummySymbolUpdater(update_threshold_hours, force=force)
        repository = DatabaseRepository(settings.db_url)
        repository.create_all()
        updater.repository = repository
        created_repositories.append(repository)
        return updater

    def cleanup() -> None:
        for repository in created_repositories:
            repository.engine.dispose()

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


def test_set_last_update_timestamp_creates_setting(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify set_last_update_timestamp creates new setting.

    Given: Updater with empty settings database,
    When: set_last_update_timestamp called with timestamp,
    Then: Setting created with ISO format value.
    """
    updater = updater_factory(3, False)
    timestamp = datetime(2024, 1, 2, tzinfo=UTC)
    updater.set_last_update_timestamp_public(timestamp)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        setting = session.execute(
            select(Setting).where(Setting.key == "test_symbols_last_update")
        ).scalar_one()
    assert setting.value == timestamp.isoformat()
    assert setting.updated_at == timestamp


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
                updated_at=datetime.now(UTC),
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


def test_should_update_returns_false_when_recently_updated(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns False when recently updated.

    Given: Updater with recent timestamp within threshold,
    When: should_update called,
    Then: False returned.
    """
    updater = updater_factory(5, False)
    recent = datetime.now(UTC)
    updater.set_last_update_timestamp_public(recent)
    assert updater.should_update() is False


def test_should_update_returns_true_when_threshold_elapsed(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify should_update returns True when threshold elapsed.

    Given: Updater with timestamp older than threshold,
    When: should_update called,
    Then: True returned.
    """
    updater = updater_factory(1, False)
    outdated = datetime.now(UTC) - timedelta(hours=2)
    updater.set_last_update_timestamp_public(outdated)
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


@pytest.mark.asyncio()
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


@pytest.mark.asyncio()
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
    await updater.setup_zmq_public()
    assert factory.created is not None
    assert updater.context is not None
    assert updater.publisher is not None
    assert socket.connected_to == updater.settings.zmq_broker_xsub
    await updater.cleanup_zmq_public()
    assert socket.closed is True
    assert factory.created.terminated is True
    assert updater.publisher is None
    assert updater.context is None


@pytest.mark.asyncio()
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
    socket = StubSocket()

    async def fake_setup() -> None:
        updater.publisher = cast(Any, socket)

    monkeypatch.setattr(updater, "_setup_zmq", fake_setup)
    await updater.broadcast_cache_invalidation()
    assert socket.sent_multipart, "Expected broadcast message to be sent"
    topic, payload = socket.sent_multipart[0]
    assert topic == "system.symbol_aliases"
    payload_data = json.loads(payload.decode("utf-8"))
    assert payload_data["event"] == "symbol_aliases_updated"
    assert payload_data["action"] == "clear_cache"
    assert "timestamp" in payload_data


@pytest.mark.asyncio()
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
    socket = StubSocket()
    setup_called = False

    async def fake_setup() -> None:
        nonlocal setup_called
        setup_called = True

    updater.publisher = cast(Any, socket)
    monkeypatch.setattr(updater, "_setup_zmq", fake_setup)
    await updater.broadcast_cache_invalidation()
    assert setup_called is False
    assert socket.sent_multipart, "Expected broadcast message when publisher present"
    topic, payload = socket.sent_multipart[0]
    assert topic == "system.symbol_aliases"
    payload_data = json.loads(payload.decode("utf-8"))
    assert payload_data["event"] == "symbol_aliases_updated"


@pytest.mark.asyncio()
async def test_cleanup_safe_when_no_resources(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _cleanup_zmq is safe when no resources allocated.

    Given: Updater with no ZMQ resources,
    When: _cleanup_zmq called,
    Then: No error and attributes remain None.
    """
    updater = updater_factory(1, False)
    await updater.cleanup_zmq_public()
    assert updater.context is None
    assert updater.publisher is None


@pytest.mark.asyncio()
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
    updater.set_last_update_timestamp_public(datetime.now(UTC))
    await updater.start()
    assert len(updater.updated_payloads) == 0


@pytest.mark.asyncio()
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

    async def mock_setup_zmq() -> None:
        nonlocal zmq_setup_called
        zmq_setup_called = True

    async def mock_cleanup_zmq() -> None:
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


@pytest.mark.asyncio()
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

    async def mock_cleanup_zmq() -> None:
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


@pytest.mark.asyncio()
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


@pytest.mark.asyncio()
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
    await updater.setup_zmq_public()
    assert factory.call_count == 1
    await updater.setup_zmq_public()
    assert factory.call_count == 1, "Context should not be created again"


@pytest.mark.asyncio()
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

    async def mock_setup_fails() -> None:
        updater.publisher = None

    monkeypatch.setattr(updater, "_setup_zmq", mock_setup_fails)
    await updater.broadcast_cache_invalidation()


@pytest.mark.asyncio()
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
    updater.set_last_update_timestamp_public(first_timestamp)
    updater.set_last_update_timestamp_public(second_timestamp)
    assert updater.repository is not None
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        settings = list(
            session.execute(
                select(Setting).where(Setting.key == "test_symbols_last_update")
            ).scalars()
        )
    assert len(settings) == 1
    assert settings[0].value == second_timestamp.isoformat()


def test_set_last_update_timestamp_raises_on_repository_error(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify set_last_update_timestamp propagates repository errors.

    Given: Repository that raises ValueError on get_session,
    When: set_last_update_timestamp called,
    Then: ValueError propagated.
    """

    class BrokenRepository:
        def get_session(self) -> None:
            raise ValueError("failed")

    updater = updater_factory(3, False)
    updater.repository = cast(DatabaseRepository, BrokenRepository())
    with pytest.raises(ValueError, match="failed"):
        updater.set_last_update_timestamp_public(datetime.now(UTC))


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
                updated_at=datetime.now(UTC),
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
                updated_at=datetime.now(UTC),
            )
        )
        session.commit()
    assert updater.get_last_update_timestamp_public() is None


def test_upsert_catalog_creates_new_entry(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_catalog creates new catalog entry when none exists.

    Given: Empty database,
    When: _upsert_catalog called with new native_symbol,
    Then: New SymbolCatalog row created and True returned.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    now = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_catalog(
            session, "BTC-USD", "BTC", "USD", "crypto", now
        )
        session.commit()
    assert result is True
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "BTC-USD")
        ).scalar_one()
    assert catalog.base == "BTC"
    assert catalog.quote == "USD"
    assert catalog.asset_type == "crypto"


def test_upsert_catalog_updates_base_currency(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_catalog updates base currency when changed.

    Given: Existing catalog entry with base=BTC,
    When: _upsert_catalog called with base=XBT,
    Then: Base updated to XBT and updated_at refreshed.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                updated_at=original_time,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_catalog(
            session, "BTC-USD", "XBT", "USD", "crypto", update_time
        )
        session.commit()
    assert result is False
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "BTC-USD")
        ).scalar_one()
    assert catalog.base == "XBT"
    assert catalog.updated_at.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_catalog_updates_quote_currency(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_catalog updates quote currency when changed.

    Given: Existing catalog entry with quote=USD,
    When: _upsert_catalog called with quote=USDT,
    Then: Quote updated to USDT and updated_at refreshed.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                updated_at=original_time,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_catalog(
            session, "BTC-USD", "BTC", "USDT", "crypto", update_time
        )
        session.commit()
    assert result is False
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "BTC-USD")
        ).scalar_one()
    assert catalog.quote == "USDT"
    assert catalog.updated_at.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_catalog_updates_asset_type(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_catalog updates asset_type when changed.

    Given: Existing catalog entry with asset_type=crypto,
    When: _upsert_catalog called with asset_type=forex,
    Then: Asset type updated to forex and updated_at refreshed.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="EUR-USD",
                base="EUR",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                updated_at=original_time,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_catalog(
            session, "EUR-USD", "EUR", "USD", "forex", update_time
        )
        session.commit()
    assert result is False
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "EUR-USD")
        ).scalar_one()
    assert catalog.asset_type == "forex"
    assert catalog.updated_at.replace(tzinfo=None) == update_time.replace(tzinfo=None)


def test_upsert_catalog_preserves_timestamp_when_unchanged(
    updater_factory: Callable[[int, bool], DummySymbolUpdater],
) -> None:
    """Verify _upsert_catalog preserves updated_at when no fields changed.

    Given: Existing catalog entry with identical values,
    When: _upsert_catalog called with same values,
    Then: updated_at timestamp preserved unchanged.
    """
    updater = updater_factory(3, False)
    assert updater.repository is not None
    original_time = datetime(2024, 1, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_time,
                updated_at=original_time,
            )
        )
        session.commit()
    update_time = datetime(2024, 6, 1, tzinfo=UTC)
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        result = SymbolUpdaterService._upsert_catalog(
            session, "BTC-USD", "BTC", "USD", "crypto", update_time
        )
        session.commit()
    assert result is False
    with updater.repository.get_session() as session:
        assert isinstance(session, Session)
        catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "BTC-USD")
        ).scalar_one()
    assert catalog.updated_at.replace(tzinfo=None) == original_time.replace(tzinfo=None)
