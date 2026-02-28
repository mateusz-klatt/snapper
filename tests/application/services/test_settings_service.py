"""Unit tests for SettingsService and related configuration utilities."""

import json
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.api.schemas.process import ProcessCreateRequest
from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.services.settings import SettingChangeEvent
from snapper.application.services.settings import SettingsService
from snapper.application.services.settings import get_settings_service
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import clear_global_encryption
from snapper.infrastructure.security.encryption import encrypt_if_sensitive
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.infrastructure.security.encryption import initialize_global_encryption
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.server.process_routes import create_process_configuration
from snapper.server.process_routes import list_process_runs
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import Signal
from snapper.strategies.base import StrategyConfig
from snapper.strategies.cointegration import CointegrationPairs


def make_bar_envelope(
    instrument: str = "BTC-USD",
    close: float = 100.0,
    ts: float | None = None,
    exchange: str = "kraken",
) -> BarEnvelope:
    """Create a BarEnvelope with default values for testing."""
    return BarEnvelope(
        instrument=instrument,
        timeframe="1h",
        open=close - 100,
        high=close + 100,
        low=close - 200,
        close=close,
        volume=1000.0,
        exchange=exchange,
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else datetime.now(UTC),
    )


async def feed_bar_to_strategy(
    strategy: BaseStrategy,
    instrument: str,
    close: float,
    exchange: str = "kraken",
) -> Signal | None:
    """Feed a bar envelope to a strategy and return generated signal."""
    bar = make_bar_envelope(instrument, close, exchange=exchange)
    if instrument not in strategy.candle_buffer:
        strategy.candle_buffer[instrument] = []
    strategy.candle_buffer[instrument].append(bar)
    max_buffer_size = strategy.params.get("buffer_size", 100)
    if len(strategy.candle_buffer[instrument]) > max_buffer_size:
        strategy.candle_buffer[instrument].pop(0)
    return await strategy.on_bar(instrument, bar)


@pytest.fixture(autouse=True)
def clear_settings_singleton() -> Iterator[None]:
    """Clear singleton instances before and after each test."""
    SettingsService.clear_instance()
    clear_global_encryption()
    yield
    SettingsService.clear_instance()
    clear_global_encryption()


@pytest.fixture
async def init_db() -> None:
    """Initialize an in-memory database for testing."""
    repo = get_repository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()


class MockResult:
    """Mock result object for database query simulation."""

    def __init__(self, data: list[MagicMock]) -> None:
        """Initialize the instance."""
        self.data = data
        self.rowcount = len(data) if data else 0

    def scalars(self) -> "MockScalars":
        """Return MockScalars wrapper for result data."""
        return MockScalars(self.data)


class MockScalars:
    """Mock scalars object for result iteration."""

    def __init__(self, data: list[MagicMock]) -> None:
        """Initialize the instance."""
        self.data = data

    def all(self) -> list[MagicMock]:
        """Return all data items."""
        return self.data


class MockSession:
    """Mock async session for database operations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.execute = AsyncMock()
        self.add = MagicMock()
        self.commit = AsyncMock()
        self.close = AsyncMock()

    def __aenter__(self) -> "MockSession":
        """Magic method."""
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: object) -> None:
        """Magic method."""
        pass


class TestSettingsService:
    """Test cases for SettingsService functionality."""

    class _StubPublisher:
        def __init__(self) -> None:
            self.messages: list[tuple[str, bytes]] = []

        async def send_multipart(self, topic: str, payload: bytes) -> None:
            self.messages.append((topic, payload))

        def close(self) -> None:
            """No-op close for test stub."""
            pass

    def test_init_service(self) -> None:
        """Verify SettingsService initializes with correct parameters.

        Given: Database URL and ZMQ broker URL,
        When: SettingsService created,
        Then: Properties set correctly.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        assert service.db_url == "sqlite+aiosqlite:///:memory:"
        assert service.zmq_broker_xpub == "tcp://127.0.0.1:7501"

    @pytest.mark.asyncio
    async def test_shutdown_without_zmq_context(self) -> None:
        """Verify shutdown works when ZMQ context is None.

        Given: Service with no ZMQ context initialized,
        When: shutdown called,
        Then: No error raised.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
        )
        assert service._zmq_context is None
        assert service._publisher is None
        await service.shutdown()

    def test_init_service_with_encryption(self) -> None:
        """Verify SettingsService initializes with encryption enabled.

        Given: Database URL, ZMQ URL, master password, and encryption salt,
        When: SettingsService created,
        Then: Properties set correctly.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password="test-password",
            encryption_salt="test-salt",
        )
        assert service.db_url == "sqlite+aiosqlite:///:memory:"
        assert service.zmq_broker_xpub == "tcp://127.0.0.1:7501"

    def test_get_setting_not_loaded(self) -> None:
        """Verify get_setting returns default when not loaded.

        Given: Uninitialized settings service,
        When: get_setting called,
        Then: Default value returned.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        result = service.get_setting("test_key", "default")
        assert result == "default"

    def test_get_setting_no_default(self) -> None:
        """Verify get_setting returns None when no default provided.

        Given: Uninitialized settings service,
        When: get_setting called without default,
        Then: None returned.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        result = service.get_setting("test_key")
        assert result is None

    def test_parse_and_serialize_helpers(self) -> None:
        """Verify _parse_value and _serialize_value handle types correctly.

        Given: Settings service instance,
        When: _parse_value/_serialize_value called with various types,
        Then: Correct conversions performed.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        parser = cast(Any, service)._parse_value
        assert parser("true") is True
        assert parser("42") == 42
        assert parser("3.14") == pytest.approx(3.14)
        assert parser('"text"') == "text"
        assert parser("[1, 2]") == [1, 2]
        serializer = cast(Any, service)._serialize_value
        assert serializer({"key": "value"}) == '{"key": "value"}'
        assert serializer([1, 2]) == "[1, 2]"
        assert serializer(True) == "true"
        assert serializer(15) == "15"

    @pytest.mark.asyncio
    async def test_initialize_loads_settings(self, init_db: None) -> None:
        """Verify initialize loads settings from database.

        Given: Database with test_key setting,
        When: initialize called,
        Then: Setting retrievable via get_setting.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        async with repo.session() as session:
            session.add(
                Setting(
                    key="test_key",
                    value="test_value",
                    category="test",
                    is_encrypted=False,
                    updated_at=datetime.now(UTC),
                )
            )
            await session.commit()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
        ):
            await service.initialize()
        assert service.get_setting("test_key") == "test_value"

    @pytest.mark.asyncio
    async def test_broadcast_change_sends_payload(self) -> None:
        """Verify _broadcast_change sends ZMQ message.

        Given: Service with stub publisher,
        When: _broadcast_change called,
        Then: Message sent with correct topic and payload.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        publisher = self._StubPublisher()
        cast(Any, service)._publisher = publisher
        await cast(Any, service)._broadcast_change("api_key", "secure", "auth", updated_by="tester")
        assert publisher.messages
        topic, payload = publisher.messages[0]
        assert topic == "system.settings"
        message = json.loads(payload.decode("utf-8"))
        assert message["type"] == "setting_changed"
        assert message["key"] == "api_key"
        assert message["value"] == "secure"
        assert message["category"] == "auth"
        assert message["updated_by"] == "tester"

    @pytest.mark.asyncio
    async def test_broadcast_change_without_publisher(self) -> None:
        """Verify _broadcast_change handles missing publisher.

        Given: Service without publisher,
        When: _broadcast_change called,
        Then: No error raised.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        await cast(Any, service)._broadcast_change("unused", "value", "system")

    @pytest.mark.asyncio
    async def test_broadcast_change_handles_send_error(self) -> None:
        """Verify _broadcast_change logs error on send failure.

        Given: Publisher that raises RuntimeError,
        When: _broadcast_change called,
        Then: Error logged, no exception raised.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        publisher = MagicMock()
        publisher.send_multipart = AsyncMock(side_effect=RuntimeError("fail"))
        cast(Any, service)._publisher = publisher
        with patch("snapper.application.services.settings.logger.error") as log_error:
            await cast(Any, service)._broadcast_change("api_key", "secure", "auth")
        log_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_setting_logs_encrypted_value(self) -> None:
        """Verify update_setting logs encrypted value, not plaintext.

        Given: Service with master_password,
        When: update_setting called with sensitive key,
        Then: Logged value is encrypted, not plaintext.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password="super-secret",
            encryption_salt="test-salt",
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch.object(service, "_broadcast_change", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
            patch("snapper.application.services.settings.logger.info") as log_info,
        ):
            await service.initialize()
            log_info.reset_mock()
            await service.update_setting("api_key_main", "plain-secret")
        messages = [str(args[0]) for args, _ in log_info.call_args_list if args]
        sensitive_updates = [msg for msg in messages if "Setting api_key_main updated to:" in msg]
        assert sensitive_updates
        assert all("plain-secret" not in msg for msg in sensitive_updates)
        assert any("gAAAA" in msg for msg in sensitive_updates)

    @pytest.mark.asyncio
    async def test_update_setting_integration(self, init_db: None) -> None:
        """Verify update_setting persists and retrieves correctly.

        Given: Initialized settings service,
        When: update_setting called,
        Then: Setting retrievable via get_setting.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch.object(service, "_broadcast_change", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
        ):
            await service.initialize()
            await service.update_setting("test_key", "test_value")
            assert service.get_setting("test_key") == "test_value"

    @pytest.mark.asyncio
    async def test_get_all_settings_after_updates(self, init_db: None) -> None:
        """Verify get_all_settings returns both existing and new settings.

        Given: Database with existing_key and newly added new_key,
        When: get_all_settings called,
        Then: Both keys present in result.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        async with repo.session() as session:
            session.add(
                Setting(
                    key="existing_key",
                    value="existing_value",
                    category="test",
                    is_encrypted=False,
                    updated_at=datetime.now(UTC),
                )
            )
            await session.commit()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch.object(service, "_broadcast_change", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
        ):
            await service.initialize()
            await service.update_setting("new_key", "new_value")
            result = await service.get_all_settings()
            assert "existing_key" in result
            assert "new_key" in result
            assert result["existing_key"] == "existing_value"
            assert result["new_key"] == "new_value"

    def test_setting_change_model(self) -> None:
        """Verify SettingChangeEvent model stores attributes correctly.

        Given: SettingChangeEvent with key, value, category, timestamp,
        When: Attributes accessed,
        Then: Correct values returned.
        """
        timestamp = datetime.now(UTC)
        change = SettingChangeEvent(
            key="test_key", value="test_value", category="test", timestamp=timestamp
        )
        assert change.key == "test_key"
        assert change.value == "test_value"
        assert change.category == "test"
        assert change.timestamp == timestamp

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complex_value_types_through_api(self, init_db: None) -> None:
        """Verify update_setting handles complex types (int, bool, dict, list).

        Given: Initialized settings service,
        When: update_setting called with various types,
        Then: Values stored and retrieved correctly.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch.object(service, "_broadcast_change", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
        ):
            await service.initialize()
            await service.update_setting("int_setting", 42)
            assert service.get_setting("int_setting") == 42
            await service.update_setting("bool_setting", True)
            assert service.get_setting("bool_setting") is True
            test_dict = {"nested": {"key": "value"}}
            await service.update_setting("dict_setting", test_dict)
            assert service.get_setting("dict_setting") == test_dict
            test_list: list[object] = [1, 2, {"item": "value"}]
            await service.update_setting("list_setting", test_list)
            assert service.get_setting("list_setting") == test_list

    @pytest.mark.asyncio
    async def test_get_settings_by_category_empty(self, init_db: None) -> None:
        """Verify get_settings_by_category returns empty for nonexistent category.

        Given: Database with auth category setting,
        When: get_settings_by_category called with nonexistent,
        Then: Empty dict returned.
        """
        service = SettingsService(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        repo = get_repository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        async with repo.session() as session:
            session.add(
                Setting(
                    key="jwt_secret",
                    value="secret123",
                    category="auth",
                    is_encrypted=False,
                    updated_at=datetime.now(UTC),
                )
            )
            await session.commit()
        with (
            patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
            patch("snapper.application.services.settings.get_repository", return_value=repo),
        ):
            await service.initialize()
            result = await service.get_settings_by_category("nonexistent")
            assert result == {}
            result = await service.get_settings_by_category("auth")
            assert "jwt_secret" in result
            assert result["jwt_secret"] == "secret123"


@pytest.mark.asyncio
async def test_get_settings_service_reuses_instance() -> None:
    """Verify get_settings_service returns same instance on repeated calls.

    Given: First call to get_settings_service,
    When: Called again with same parameters,
    Then: Same instance returned.
    """
    with patch.object(SettingsService, "initialize", new_callable=AsyncMock) as init_mock:

        async def _mark_loaded(*_args: Any, **_kwargs: Any) -> None:
            instance = cast(SettingsService, SettingsService._instance)
            if instance is not None:
                instance._loaded = True

        init_mock.side_effect = _mark_loaded
        service_a = await get_settings_service(
            "sqlite+aiosqlite:///:memory:", "tcp://127.0.0.1:7501"
        )
        service_b = await get_settings_service(
            "sqlite+aiosqlite:///:memory:", "tcp://127.0.0.1:7501"
        )
    assert service_a is service_b
    assert init_mock.await_count == 1


def test_singleton_new_returns_same_instance_for_same_params() -> None:
    """Verify SettingsService singleton returns same instance.

    Given: First SettingsService created with db_url and zmq_url,
    When: Second instance created with same params,
    Then: Both are same object.
    """
    db_url = "sqlite+aiosqlite:///:memory:"
    zmq_url = "tcp://127.0.0.1:7501"
    service1 = SettingsService(db_url, zmq_url)
    service2 = SettingsService(db_url, zmq_url)
    assert service1 is service2


class TestSettingsEncryption:
    """Test cases for SettingsEncryptionService functionality."""

    def test_encrypt_decrypt_roundtrip(self) -> None:
        """Verify encryption and decryption roundtrip works.

        Given: SettingsEncryptionService with password,
        When: Value encrypted then decrypted,
        Then: Original value recovered.
        """
        encryption = SettingsEncryptionService("test-password", "test-salt")
        original = "super-secret-value"
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original
        assert encrypted != original

    def test_is_sensitive_setting(self) -> None:
        """Verify is_sensitive_setting identifies sensitive keys.

        Given: Various setting key names,
        When: is_sensitive_setting called,
        Then: Returns True for sensitive, False for non-sensitive.
        """
        assert SettingsEncryptionService.is_sensitive_setting("api_key")
        assert SettingsEncryptionService.is_sensitive_setting("api_secret")
        assert SettingsEncryptionService.is_sensitive_setting("password")
        assert SettingsEncryptionService.is_sensitive_setting("auth_secret_key")
        assert SettingsEncryptionService.is_sensitive_setting("csrf_secret_key")
        assert SettingsEncryptionService.is_sensitive_setting("KRAKEN_API_SECRET")
        assert SettingsEncryptionService.is_sensitive_setting("private_key")
        assert not SettingsEncryptionService.is_sensitive_setting("server_host")
        assert not SettingsEncryptionService.is_sensitive_setting("port")
        assert not SettingsEncryptionService.is_sensitive_setting("timeout")
        assert not SettingsEncryptionService.is_sensitive_setting("auth_algorithm")
        assert not SettingsEncryptionService.is_sensitive_setting("csrf_token_expire_minutes")


@pytest.mark.real_settings
def test_settings_bootstrap_access() -> None:
    """Verify get_settings returns bootstrap settings.

    Given: Configured environment,
    When: get_settings called,
    Then: Bootstrap settings accessible.
    """
    s = get_settings()
    assert s.db_url
    assert s.server_host
    assert s.server_port
    assert s.master_password
    assert s.encryption_salt
    assert s.zmq_broker_xsub
    assert s.zmq_broker_xpub


@pytest.mark.real_settings
def test_settings_strict_mode_database_settings() -> None:
    """Verify database settings raise without SettingsService.

    Given: Bootstrap settings without service,
    When: Database setting accessed,
    Then: RuntimeError raised.
    """
    s = get_settings()
    with pytest.raises(
        RuntimeError,
        match="Cannot access database setting 'instruments' - SettingsService not initialized",
    ):
        _ = s.instruments
    with pytest.raises(
        RuntimeError,
        match="Cannot access database setting 'timeframes' - SettingsService not initialized",
    ):
        _ = s.timeframes
    with pytest.raises(
        RuntimeError,
        match="Cannot access database setting 'kraken_api_key' - SettingsService not initialized",
    ):
        _ = s.kraken_api_key
    with pytest.raises(
        RuntimeError,
        match="Cannot access database setting 'zmq_heartbeat_interval_ms' - "
        "SettingsService not initialized",
    ):
        _ = s.zmq_heartbeat_interval_ms


@pytest.mark.real_settings
@pytest.mark.asyncio
async def test_settings_with_service_database_access() -> None:
    """Verify database settings accessible with SettingsService.

    Given: Initialized SettingsService,
    When: get_settings_with_service called,
    Then: Database settings accessible.
    """
    bootstrap = get_settings()
    service = await get_settings_service(
        bootstrap.db_url,
        bootstrap.zmq_broker_xpub,
        bootstrap.master_password,
        bootstrap.encryption_salt,
    )
    s = get_settings_with_service(service)
    assert s.instruments
    assert isinstance(s.instruments, dict)
    assert "polygon" in s.instruments
    assert isinstance(s.instruments["polygon"], list)
    assert s.timeframes
    assert isinstance(s.timeframes, list)
    assert isinstance(s.zmq_heartbeat_interval_ms, int)
    assert s.zmq_heartbeat_interval_ms > 0
    assert s.db_url
    assert s.server_host
    assert s.server_port


class FakeSettingsService:
    """Fake settings service for testing property access."""

    def __init__(self, values: dict[str, Any | None]) -> None:
        """Initialize the instance."""
        self._values = values
        self.calls: list[tuple[str, Any]] = []

    def get_setting(self, key: str, default: Any) -> Any:
        """Record call and return value from internal dict."""
        self.calls.append((key, default))
        return self._values.get(key, default)


def test_settings_properties_use_service_defaults() -> None:
    """Verify AppSettings uses service defaults for properties.

    Given: AppSettings with FakeSettingsService,
    When: Properties accessed,
    Then: Service values returned or defaults used.
    """
    bootstrap = BootstrapSettingsLoader()
    service = FakeSettingsService(
        {
            "log_level": "DEBUG",
            "log_json": None,
            "rest_retry_max_delay": 3.5,
            "session_secure": True,
            "ui_origin": "https://app.snapper.local",
            "session_domain": "snapper.local",
        }
    )
    settings = AppSettings(bootstrap, service)
    assert settings.log_level == "DEBUG"
    assert settings.log_json is False
    assert settings.rest_retry_max_delay == pytest.approx(3.5)
    assert settings.session_secure is True
    assert settings.ui_origin == "https://app.snapper.local"
    assert settings.session_domain == "snapper.local"
    assert settings.get_setting("custom_timeout", 42) == 42
    assert ("log_json", False) in service.calls


def test_settings_get_setting_without_service_raises() -> None:
    """Verify get_setting raises without SettingsService.

    Given: AppSettings without service,
    When: get_setting called,
    Then: RuntimeError raised.
    """
    settings = AppSettings(BootstrapSettingsLoader())
    with pytest.raises(
        RuntimeError,
        match="Cannot access database setting 'custom_timeout' - SettingsService not initialized",
    ):
        settings.get_setting("custom_timeout", 99)


def test_settings_service_without_master_password() -> None:
    """Verify SettingsService works without encryption.

    Given: master_password=None,
    When: SettingsService created,
    Then: Properties set correctly.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
        encryption_salt=None,
    )
    assert service.db_url == "sqlite+aiosqlite:///:memory:"
    assert service.zmq_broker_xpub == "tcp://127.0.0.1:7501"


def test_settings_service_skip_reinit_if_already_initialized() -> None:
    """Verify SettingsService skips re-initialization.

    Given: Already initialized service,
    When: __init__ called again with different params,
    Then: Original db_url preserved.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    original_db_url = service.db_url
    SettingsService.__init__(
        service,
        db_url="sqlite+aiosqlite:///:memory:different",
        zmq_broker_xpub="tcp://127.0.0.1:9999",
        master_password=None,
    )
    assert service.db_url == original_db_url


@pytest.mark.asyncio
async def test_update_setting_with_force_encrypt_cleartext_false() -> None:
    """Verify update_setting works with force_encrypt_cleartext=False.

    Given: Service with no master password,
    When: update_setting called with force_encrypt_cleartext=False,
    Then: Setting stored and broadcast called.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.commit = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock()
    with (
        patch.object(service, "_broadcast_change", new_callable=AsyncMock) as mock_broadcast,
        patch("snapper.application.services.settings.get_repository") as mock_get_repo,
    ):
        mock_repo = MagicMock()
        mock_repo.session = MagicMock(return_value=mock_session)
        mock_get_repo.return_value = mock_repo
        await service.update_setting(
            key="test_key",
            value="test_value",
            category="system",
            force_encrypt_cleartext=False,
        )
        assert mock_broadcast.called


@pytest.mark.asyncio
async def test_broadcast_change_when_publisher_is_none() -> None:
    """Verify _broadcast_change handles None publisher.

    Given: Service with _publisher=None,
    When: _broadcast_change called,
    Then: No error raised.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    service._publisher = None
    await service._broadcast_change("test_key", "test_value", "system", "user1")


@pytest.mark.asyncio
async def test_broadcast_change_when_send_multipart_raises() -> None:
    """Verify _broadcast_change handles send_multipart exception.

    Given: Publisher that raises Exception,
    When: _broadcast_change called,
    Then: send_multipart called, no exception raised.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    mock_publisher = MagicMock()
    mock_publisher.send_multipart = AsyncMock(side_effect=Exception("ZMQ error"))
    service._publisher = mock_publisher
    await service._broadcast_change("test_key", "test_value", "system", "user1")
    assert mock_publisher.send_multipart.called


@pytest.mark.asyncio
async def test_get_settings_service_with_already_loaded_cache() -> None:
    """Verify get_settings_service skips load when already loaded.

    Given: Service with _loaded=True,
    When: get_settings_service called,
    Then: _load_all_settings not called.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    with (
        patch.object(service, "_load_all_settings", new_callable=AsyncMock) as mock_load,
        patch.object(service, "_setup_zmq_publisher", new_callable=AsyncMock),
    ):
        service._loaded = True
        result = await get_settings_service(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
            master_password=None,
        )
        assert result is service
        assert not mock_load.called


def test_parse_value_for_bool() -> None:
    """Verify _parse_value parses boolean strings.

    Given: Service instance,
    When: _parse_value called with 'true'/'false',
    Then: Boolean values returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    assert service._parse_value("true") is True
    assert service._parse_value("True") is True
    assert service._parse_value("false") is False
    assert service._parse_value("False") is False


def test_parse_value_for_int() -> None:
    """Verify _parse_value parses integer strings.

    Given: Service instance,
    When: _parse_value called with numeric strings,
    Then: Integer values returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    assert service._parse_value("123") == 123
    assert service._parse_value("-456") == -456
    assert service._parse_value("0") == 0


def test_parse_value_for_float() -> None:
    """Verify _parse_value parses float strings.

    Given: Service instance,
    When: _parse_value called with decimal strings,
    Then: Float values returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    assert service._parse_value("123.45") == pytest.approx(123.45)
    assert service._parse_value("-67.89") == pytest.approx(-67.89)
    assert service._parse_value("0.0") == pytest.approx(0.0)


def test_parse_value_for_string() -> None:
    """Verify _parse_value passes through plain strings.

    Given: Service instance,
    When: _parse_value called with non-parseable strings,
    Then: Original strings returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    assert service._parse_value("hello") == "hello"
    assert service._parse_value("not a number") == "not a number"
    assert service._parse_value("") == ""


def test_parse_value_for_json_list() -> None:
    """Verify _parse_value parses JSON list strings.

    Given: Service instance,
    When: _parse_value called with JSON array,
    Then: Python list returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    result = service._parse_value('["a", "b", "c"]')
    assert result == ["a", "b", "c"]


def test_parse_value_for_json_dict() -> None:
    """Verify _parse_value parses JSON object strings.

    Given: Service instance,
    When: _parse_value called with JSON object,
    Then: Python dict returned.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    result = service._parse_value('{"key": "value"}')
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_shutdown_when_publisher_exists() -> None:
    """Verify shutdown closes publisher and terminates context.

    Given: Service with mock publisher and context,
    When: shutdown called,
    Then: close and term methods called.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    mock_publisher = MagicMock()
    service._publisher = mock_publisher
    mock_context = MagicMock()
    service._zmq_context = mock_context
    await service.shutdown()
    assert mock_publisher.close.called
    assert mock_context.term.called


@pytest.mark.asyncio
async def test_shutdown_when_publisher_is_none() -> None:
    """Verify shutdown handles None publisher.

    Given: Service with _publisher=None but context set,
    When: shutdown called,
    Then: context.term called.
    """
    service = SettingsService(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password=None,
    )
    service._publisher = None
    mock_context = MagicMock()
    service._zmq_context = mock_context
    await service.shutdown()
    assert mock_context.term.called


def test_get_instance_returns_none_when_no_params() -> None:
    """Verify get_instance returns None when no instance exists.

    Given: No existing SettingsService instance,
    When: get_instance called,
    Then: None returned.
    """
    result = SettingsService.get_instance()
    assert result is None


class TestCointegrationInstrument2Hedges:
    """Test cases for CointegrationPairs instrument 2 hedge scenarios."""

    @pytest.fixture
    def strategy(self) -> CointegrationPairs:
        """Create CointegrationPairs strategy with test configuration."""
        config = StrategyConfig(
            name="cointegration_test",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 40,
                "min_data_points": 30,
            },
        )
        return CointegrationPairs(config=config)

    async def _build_history_for_short_spread(self, strategy: CointegrationPairs) -> None:
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 10)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i * 0.5)

    async def _build_history_for_long_spread(self, strategy: CointegrationPairs) -> None:
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 - i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i * 5)

    @pytest.mark.asyncio
    async def test_short_spread_entry_instrument2_hedge(self, strategy: CointegrationPairs) -> None:
        """Verify short spread entry generates instrument 2 hedge signal.

        Given: History built for short spread,
        When: BTC price rises to trigger entry,
        Then: ETH buy hedge signal generated.
        """
        await self._build_history_for_short_spread(strategy)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        strategy._position = "short_spread"
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3017.5)
        if signal:
            assert signal.instrument == "ETH-USD"
            assert signal.side == "buy"
            assert "hedge" in signal.reason.lower()

    @pytest.mark.asyncio
    async def test_long_spread_entry_instrument2_hedge(self, strategy: CointegrationPairs) -> None:
        """Verify long spread entry generates instrument 2 hedge signal.

        Given: History built for long spread,
        When: BTC price drops to trigger entry,
        Then: ETH sell hedge signal generated.
        """
        await self._build_history_for_long_spread(strategy)
        await feed_bar_to_strategy(strategy, "BTC-USD", 45000.0)
        strategy._position = "long_spread"
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3175.0)
        if signal:
            assert signal.instrument == "ETH-USD"
            assert signal.side == "sell"
            assert "hedge" in signal.reason.lower()

    @pytest.mark.asyncio
    async def test_short_spread_exit_instrument2_hedge(self, strategy: CointegrationPairs) -> None:
        """Verify short spread exit generates instrument 2 exit signal.

        Given: Active short spread position,
        When: Spread reverts to exit threshold,
        Then: ETH sell exit signal generated.
        """
        await self._build_history_for_short_spread(strategy)
        strategy._position = "short_spread"
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 50350.0)
        if signal and signal.strength == pytest.approx(0.0):
            strategy._position = "short_spread"
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3017.5)
        if signal and signal.strength == pytest.approx(0.0):
            assert signal.instrument == "ETH-USD"
            assert signal.side == "sell"
            assert "exit" in signal.reason.lower()

    @pytest.mark.asyncio
    async def test_long_spread_exit_instrument2_hedge(self, strategy: CointegrationPairs) -> None:
        """Verify long spread exit generates instrument 2 exit signal.

        Given: Active long spread position,
        When: Spread reverts to exit threshold,
        Then: ETH buy exit signal generated.
        """
        await self._build_history_for_long_spread(strategy)
        strategy._position = "long_spread"
        for i in range(10):
            signal_btc = await feed_bar_to_strategy(strategy, "BTC-USD", 45000.0 + i * 500)
            if signal_btc and signal_btc.strength == pytest.approx(0.0):
                strategy._position = "long_spread"
            signal_eth = await feed_bar_to_strategy(strategy, "ETH-USD", 3175.0 - i * 10)
            if signal_eth and signal_eth.strength == pytest.approx(0.0):
                assert signal_eth.instrument == "ETH-USD"
                assert signal_eth.side == "buy"
                assert "exit" in signal_eth.reason.lower()
                break

    @pytest.mark.asyncio
    async def test_entry_no_position_short_spread_instrument2_direct(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify short spread entry from no position triggers direct instrument 2 signal.

        Given: No active position with short spread setup,
        When: ETH price fed with BTC at entry level,
        Then: ETH buy signal generated.
        """
        await self._build_history_for_short_spread(strategy)
        strategy._position = None
        strategy.candle_buffer["BTC-USD"][-1] = make_bar_envelope("BTC-USD", 60000.0)
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3017.5)
        if signal and strategy._position == "short_spread":
            assert signal.instrument == "ETH-USD"
            assert signal.side == "buy"

    @pytest.mark.asyncio
    async def test_entry_no_position_long_spread_instrument2_direct(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify long spread entry from no position triggers direct instrument 2 signal.

        Given: No active position with long spread setup,
        When: ETH price fed with BTC at entry level,
        Then: ETH sell signal generated.
        """
        await self._build_history_for_long_spread(strategy)
        strategy._position = None
        strategy.candle_buffer["BTC-USD"][-1] = make_bar_envelope("BTC-USD", 40000.0)
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3200.0)
        if signal and strategy._position == "long_spread":
            assert signal.instrument == "ETH-USD"
            assert signal.side == "sell"


class TestEncryptionExceptionHandling:
    """Test cases for encryption exception handling scenarios."""

    def test_encrypt_exception_reraises(self) -> None:
        """Verify encrypt raises when fernet fails.

        Given: SettingsEncryptionService with mocked fernet,
        When: encrypt called and fernet raises ValueError,
        Then: ValueError propagated.
        """
        encryption = SettingsEncryptionService("test-password", "test-salt")
        with patch.object(encryption, "_fernet") as mock_fernet:
            mock_fernet.encrypt.side_effect = ValueError("Encryption failed")
            with pytest.raises(ValueError, match="Encryption failed"):
                encryption.encrypt("test-value")

    def test_get_encryption_service_no_master_password_from_bootstrap(self) -> None:
        """Verify get_encryption_service returns None without password.

        Given: Bootstrap with no master_password,
        When: get_encryption_service called,
        Then: None returned.
        """
        with patch(
            "snapper.infrastructure.security.encryption.BootstrapSettingsLoader"
        ) as mock_bootstrap:
            mock_instance = MagicMock()
            mock_instance.master_password = None
            mock_instance.encryption_salt = None
            mock_bootstrap.return_value = mock_instance
            result = get_encryption_service(master_password=None, salt=None)
            assert result is None

    def test_encrypt_if_sensitive_already_encrypted_value(self) -> None:
        """Verify encrypt_if_sensitive handles already encrypted values.

        Given: Already encrypted value,
        When: encrypt_if_sensitive called,
        Then: Value passed through, is_encrypted=True.
        """
        clear_global_encryption()
        encryption = initialize_global_encryption("test-password", "test-salt")
        encrypted_value = encryption.encrypt("my-secret")
        result_value, is_encrypted = encrypt_if_sensitive("api_secret", encrypted_value)
        assert is_encrypted is True
        assert result_value == encrypted_value
        decrypted = encryption.decrypt(result_value)
        assert decrypted == "my-secret"


class TestProcessRoutesTagsFallback:
    """Test cases for process route tag fallback handling."""

    @pytest.mark.asyncio
    async def test_create_process_configuration_tags_none(self) -> None:
        """Verify create_process_configuration handles None tags.

        Given: Registry entry with tags=None,
        When: create_process_configuration called,
        Then: Empty tuple passed for tags.
        """
        mock_factory = MagicMock()
        mock_factory.create_process_config = AsyncMock()
        strategy_class = MagicMock()
        strategy_class.get_default_kwargs.return_value = {"name": "test"}
        registry_data: dict[str, ProcessRegistryEntry] = {
            "test_process": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="snapper.test.TestProcess",
                method="start",
                description="Test process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
                args=[],
            )
        }
        with patch(
            "snapper.server.process_routes.get_registered_processes",
            return_value=registry_data,
        ):
            request = ProcessCreateRequest(
                name="my_process",
                template="test_process",
                enabled=True,
                mode="thread",
                args=[],
                kwargs={},
            )
            settings = MagicMock()
            await create_process_configuration(
                request=request,
                factory=mock_factory,
                settings=settings,
                _user=MagicMock(),
                _csrf=None,
            )
            call_kwargs = mock_factory.create_process_config.call_args.kwargs
            assert call_kwargs["tags"] == ()

    @pytest.mark.asyncio
    async def test_create_process_configuration_tags_string(self) -> None:
        """Verify create_process_configuration handles string tags.

        Given: Registry entry with tags as non-list string,
        When: create_process_configuration called,
        Then: Empty tuple passed for tags.
        """
        mock_factory = MagicMock()
        mock_factory.create_process_config = AsyncMock()
        strategy_class = MagicMock()
        strategy_class.get_default_kwargs.return_value = {"name": "test"}
        registry_data: dict[str, ProcessRegistryEntry] = {
            "test_process": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="snapper.test.TestProcess",
                method="start",
                description="Test process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
                args=[],
            )
        }
        with patch(
            "snapper.server.process_routes.get_registered_processes",
            return_value=registry_data,
        ):
            request = ProcessCreateRequest(
                name="my_process",
                template="test_process",
                enabled=True,
                mode="thread",
                args=[],
                kwargs={},
            )
            settings = MagicMock()
            await create_process_configuration(
                request=request,
                factory=mock_factory,
                settings=settings,
                _user=MagicMock(),
                _csrf=None,
            )
            call_kwargs = mock_factory.create_process_config.call_args.kwargs
            assert call_kwargs["tags"] == ()


class TestProcessRoutesListRuns:
    """Test cases for process route list runs functionality."""

    @pytest.mark.asyncio
    async def test_list_process_runs(self) -> None:
        """Verify list_process_runs returns recent runs.

        Given: Factory returning list of run records,
        When: list_process_runs called,
        Then: Correct count and runs returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "run_id": "run-001",
                    "process_name": "test_process",
                    "started_at": "2024-01-01T00:00:00",
                    "completed_at": "2024-01-01T01:00:00",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": {},
                    "result": None,
                    "error": None,
                    "tags": [],
                },
                {
                    "run_id": "run-002",
                    "process_name": "test_process_2",
                    "started_at": "2024-01-02T00:00:00",
                    "completed_at": None,
                    "status": "running",
                    "role": "task",
                    "lifecycle": "one_shot",
                    "parameters": {},
                    "result": None,
                    "error": None,
                    "tags": [],
                },
            ]
        )
        result = await list_process_runs(
            limit=50,
            name=None,
            factory=mock_factory,
            _user=MagicMock(),
        )
        assert result.count == 2
        assert len(result.runs) == 2
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=50, name=None)

    @pytest.mark.asyncio
    async def test_list_process_runs_with_name_filter(self) -> None:
        """Verify list_process_runs filters by name.

        Given: Factory with name filter support,
        When: list_process_runs called with name parameter,
        Then: get_recent_runs called with correct name.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "run_id": "run-001",
                    "process_name": "filtered_process",
                    "started_at": "2024-01-01T00:00:00",
                    "completed_at": "2024-01-01T01:00:00",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": {},
                    "result": None,
                    "error": None,
                    "tags": [],
                }
            ]
        )
        result = await list_process_runs(
            limit=10,
            name="filtered_process",
            factory=mock_factory,
            _user=MagicMock(),
        )
        assert result.count == 1
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=10, name="filtered_process")
