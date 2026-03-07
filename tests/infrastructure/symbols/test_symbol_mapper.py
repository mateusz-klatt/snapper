"""Tests for symbol mapping service and conversion functions."""

import importlib.util
from functools import lru_cache
from types import ModuleType
from typing import Any
from typing import cast
from typing import get_args
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError

import snapper.infrastructure.symbols.functions as functions
import snapper.infrastructure.symbols.mapper as symbol_mapper_module
from snapper.core.types import AllExchange
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketSubscribeExchange
from snapper.core.types import OrderExchange
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.infrastructure.symbols.functions import ccxt_to_kraken_websocket
from snapper.infrastructure.symbols.functions import ccxt_to_native
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_kraken_rest_symbols
from snapper.infrastructure.symbols.functions import get_available_kraken_symbols
from snapper.infrastructure.symbols.functions import get_available_polygon_rest_symbols
from snapper.infrastructure.symbols.functions import get_available_polygon_symbols
from snapper.infrastructure.symbols.functions import get_available_symbols
from snapper.infrastructure.symbols.functions import get_available_walutomat_rest_symbols
from snapper.infrastructure.symbols.functions import get_available_walutomat_symbols
from snapper.infrastructure.symbols.functions import get_available_ws_symbols
from snapper.infrastructure.symbols.functions import get_available_zonda_symbols
from snapper.infrastructure.symbols.functions import get_market_data_exchanges
from snapper.infrastructure.symbols.functions import get_market_data_symbols
from snapper.infrastructure.symbols.functions import get_market_subscribe_exchanges
from snapper.infrastructure.symbols.functions import get_tradeable_symbols
from snapper.infrastructure.symbols.functions import is_market_data_available
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.functions import kraken_rest_to_native
from snapper.infrastructure.symbols.functions import kraken_websocket_to_ccxt
from snapper.infrastructure.symbols.functions import kraken_websocket_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_rest
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket
from snapper.infrastructure.symbols.functions import native_to_polygon_rest
from snapper.infrastructure.symbols.functions import native_to_walutomat_rest
from snapper.infrastructure.symbols.functions import native_to_walutomat_ws
from snapper.infrastructure.symbols.functions import native_to_zonda_ws
from snapper.infrastructure.symbols.functions import polygon_rest_to_native
from snapper.infrastructure.symbols.functions import validate_symbol
from snapper.infrastructure.symbols.functions import walutomat_rest_to_native
from snapper.infrastructure.symbols.functions import walutomat_ws_to_native
from snapper.infrastructure.symbols.functions import zonda_ws_to_native
from snapper.infrastructure.symbols.mapper import CapabilityInfo
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.infrastructure.symbols.mapper import make_native_symbol


class TestDatabaseSymbolMapper:
    """Tests for SymbolMapperService database integration."""

    def test_init(self) -> None:
        """Initialize SymbolMapperService with repository.

        Given: Default environment settings,
        When: SymbolMapperService is instantiated,
        Then: Repository and cache dictionaries are initialized.
        """
        mapper = SymbolMapperService()
        assert mapper is not None
        assert mapper.repository is not None
        assert mapper.native_to_kraken_ws is not None

    def test_cache_invalidation_pattern(self) -> None:
        """Cache invalidation preserves existing mappings.

        Given: Mapper with loaded cache,
        When: trigger_cache_invalidation is called,
        Then: Cache count remains unchanged.
        """
        mapper = SymbolMapperService()
        initial_count = len(mapper.native_to_kraken_ws)
        mapper.trigger_cache_invalidation(fail_fast=False)
        assert len(mapper.native_to_kraken_ws) == initial_count

    def test_load_cache_if_needed_simple(self) -> None:
        """Cache loading is idempotent.

        Given: Mapper instance,
        When: load_cache_if_needed is called twice,
        Then: Cache count is same after both calls.
        """
        mapper = SymbolMapperService()
        mapper.load_cache_if_needed()
        first_count = len(mapper.native_to_kraken_ws)
        mapper.load_cache_if_needed()
        second_count = len(mapper.native_to_kraken_ws)
        assert second_count == first_count

    def test_safe_zmq_invalidation(self) -> None:
        """ZMQ invalidation is safe with fail_fast=False.

        Given: Mapper with loaded cache,
        When: trigger_cache_invalidation called with fail_fast=False,
        Then: Cache remains unchanged.
        """
        mapper = SymbolMapperService()
        mapper.load_cache_if_needed()
        first_count = len(mapper.native_to_kraken_ws)
        mapper.trigger_cache_invalidation(fail_fast=False)
        second_count = len(mapper.native_to_kraken_ws)
        assert second_count == first_count

    def test_load_mappings_from_db_empty(self) -> None:
        """Load mappings returns list from database.

        Given: Mapper instance,
        When: load_mappings_from_db is called,
        Then: Returns list of SymbolAlias objects.
        """
        mapper = SymbolMapperService()
        mappings = mapper.load_mappings_from_db()
        assert isinstance(mappings, list)

    def test_direct_cache_access(self) -> None:
        """Cache dictionaries are accessible.

        Given: Mapper instance,
        When: Cache properties are accessed,
        Then: Returns dictionary objects.
        """
        mapper = SymbolMapperService()
        assert isinstance(mapper.native_to_kraken_ws, dict)
        assert isinstance(mapper.native_to_kraken_rest, dict)
        assert isinstance(mapper.kraken_ws_to_native, dict)
        assert isinstance(mapper.kraken_rest_to_native, dict)


class TestMakeNativeSymbol:
    """Tests for make_native_symbol utility function."""

    def test_make_native_symbol_success(self) -> None:
        """Create native symbol from base and quote.

        Given: Valid base and quote currencies,
        When: make_native_symbol is called,
        Then: Returns hyphen-separated symbol.
        """
        result = make_native_symbol("BTC", "USD")
        assert result == "BTC-USD"

    def test_make_native_symbol_strips_whitespace(self) -> None:
        """Strip whitespace from currency codes.

        Given: Currencies with leading/trailing spaces,
        When: make_native_symbol is called,
        Then: Returns trimmed symbol.
        """
        result = make_native_symbol(" BTC ", " USD ")
        assert result == "BTC-USD"

    def test_make_native_symbol_converts_to_uppercase(self) -> None:
        """Convert currency codes to uppercase.

        Given: Lowercase currency codes,
        When: make_native_symbol is called,
        Then: Returns uppercase symbol.
        """
        result = make_native_symbol("btc", "usd")
        assert result == "BTC-USD"

    def test_make_native_symbol_empty_base_raises(self) -> None:
        """Reject empty base currency.

        Given: Empty string as base currency,
        When: make_native_symbol is called,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Base and quote currencies must be non-empty"):
            make_native_symbol("", "USD")

    def test_make_native_symbol_empty_quote_raises(self) -> None:
        """Reject empty quote currency.

        Given: Empty string as quote currency,
        When: make_native_symbol is called,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Base and quote currencies must be non-empty"):
            make_native_symbol("BTC", "")

    def test_make_native_symbol_whitespace_only_base_raises(self) -> None:
        """Reject whitespace-only base currency.

        Given: Whitespace-only string as base,
        When: make_native_symbol is called,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Base and quote currencies must be non-empty"):
            make_native_symbol("   ", "USD")

    def test_make_native_symbol_whitespace_only_quote_raises(self) -> None:
        """Reject whitespace-only quote currency.

        Given: Whitespace-only string as quote,
        When: make_native_symbol is called,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Base and quote currencies must be non-empty"):
            make_native_symbol("BTC", "   ")


class TestSymbolMapperServiceIntegration:
    """Tests for SymbolMapperService singleton and integration behavior."""

    def test_singleton_functionality(self) -> None:
        """Service implements singleton pattern.

        Given: Cleared singleton instance,
        When: Two instances are created,
        Then: Both references point to same object.
        """
        mock_settings = Mock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5556"
        mock_repo = Mock()
        session_cm = Mock()
        session_cm.__enter__ = Mock(return_value=Mock())
        session_cm.__exit__ = Mock(return_value=None)
        mock_repo.get_session.return_value = session_cm
        SymbolMapperService.clear_instance()
        with patch(
            "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
            return_value=mock_settings,
        ), patch(
            "snapper.infrastructure.symbols.mapper.DatabaseRepository",
            return_value=mock_repo,
        ), patch.object(
            SymbolMapperService, "load_mappings_from_db", return_value=[]
        ):
            instance1 = SymbolMapperService()
            instance2 = SymbolMapperService()
            assert instance1 is instance2

    def test_operational_error_handling(self) -> None:
        """Handle missing table gracefully.

        Given: Database session that raises OperationalError,
        When: load_mappings_from_db is called,
        Then: Returns empty list without propagating error.
        """
        mock_settings = Mock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5556"
        mock_repo = Mock()
        session_cm = Mock()
        session = Mock()
        error = OperationalError("statement", "params", Exception("no such table: symbol_aliases"))
        session.execute.side_effect = error
        session_cm.__enter__ = Mock(return_value=session)
        session_cm.__exit__ = Mock(return_value=None)
        mock_repo.get_session.return_value = session_cm
        SymbolMapperService.clear_instance()
        with patch(
            "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
            return_value=mock_settings,
        ), patch(
            "snapper.infrastructure.symbols.mapper.DatabaseRepository",
            return_value=mock_repo,
        ), patch.object(
            SymbolMapperService, "trigger_cache_invalidation"
        ):
            mapper = SymbolMapperService()
            result = mapper.load_mappings_from_db()
            assert result == []

    def test_cache_operations(self) -> None:
        """Cache invalidation triggers reload.

        Given: Mapper with mocked load_cache_if_needed,
        When: trigger_cache_invalidation is called,
        Then: load_cache_if_needed is invoked.
        """
        mock_settings = Mock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5556"
        SymbolMapperService.clear_instance()
        with patch(
            "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
            return_value=mock_settings,
        ), patch("snapper.infrastructure.symbols.mapper.DatabaseRepository"), patch.object(
            SymbolMapperService, "load_cache_if_needed", autospec=True
        ) as mock_load:
            mapper = SymbolMapperService()
            mock_load.reset_mock()
            mapper.trigger_cache_invalidation(fail_fast=True)
            mock_load.assert_called_once_with(mapper, fail_fast=True)

    def test_get_instance_method(self) -> None:
        """get_instance returns singleton.

        Given: Cleared singleton instance,
        When: get_instance called twice,
        Then: Same instance is returned.
        """
        mock_settings = Mock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5556"
        SymbolMapperService.clear_instance()
        with patch(
            "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
            return_value=mock_settings,
        ), patch("snapper.infrastructure.symbols.mapper.DatabaseRepository"), patch.object(
            SymbolMapperService, "load_cache_if_needed", autospec=True
        ):
            instance1 = SymbolMapperService.get_instance()
            instance2 = SymbolMapperService.get_instance()
            assert instance1 is instance2


@pytest.fixture
def mock_settings() -> MagicMock:
    """Provide mock application settings."""
    settings = MagicMock()
    settings.db_url = "sqlite:///:memory:"
    settings.zmq_broker_xpub = "tcp://localhost:5556"
    return settings


@pytest.fixture
def mock_repository() -> MagicMock:
    """Provide mock repository with session context manager."""
    repository = MagicMock()
    session = MagicMock()
    repository.get_session.return_value.__enter__.return_value = session
    repository.get_session.return_value.__exit__.return_value = None
    return repository


@pytest.fixture
def sample_aliases() -> list[SymbolAlias]:
    """Provide sample BTC-USD symbol aliases for kraken ws/rest/ccxt."""
    return [
        SymbolAlias(
            native_symbol="BTC-USD", exchange="kraken", channel="ws", exchange_symbol="BTC/USD"
        ),
        SymbolAlias(
            native_symbol="BTC-USD", exchange="kraken", channel="rest", exchange_symbol="XXBTZUSD"
        ),
        SymbolAlias(
            native_symbol="BTC-USD", exchange="kraken", channel="ccxt", exchange_symbol="BTC/USD"
        ),
    ]


class TestSymbolMapperServiceErrorHandling:
    """Tests for SymbolMapperService error handling scenarios."""

    @pytest.fixture(autouse=True)
    def clear_singleton(self) -> None:
        """Clear singleton instance before each test."""
        SymbolMapperService.clear_instance()

    def test_load_mappings_from_db_no_such_table_returns_empty_list(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Return empty list when table doesn't exist.

        Given: Session that raises 'no such table' error,
        When: load_mappings_from_db is called,
        Then: Returns empty list.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            error = Exception("no such table: symbol_aliases")
            mock_session.execute.side_effect = OperationalError(
                "no such table: symbol_aliases",
                params=None,
                orig=error,
            )
            result = mapper.load_mappings_from_db()
            assert result == []

    def test_load_mappings_from_db_other_operational_error_raises(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Propagate non-table-missing operational errors.

        Given: Session that raises 'database is locked' error,
        When: load_mappings_from_db is called,
        Then: Raises OperationalError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            error = Exception("database is locked")
            mock_session.execute.side_effect = OperationalError(
                "database is locked",
                params=None,
                orig=error,
            )
            with pytest.raises(OperationalError, match="database is locked"):
                mapper.load_mappings_from_db()

    def test_load_cache_if_needed_fail_fast_false_keeps_cache_on_error(
        self,
        mock_settings: MagicMock,
        mock_repository: MagicMock,
        sample_aliases: list[SymbolAlias],
    ) -> None:
        """Preserve cache on error with fail_fast=False.

        Given: Mapper with loaded cache and subsequent DB error,
        When: load_cache_if_needed called with fail_fast=False,
        Then: Existing cache is preserved.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
        ):
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = sample_aliases
            mock_session.execute.return_value = mock_result
            mapper = SymbolMapperService()
            assert "BTC-USD" in mapper.native_to_kraken_ws
            old_cache_size = len(mapper.native_to_kraken_ws)
            mapper._cache_loaded = False
            mock_session.execute.side_effect = RuntimeError("Database connection failed")
            mapper.load_cache_if_needed(fail_fast=False)
            assert "BTC-USD" in mapper.native_to_kraken_ws
            assert len(mapper.native_to_kraken_ws) == old_cache_size


class TestSymbolMapperAdditionalBranches:
    """Tests for additional SymbolMapperService code branches."""

    @pytest.fixture(autouse=True)
    def clear_singleton(self) -> None:
        """Clear singleton instance before each test."""
        SymbolMapperService.clear_instance()

    def test_init_warmup_failure_raises(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Initialization failure propagates exception.

        Given: load_cache_if_needed that raises RuntimeError,
        When: SymbolMapperService is instantiated,
        Then: RuntimeError is propagated.
        """
        SymbolMapperService.clear_instance()
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.SymbolMapperService.load_cache_if_needed",
                side_effect=RuntimeError("DB connection failed"),
            ),
            pytest.raises(RuntimeError, match="DB connection failed"),
        ):
            SymbolMapperService()

    @pytest.mark.real_settings
    def test_load_cache_if_needed_skips_when_already_loaded(self) -> None:
        """Skip cache load when already loaded.

        Given: Mapper with cache already loaded,
        When: load_cache_if_needed is called again,
        Then: DB is not queried again.
        """
        SymbolMapperService.clear_instance()
        load_call_count = 0
        original_load = SymbolMapperService.load_mappings_from_db

        def counting_load(self: Any) -> list[SymbolAlias]:
            nonlocal load_call_count
            load_call_count += 1
            return original_load(self)

        with patch.object(SymbolMapperService, "load_mappings_from_db", counting_load):
            mapper = SymbolMapperService()
            initial_count = load_call_count
            mapper.load_cache_if_needed(fail_fast=False)
            assert load_call_count == initial_count


@lru_cache(maxsize=1)
def _load_original_symbol_mapper_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "symbol_mapper_original", symbol_mapper_module.__file__
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load original symbol_mapper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _call_original_load_cache_if_needed(
    mapper: SymbolMapperService,
    *,
    fail_fast: bool = False,
) -> None:
    module = _load_original_symbol_mapper_module()
    module.SymbolMapperService.load_cache_if_needed(mapper, fail_fast=fail_fast)


def _make_mapper_with_empty_cache() -> SymbolMapperService:
    SymbolMapperService.clear_instance()
    mapper = SymbolMapperService.__new__(SymbolMapperService)
    mapper.repository = MagicMock()
    mapper.forward = {}
    mapper.reverse = {}
    mapper.native_to_kraken_ws = {}
    mapper.native_to_kraken_rest = {}
    mapper.native_to_ccxt = {}
    mapper.kraken_ws_to_native = {}
    mapper.kraken_rest_to_native = {}
    mapper.ccxt_to_native = {}
    mapper.native_to_zonda_ws = {}
    mapper.zonda_ws_to_native = {}
    mapper.native_to_walutomat_ws = {}
    mapper.walutomat_ws_to_native = {}
    mapper.native_to_walutomat_rest = {}
    mapper.walutomat_rest_to_native = {}
    mapper.native_to_polygon_rest = {}
    mapper.polygon_rest_to_native = {}
    mapper.capabilities = {}
    mapper._cache_loaded = False
    mapper.context = None
    mapper.subscriber = None
    mapper._listening = False
    mapper._listener_task = None
    return mapper


def test_clear_instance_logs_warning_on_repository_dispose_error() -> None:
    """Clear instance logs a warning when repository disposal fails.

    Given: A singleton instance whose repository dispose raises,
    When: clear_instance is called,
    Then: The instance is cleared and the warning is emitted.
    """
    mapper = SymbolMapperService.__new__(SymbolMapperService)
    repository = Mock()
    repository.dispose.side_effect = RuntimeError("dispose failed")
    mapper.repository = repository
    SymbolMapperService._instance = mapper
    with patch.object(symbol_mapper_module.logger, "warning") as warning_mock:
        SymbolMapperService.clear_instance()
    warning_mock.assert_called_once()
    assert SymbolMapperService._instance is None


class TestDatabaseSymbolMapperCore:
    """Tests for core SymbolMapperService operations."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings with test configuration."""
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        settings.zmq_broker_xpub = "tcp://localhost:5556"
        return settings

    @pytest.fixture
    def mock_repository(self) -> MagicMock:
        """Create mock repository with session context manager."""
        repository = MagicMock()
        session = MagicMock()
        repository.get_session.return_value.__enter__.return_value = session
        return repository

    @pytest.fixture
    def sample_aliases(self) -> list[SymbolAlias]:
        """Create sample symbol aliases for testing."""
        return [
            SymbolAlias(
                native_symbol="BTC-USD", exchange="kraken", channel="ws", exchange_symbol="BTC/USD"
            ),
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="kraken",
                channel="rest",
                exchange_symbol="XXBTZUSD",
            ),
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="kraken",
                channel="ccxt",
                exchange_symbol="BTC/USD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD", exchange="kraken", channel="ws", exchange_symbol="ETH/USD"
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="kraken",
                channel="rest",
                exchange_symbol="XETHZUSD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="kraken",
                channel="ccxt",
                exchange_symbol="ETH/USD",
            ),
        ]

    def test_init_success(self, mock_settings: MagicMock, mock_repository: MagicMock) -> None:
        """Initialize mapper with mocked dependencies.

        Given: Mocked settings and repository,
        When: SymbolMapperService is instantiated,
        Then: All cache dictionaries are initialized.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "load_mappings_from_db", return_value=[]),
        ):
            mapper = SymbolMapperService()
            assert mapper.repository is mock_repository
            assert isinstance(mapper.native_to_kraken_ws, dict)
            assert isinstance(mapper.native_to_kraken_rest, dict)
            assert isinstance(mapper.kraken_ws_to_native, dict)
            assert isinstance(mapper.kraken_rest_to_native, dict)
            assert hasattr(mapper, "_cache_loaded")

    def test_load_mappings_from_db_success(
        self,
        mock_settings: MagicMock,
        mock_repository: MagicMock,
        sample_aliases: list[SymbolAlias],
    ) -> None:
        """Load aliases from database successfully.

        Given: Mock session returning sample aliases,
        When: load_mappings_from_db is called,
        Then: Returns list of SymbolAlias objects.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
        ):
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = sample_aliases
            mock_session.execute.return_value = mock_result
            with patch.object(SymbolMapperService, "trigger_cache_invalidation"):
                mapper = SymbolMapperService()
                aliases = mapper.load_mappings_from_db()
                assert len(aliases) == 6
                assert aliases[0].native_symbol == "BTC-USD"
                assert aliases[3].native_symbol == "ETH-USD"

    def test_cache_operations(
        self,
        mock_settings: MagicMock,
        mock_repository: MagicMock,
        sample_aliases: list[SymbolAlias],
    ) -> None:
        """Cache operations work with mocked mapper.

        Given: Mapper with mocked load_mappings_from_db,
        When: Cache methods are called,
        Then: Methods complete without error.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(
                SymbolMapperService,
                "load_mappings_from_db",
                return_value=sample_aliases,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mapper.load_cache_if_needed()
            assert mapper.native_to_kraken_ws is not None
            mapper.trigger_cache_invalidation(fail_fast=False)
            assert mapper.native_to_kraken_ws is not None

    def test_load_cache_if_needed_populates_mappings(self) -> None:
        """Cache loading populates all mapping dictionaries.

        Given: Mapper with sample crypto and stock aliases,
        When: load_cache_if_needed is called,
        Then: All exchange format caches are populated via forward/reverse.
        """
        aliases = [
            SymbolAlias(
                native_symbol="ETH-USD", exchange="kraken", channel="ws", exchange_symbol="ETH/USD"
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="kraken",
                channel="rest",
                exchange_symbol="XETHZUSD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="kraken",
                channel="ccxt",
                exchange_symbol="ETH/USD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD", exchange="zonda", channel="ws", exchange_symbol="ETH-USD"
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="walutomat",
                channel="ws",
                exchange_symbol="ETH_USD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="walutomat",
                channel="rest",
                exchange_symbol="ETHUSD",
            ),
            SymbolAlias(
                native_symbol="ETH-USD",
                exchange="polygon",
                channel="rest",
                exchange_symbol="X:ETHUSD",
            ),
            SymbolAlias(
                native_symbol="AAPL", exchange="polygon", channel="rest", exchange_symbol="AAPL"
            ),
        ]
        mapper = _make_mapper_with_empty_cache()
        mock_loader = MagicMock(return_value=aliases)
        cast(Any, mapper).load_mappings_from_db = mock_loader
        mapper._cache_loaded = False
        assert mapper._cache_loaded is False
        _call_original_load_cache_if_needed(mapper)
        mock_loader.assert_called_once()
        assert "ETH-USD" in mapper.native_to_polygon_rest
        assert mapper.native_to_kraken_ws["ETH-USD"] == "ETH/USD"
        assert mapper.kraken_ws_to_native["ETH/USD"] == "ETH-USD"
        assert mapper.native_to_kraken_rest["ETH-USD"] == "XETHZUSD"
        assert mapper.native_to_ccxt["ETH-USD"] == "ETH/USD"
        assert mapper.native_to_polygon_rest["ETH-USD"] == "X:ETHUSD"
        assert mapper.native_to_polygon_rest["AAPL"] == "AAPL"
        assert mapper.native_to_walutomat_ws["ETH-USD"] == "ETH_USD"
        assert mapper.native_to_walutomat_rest["ETH-USD"] == "ETHUSD"
        assert mapper.forward[("kraken", "ws")]["ETH-USD"] == "ETH/USD"
        assert mapper.reverse[("polygon", "rest")]["AAPL"] == "AAPL"
        SymbolMapperService.clear_instance()

    def test_load_cache_if_needed_fail_fast(self) -> None:
        """fail_fast=True propagates loading errors.

        Given: Mapper with load_mappings_from_db that fails,
        When: load_cache_if_needed called with fail_fast=True,
        Then: RuntimeError is propagated.
        """
        mapper = _make_mapper_with_empty_cache()
        failing_loader = MagicMock(side_effect=RuntimeError("db down"))
        cast(Any, mapper).load_mappings_from_db = failing_loader
        mapper._cache_loaded = False
        _call_original_load_cache_if_needed(mapper, fail_fast=False)
        assert mapper._cache_loaded is True
        mapper._cache_loaded = False
        with pytest.raises(RuntimeError, match="db down"):
            _call_original_load_cache_if_needed(mapper, fail_fast=True)
        SymbolMapperService.clear_instance()


class TestToExchangeToNative:
    """Tests for SymbolMapperService.to_exchange and to_native methods."""

    def test_to_exchange_success(self) -> None:
        """Convert native symbol to exchange format via to_exchange.

        Given: Mapper with populated forward dict,
        When: to_exchange is called with valid args,
        Then: Returns the exchange-specific symbol.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.forward = {("kraken", "ws"): {"BTC-USD": "XBT/USD"}}
        assert mapper.to_exchange("BTC-USD", "kraken", "ws") == "XBT/USD"
        SymbolMapperService.clear_instance()

    def test_to_exchange_unknown_raises(self) -> None:
        """Reject unknown native symbol in to_exchange.

        Given: Mapper with populated forward dict,
        When: to_exchange is called with unknown native symbol,
        Then: Raises ValueError.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.forward = {("kraken", "ws"): {"BTC-USD": "XBT/USD"}}
        with pytest.raises(ValueError, match="No alias for ETH-USD on kraken/ws"):
            mapper.to_exchange("ETH-USD", "kraken", "ws")
        SymbolMapperService.clear_instance()

    def test_to_exchange_unknown_exchange_raises(self) -> None:
        """Reject unknown exchange/channel in to_exchange.

        Given: Mapper with populated forward dict,
        When: to_exchange is called with unknown exchange,
        Then: Raises ValueError.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.forward = {("kraken", "ws"): {"BTC-USD": "XBT/USD"}}
        with pytest.raises(ValueError, match="No alias for BTC-USD on binance/ws"):
            mapper.to_exchange("BTC-USD", "binance", "ws")
        SymbolMapperService.clear_instance()

    def test_to_native_success(self) -> None:
        """Convert exchange symbol to native format via to_native.

        Given: Mapper with populated reverse dict,
        When: to_native is called with valid args,
        Then: Returns the native symbol.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.reverse = {("kraken", "ws"): {"XBT/USD": "BTC-USD"}}
        assert mapper.to_native("XBT/USD", "kraken", "ws") == "BTC-USD"
        SymbolMapperService.clear_instance()

    def test_to_native_unknown_raises(self) -> None:
        """Reject unknown exchange symbol in to_native.

        Given: Mapper with populated reverse dict,
        When: to_native is called with unknown exchange symbol,
        Then: Raises ValueError.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.reverse = {("kraken", "ws"): {"XBT/USD": "BTC-USD"}}
        with pytest.raises(ValueError, match="No native symbol for ETH/USD on kraken/ws"):
            mapper.to_native("ETH/USD", "kraken", "ws")
        SymbolMapperService.clear_instance()

    def test_to_native_unknown_exchange_raises(self) -> None:
        """Reject unknown exchange/channel in to_native.

        Given: Mapper with populated reverse dict,
        When: to_native is called with unknown exchange,
        Then: Raises ValueError.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper.reverse = {("kraken", "ws"): {"XBT/USD": "BTC-USD"}}
        with pytest.raises(ValueError, match="No native symbol for XBT/USD on binance/ws"):
            mapper.to_native("XBT/USD", "binance", "ws")
        SymbolMapperService.clear_instance()


class TestDatabaseSymbolMapperFunctions:
    """Tests for symbol mapper conversion functions."""

    @pytest.fixture
    def mock_global_mapper(self) -> MagicMock:
        """Create mock mapper with sample cache data."""
        mapper = MagicMock()
        mapper.native_to_kraken_ws = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.native_to_kraken_rest = {"BTC-USD": "XXBTZUSD", "ETH-USD": "XETHZUSD"}
        mapper.native_to_ccxt = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.kraken_ws_to_native = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.kraken_rest_to_native = {"XXBTZUSD": "BTC-USD", "XETHZUSD": "ETH-USD"}
        mapper.ccxt_to_native = {
            "BTC-USD": "BTC-USD",
            "ETH-USD": "ETH-USD",
        }
        return mapper

    def test_native_to_kraken_websocket_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert native symbol to Kraken WebSocket format.

        Given: Native symbol in mapper cache,
        When: native_to_kraken_websocket is called,
        Then: Returns WebSocket format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = native_to_kraken_websocket("BTC-USD")
            assert result == "BTC-USD"

    def test_native_to_kraken_websocket_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown native symbol for WebSocket.

        Given: Native symbol not in mapper cache,
        When: native_to_kraken_websocket is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown native symbol: UNKNOWN-USD"),
        ):
            native_to_kraken_websocket("UNKNOWN-USD")

    def test_native_to_kraken_rest_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert native symbol to Kraken REST format.

        Given: Native symbol in mapper cache,
        When: native_to_kraken_rest is called,
        Then: Returns REST format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = native_to_kraken_rest("ETH-USD")
            assert result == "XETHZUSD"

    def test_native_to_kraken_rest_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown native symbol for REST.

        Given: Native symbol not in mapper cache,
        When: native_to_kraken_rest is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown native symbol: UNKNOWN-USD"),
        ):
            native_to_kraken_rest("UNKNOWN-USD")

    def test_native_to_ccxt_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert native symbol to CCXT format.

        Given: Native symbol in mapper cache,
        When: native_to_ccxt is called,
        Then: Returns CCXT format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = native_to_ccxt("BTC-USD")
            assert result == "BTC-USD"

    def test_native_to_ccxt_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown native symbol for CCXT.

        Given: Native symbol not in mapper cache,
        When: native_to_ccxt is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown native symbol: UNKNOWN-USD"),
        ):
            native_to_ccxt("UNKNOWN-USD")

    def test_kraken_websocket_to_native_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert Kraken WebSocket symbol to native.

        Given: WebSocket symbol in mapper cache,
        When: kraken_websocket_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = kraken_websocket_to_native("ETH-USD")
            assert result == "ETH-USD"

    def test_kraken_websocket_to_native_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown WebSocket symbol.

        Given: WebSocket symbol not in mapper cache,
        When: kraken_websocket_to_native is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown Kraken WebSocket v2 symbol: UNKNOWN"),
        ):
            kraken_websocket_to_native("UNKNOWN")

    def test_kraken_rest_to_native_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert Kraken REST symbol to native.

        Given: REST symbol in mapper cache,
        When: kraken_rest_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = kraken_rest_to_native("XXBTZUSD")
            assert result == "BTC-USD"

    def test_kraken_rest_to_native_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown REST symbol.

        Given: REST symbol not in mapper cache,
        When: kraken_rest_to_native is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown Kraken REST symbol: UNKNOWN"),
        ):
            kraken_rest_to_native("UNKNOWN")

    def test_ccxt_to_native_success(self, mock_global_mapper: MagicMock) -> None:
        """Convert CCXT symbol to native.

        Given: CCXT symbol in mapper cache,
        When: ccxt_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = ccxt_to_native("ETH-USD")
            assert result == "ETH-USD"

    def test_ccxt_to_native_unknown(self, mock_global_mapper: MagicMock) -> None:
        """Reject unknown CCXT symbol.

        Given: CCXT symbol not in mapper cache,
        When: ccxt_to_native is called,
        Then: Raises ValueError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
                return_value=mock_global_mapper,
            ),
            pytest.raises(ValueError, match="Unknown CCXT symbol: UNKNOWN"),
        ):
            ccxt_to_native("UNKNOWN")

    def test_validate_symbol_valid(self, mock_global_mapper: MagicMock) -> None:
        """Validate known symbols return True.

        Given: Symbol in mapper cache,
        When: validate_symbol is called,
        Then: Returns True.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = validate_symbol("BTC-USD")
            assert result is True
            assert validate_symbol("ETH-USD") is True

    def test_validate_symbol_invalid(self, mock_global_mapper: MagicMock) -> None:
        """Validate unknown symbols return False.

        Given: Symbol not in mapper cache,
        When: validate_symbol is called,
        Then: Returns False.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = validate_symbol("UNKNOWN")
            assert result is False

    def test_get_available_ws_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get sorted list of WebSocket symbols.

        Given: Mapper with native_to_kraken_ws cache,
        When: get_available_ws_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = get_available_ws_symbols()
            expected = ["BTC-USD", "ETH-USD"]
            assert result == expected

    def test_get_available_kraken_rest_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get sorted list of Kraken REST symbols.

        Given: Mapper with native_to_kraken_rest cache,
        When: get_available_kraken_rest_symbols is called,
        Then: Returns sorted list of REST symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions.SymbolMapperService.get_instance",
            return_value=mock_global_mapper,
        ):
            result = get_available_kraken_rest_symbols()
            expected = ["XETHZUSD", "XXBTZUSD"]
            assert result == expected

    def test_get_available_kraken_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get sorted list of Kraken native symbols.

        Given: Mapper with Kraken symbol mappings,
        When: get_available_kraken_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        mock_mapper = mock_global_mapper.return_value
        mock_mapper.native_to_kraken_ws = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
        with patch("snapper.infrastructure.symbols.functions._get_db_mapper", mock_global_mapper):
            result = get_available_kraken_symbols()
            expected = ["BTC-USD", "ETH-USD"]
            assert result == expected

    def test_get_available_walutomat_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get sorted list of Walutomat native symbols.

        Given: Mapper with Walutomat symbol mappings,
        When: get_available_walutomat_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        mock_mapper = mock_global_mapper.return_value
        mock_mapper.walutomat_ws_to_native = {"EUR_PLN": "EUR-PLN", "USD_PLN": "USD-PLN"}
        with patch("snapper.infrastructure.symbols.functions._get_db_mapper", mock_global_mapper):
            result = get_available_walutomat_symbols()
            expected = ["EUR-PLN", "USD-PLN"]
            assert result == expected

    def test_get_available_polygon_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get sorted list of Polygon native symbols.

        Given: Mapper with Polygon symbol mappings,
        When: get_available_polygon_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        mock_mapper = mock_global_mapper.return_value
        mock_mapper.polygon_rest_to_native = {
            "X:BTCUSD": "BTC-USD",
            "C:EURUSD": "EUR-USD",
            "AAPL": "AAPL",
        }
        with patch("snapper.infrastructure.symbols.functions._get_db_mapper", mock_global_mapper):
            result = get_available_polygon_symbols()
            expected = ["AAPL", "BTC-USD", "EUR-USD"]
            assert result == expected
            assert result == expected

    def test_get_available_symbols(self, mock_global_mapper: MagicMock) -> None:
        """Get combined sorted list of all symbols.

        Given: Multiple exchange symbol functions,
        When: get_available_symbols is called,
        Then: Returns sorted deduplicated union.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.functions.get_available_kraken_symbols",
                return_value=["BTC-USD", "ETH-USD"],
            ),
            patch(
                "snapper.infrastructure.symbols.functions.get_available_zonda_symbols",
                return_value=["BTC-PLN", "ETH-PLN"],
            ),
            patch(
                "snapper.infrastructure.symbols.functions.get_available_walutomat_symbols",
                return_value=["EUR-PLN", "USD-PLN"],
            ),
            patch(
                "snapper.infrastructure.symbols.functions.get_available_polygon_symbols",
                return_value=["BTC-USD", "EUR-USD"],
            ),
        ):
            result = get_available_symbols()
            expected = ["BTC-PLN", "BTC-USD", "ETH-PLN", "ETH-USD", "EUR-PLN", "EUR-USD", "USD-PLN"]
            assert result == expected


class TestDatabaseSymbolMapperZMQIntegration:
    """Tests for SymbolMapperService ZMQ cache invalidation."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings with test configuration."""
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        settings.zmq_broker_xpub = "tcp://localhost:5556"
        return settings

    @pytest.fixture
    def mock_repository(self) -> MagicMock:
        """Create mock repository with session context manager."""
        repository = MagicMock()
        session = MagicMock()
        repository.get_session.return_value.__enter__.return_value = session
        return repository

    def test_trigger_cache_invalidation_public(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Public invalidation method is callable.

        Given: Mapper instance,
        When: trigger_cache_invalidation called with both modes,
        Then: No exception is raised.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "load_mappings_from_db", return_value=[]),
        ):
            mapper = SymbolMapperService()
            try:
                mapper.trigger_cache_invalidation(fail_fast=False)
                mapper.trigger_cache_invalidation(fail_fast=True)
            except Exception as e:
                pytest.fail(f"trigger_cache_invalidation should not raise: {e}")


class TestSymbolMapperEdgeCases:
    """Tests for SymbolMapperService edge cases."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings with test configuration."""
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        settings.zmq_broker_xpub = "tcp://localhost:5556"
        return settings

    def test_init_warmup_failure_raises(self, mock_settings: MagicMock) -> None:
        """Repository initialization failure propagates.

        Given: DatabaseRepository that raises RuntimeError,
        When: SymbolMapperService is instantiated,
        Then: RuntimeError is propagated.
        """
        SymbolMapperService.clear_instance()
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                side_effect=RuntimeError("DB connection failed"),
            ),
            pytest.raises(RuntimeError, match="DB connection failed"),
        ):
            SymbolMapperService()

    def test_load_cache_if_needed_skips_when_loaded(self, mock_settings: MagicMock) -> None:
        """Skip cache load when already loaded flag is set.

        Given: Mapper with _cache_loaded=True,
        When: load_cache_if_needed is called,
        Then: load_mappings_from_db is not called.
        """
        mapper = _make_mapper_with_empty_cache()
        mapper._cache_loaded = True
        with patch.object(mapper, "load_mappings_from_db") as mock_load:
            mapper.load_cache_if_needed()
            mock_load.assert_not_called()


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Provide mock SymbolMapperService with preconfigured mappings."""
    mapper = MagicMock(spec=SymbolMapperService)
    mapper.native_to_zonda_ws = {"BTC-PLN": "BTC-PLN", "ETH-PLN": "ETH-PLN"}
    mapper.zonda_ws_to_native = {"BTC-PLN": "BTC-PLN", "ETH-PLN": "ETH-PLN"}
    mapper.native_to_walutomat_ws = {"EUR-PLN": "EUR_PLN", "USD-PLN": "USD_PLN"}
    mapper.walutomat_ws_to_native = {"EUR_PLN": "EUR-PLN", "USD_PLN": "USD-PLN"}
    mapper.native_to_walutomat_rest = {"EUR-PLN": "EURPLN", "USD-PLN": "USDPLN"}
    mapper.walutomat_rest_to_native = {"EURPLN": "EUR-PLN", "USDPLN": "USD-PLN"}
    mapper.native_to_polygon_rest = {"BTC-USD": "X:BTCUSD", "EUR-USD": "C:EURUSD"}
    mapper.polygon_rest_to_native = {"X:BTCUSD": "BTC-USD", "C:EURUSD": "EUR-USD"}
    mapper.native_to_kraken_ws = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
    mapper.kraken_ws_to_native = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}
    mapper.native_to_ccxt = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
    mapper.ccxt_to_native = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}
    return mapper


class TestZondaHelpers:
    """Tests for Zonda symbol conversion helpers."""

    def test_native_to_zonda_ws_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Zonda format.

        Given: Native symbol in Zonda mapping cache,
        When: native_to_zonda_ws is called,
        Then: Returns Zonda format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_zonda_ws("BTC-PLN")
            assert result == "BTC-PLN"

    def test_native_to_zonda_ws_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Zonda.

        Given: Native symbol not in Zonda mapping,
        When: native_to_zonda_ws is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Zonda\): INVALID-SYMBOL",
        ):
            native_to_zonda_ws("INVALID-SYMBOL")

    def test_zonda_ws_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Zonda symbol to native format.

        Given: Zonda symbol in mapping cache,
        When: zonda_ws_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = zonda_ws_to_native("BTC-PLN")
            assert result == "BTC-PLN"

    def test_zonda_ws_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Zonda symbol.

        Given: Zonda symbol not in mapping cache,
        When: zonda_ws_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Zonda WebSocket symbol: INVALID-SYMBOL"):
            zonda_ws_to_native("INVALID-SYMBOL")

    def test_get_available_zonda_symbols_returns_sorted_list(self, mock_mapper: MagicMock) -> None:
        """Get sorted list of Zonda native symbols.

        Given: Mapper with Zonda symbol mappings,
        When: get_available_zonda_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = get_available_zonda_symbols()
            assert result == ["BTC-PLN", "ETH-PLN"]


class TestWalutomatHelpers:
    """Tests for Walutomat symbol conversion helpers."""

    def test_native_to_walutomat_ws_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Walutomat format.

        Given: Native symbol in Walutomat mapping,
        When: native_to_walutomat_ws is called,
        Then: Returns Walutomat format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_walutomat_ws("EUR-PLN")
            assert result == "EUR_PLN"

    def test_native_to_walutomat_ws_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Walutomat.

        Given: Native symbol not in Walutomat mapping,
        When: native_to_walutomat_ws is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Walutomat WS\): INVALID-SYMBOL",
        ):
            native_to_walutomat_ws("INVALID-SYMBOL")

    def test_walutomat_ws_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Walutomat symbol to native format.

        Given: Walutomat symbol in mapping cache,
        When: walutomat_ws_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = walutomat_ws_to_native("EUR_PLN")
            assert result == "EUR-PLN"

    def test_walutomat_ws_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Walutomat symbol.

        Given: Walutomat symbol not in mapping cache,
        When: walutomat_ws_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Walutomat WebSocket symbol: INVALID_SYMBOL"):
            walutomat_ws_to_native("INVALID_SYMBOL")

    def test_native_to_walutomat_rest_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Walutomat REST format.

        Given: Native symbol in REST mapping,
        When: native_to_walutomat_rest is called,
        Then: Returns REST format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_walutomat_rest("EUR-PLN")
            assert result == "EURPLN"

    def test_native_to_walutomat_rest_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Walutomat REST.

        Given: Native symbol not in REST mapping,
        When: native_to_walutomat_rest is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Walutomat REST\): INVALID-SYMBOL",
        ):
            native_to_walutomat_rest("INVALID-SYMBOL")

    def test_walutomat_rest_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Walutomat REST symbol to native.

        Given: REST symbol in mapping cache,
        When: walutomat_rest_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = walutomat_rest_to_native("EURPLN")
            assert result == "EUR-PLN"

    def test_walutomat_rest_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Walutomat REST symbol.

        Given: REST symbol not in mapping cache,
        When: walutomat_rest_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Walutomat REST symbol: INVALID_SYMBOL"):
            walutomat_rest_to_native("INVALID_SYMBOL")

    def test_get_available_walutomat_rest_symbols_returns_sorted_list(
        self, mock_mapper: MagicMock
    ) -> None:
        """Get sorted list of Walutomat REST symbols.

        Given: Mapper with Walutomat REST mappings,
        When: get_available_walutomat_rest_symbols is called,
        Then: Returns sorted list of REST symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = get_available_walutomat_rest_symbols()
            assert result == ["EURPLN", "USDPLN"]

    def test_get_available_walutomat_symbols_returns_sorted_native_format(
        self, mock_mapper: MagicMock
    ) -> None:
        """Get sorted list of Walutomat native symbols.

        Given: Mapper with Walutomat symbol mappings,
        When: get_available_walutomat_symbols is called,
        Then: Returns sorted list of native symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = get_available_walutomat_symbols()
            assert result == ["EUR-PLN", "USD-PLN"]


class TestPolygonHelpers:
    """Tests for Polygon symbol conversion helpers."""

    def test_native_to_polygon_rest_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Polygon format.

        Given: Native symbol in Polygon mapping,
        When: native_to_polygon_rest is called,
        Then: Returns Polygon format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_polygon_rest("BTC-USD")
            assert result == "X:BTCUSD"

    def test_native_to_polygon_rest_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Polygon.

        Given: Native symbol not in Polygon mapping,
        When: native_to_polygon_rest is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Polygon\): INVALID-SYMBOL",
        ):
            native_to_polygon_rest("INVALID-SYMBOL")

    def test_polygon_rest_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Polygon symbol to native format.

        Given: Polygon symbol in mapping cache,
        When: polygon_rest_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = polygon_rest_to_native("X:BTCUSD")
            assert result == "BTC-USD"

    def test_polygon_rest_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Polygon symbol.

        Given: Polygon symbol not in mapping cache,
        When: polygon_rest_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Polygon REST symbol: INVALID:SYMBOL"):
            polygon_rest_to_native("INVALID:SYMBOL")

    def test_get_available_polygon_rest_symbols_returns_sorted_list(
        self, mock_mapper: MagicMock
    ) -> None:
        """Get sorted list of Polygon REST symbols.

        Given: Mapper with Polygon symbol mappings,
        When: get_available_polygon_rest_symbols is called,
        Then: Returns sorted list of Polygon symbols.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = get_available_polygon_rest_symbols()
            assert result == ["C:EURUSD", "X:BTCUSD"]


class TestCompositeHelpers:
    """Tests for composite symbol conversion helpers."""

    def test_kraken_websocket_to_ccxt_success(self, mock_mapper: MagicMock) -> None:
        """Convert Kraken WebSocket to CCXT via native.

        Given: WebSocket symbol in mapper cache,
        When: kraken_websocket_to_ccxt is called,
        Then: Returns CCXT format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = kraken_websocket_to_ccxt("BTC/USD")
            assert result == "BTC/USD"

    def test_ccxt_to_kraken_websocket_success(self, mock_mapper: MagicMock) -> None:
        """Convert CCXT to Kraken WebSocket via native.

        Given: CCXT symbol in mapper cache,
        When: ccxt_to_kraken_websocket is called,
        Then: Returns WebSocket format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = ccxt_to_kraken_websocket("BTC/USD")
            assert result == "BTC/USD"


class TestGetAvailableExchanges:
    """Tests for get_available_exchanges function."""

    def test_get_available_exchanges_returns_trading_exchanges(self) -> None:
        """Return list of trading exchanges only.

        Given: No parameters,
        When: get_available_exchanges is called,
        Then: Returns sorted list excluding data-only exchanges.
        """
        result = get_available_exchanges()
        assert result == ["kraken", "paper", "walutomat", "zonda"]
        assert "polygon" not in result

    def test_get_market_subscribe_exchanges_returns_live_feeds(self) -> None:
        """Return list of live market feed exchanges only.

        Given: No parameters,
        When: get_market_subscribe_exchanges is called,
        Then: Returns live feed exchanges without paper or polygon.
        """
        result = get_market_subscribe_exchanges()
        assert result == ["kraken", "walutomat", "zonda"]
        assert "paper" not in result
        assert "polygon" not in result

    def test_get_market_data_exchanges_includes_polygon(self) -> None:
        """Return list of market data exchanges including polygon.

        Given: No parameters,
        When: get_market_data_exchanges is called,
        Then: Returns exchanges valid for market data, including polygon.
        """
        result = get_market_data_exchanges()
        assert result == ["kraken", "polygon", "walutomat", "zonda"]
        assert "paper" not in result

    def test_market_subscribe_exchange_excludes_paper_and_polygon(self) -> None:
        """Verify MarketSubscribeExchange excludes paper and polygon.

        Given: MarketSubscribeExchange Literal type,
        When: Its args are inspected,
        Then: Paper and polygon are absent.
        """
        args = get_args(MarketSubscribeExchange)
        assert "paper" not in args
        assert "polygon" not in args
        assert "kraken" in args

    def test_market_data_exchange_includes_polygon_excludes_paper(self) -> None:
        """Verify MarketDataExchange includes polygon but not paper.

        Given: MarketDataExchange Literal type,
        When: Its args are inspected,
        Then: Polygon is present, paper is absent.
        """
        args = get_args(MarketDataExchange)
        assert "polygon" in args
        assert "paper" not in args

    def test_all_exchange_is_superset(self) -> None:
        """Verify AllExchange covers all domain-specific types.

        Given: AllExchange Literal type,
        When: Its args are compared to OrderExchange and MarketDataExchange,
        Then: AllExchange is a superset of both.
        """
        all_args = set(get_args(AllExchange))
        assert set(get_args(OrderExchange)).issubset(all_args)
        assert set(get_args(MarketDataExchange)).issubset(all_args)
        assert set(get_args(MarketSubscribeExchange)).issubset(all_args)


class TestCapabilityInfo:
    """Tests for CapabilityInfo NamedTuple."""

    def test_capability_info_fields(self) -> None:
        """Access all fields on CapabilityInfo.

        Given: A CapabilityInfo with all fields set,
        When: Individual fields are accessed,
        Then: Each field returns the expected value.
        """
        cap = CapabilityInfo(
            can_market_data=True,
            can_trade=False,
            source="kraken_updater",
            reason="symbol delisted",
        )
        assert cap.can_market_data is True
        assert cap.can_trade is False
        assert cap.source == "kraken_updater"
        assert cap.reason == "symbol delisted"

    def test_capability_info_equality(self) -> None:
        """Two identical CapabilityInfo instances are equal.

        Given: Two CapabilityInfo instances with the same values,
        When: They are compared with ==,
        Then: They are equal.
        """
        cap_a = CapabilityInfo(True, True, "source_a", None)
        cap_b = CapabilityInfo(True, True, "source_a", None)
        assert cap_a == cap_b


class TestLoadCapabilitiesFromDb:
    """Tests for SymbolMapperService.load_capabilities_from_db."""

    @pytest.fixture(autouse=True)
    def clear_singleton(self) -> None:
        """Clear singleton instance before each test."""
        SymbolMapperService.clear_instance()

    def test_load_capabilities_from_db_no_such_table(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Return empty list when capabilities table does not exist.

        Given: Session that raises 'no such table: symbol_exchange_capabilities',
        When: load_capabilities_from_db is called,
        Then: Returns empty list.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            error = Exception("no such table: symbol_exchange_capabilities")
            mock_session.execute.side_effect = OperationalError(
                "no such table: symbol_exchange_capabilities",
                params=None,
                orig=error,
            )
            result = mapper.load_capabilities_from_db()
            assert result == []

    def test_load_capabilities_from_db_other_error_raises(
        self, mock_settings: MagicMock, mock_repository: MagicMock
    ) -> None:
        """Propagate non-table-missing operational errors.

        Given: Session that raises 'database is locked' error,
        When: load_capabilities_from_db is called,
        Then: Raises OperationalError.
        """
        with (
            patch(
                "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
                return_value=mock_settings,
            ),
            patch(
                "snapper.infrastructure.symbols.mapper.DatabaseRepository",
                return_value=mock_repository,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mock_session = MagicMock()
            mock_repository.get_session.return_value.__enter__.return_value = mock_session
            error = Exception("database is locked")
            mock_session.execute.side_effect = OperationalError(
                "database is locked",
                params=None,
                orig=error,
            )
            with pytest.raises(OperationalError, match="database is locked"):
                mapper.load_capabilities_from_db()


class TestPopulateCapabilitiesFromRows:
    """Tests for SymbolMapperService._populate_capabilities_from_rows."""

    def test_populate_capabilities_from_rows(self) -> None:
        """Build capabilities dict from mock capability rows.

        Given: Mock SymbolExchangeCapability objects with known attributes,
        When: _populate_capabilities_from_rows is called,
        Then: Mapper capabilities dict is populated with correct keys and values.
        """
        mapper = _make_mapper_with_empty_cache()
        row1 = MagicMock(spec=SymbolExchangeCapability)
        row1.native_symbol = "BTC-USD"
        row1.exchange = "kraken"
        row1.can_market_data = True
        row1.can_trade = True
        row1.source = "kraken_updater"
        row1.reason = None
        row2 = MagicMock(spec=SymbolExchangeCapability)
        row2.native_symbol = "ETH-USD"
        row2.exchange = "polygon"
        row2.can_market_data = True
        row2.can_trade = False
        row2.source = "polygon_updater"
        row2.reason = "data only"
        mapper._populate_capabilities_from_rows([row1, row2])
        assert ("BTC-USD", "kraken") in mapper.capabilities
        assert ("ETH-USD", "polygon") in mapper.capabilities
        cap_btc = mapper.capabilities[("BTC-USD", "kraken")]
        assert cap_btc == CapabilityInfo(True, True, "kraken_updater", None)
        cap_eth = mapper.capabilities[("ETH-USD", "polygon")]
        assert cap_eth == CapabilityInfo(True, False, "polygon_updater", "data only")
        SymbolMapperService.clear_instance()


class TestLoadCacheIfNeededCapabilities:
    """Tests for load_cache_if_needed populating capabilities."""

    def test_load_cache_if_needed_populates_capabilities(self) -> None:
        """Cache loading populates capabilities alongside alias mappings.

        Given: Mapper with sample aliases and capability rows,
        When: load_cache_if_needed is called,
        Then: Both alias maps and capabilities dict are populated.
        """
        aliases = [
            SymbolAlias(
                native_symbol="BTC-USD", exchange="kraken", channel="ws", exchange_symbol="BTC/USD"
            ),
        ]
        cap_row = MagicMock(spec=SymbolExchangeCapability)
        cap_row.native_symbol = "BTC-USD"
        cap_row.exchange = "kraken"
        cap_row.can_market_data = True
        cap_row.can_trade = True
        cap_row.source = "kraken_updater"
        cap_row.reason = None
        mapper = _make_mapper_with_empty_cache()
        cast(Any, mapper).load_mappings_from_db = MagicMock(return_value=aliases)
        cast(Any, mapper).load_capabilities_from_db = MagicMock(return_value=[cap_row])
        mapper._cache_loaded = False
        _call_original_load_cache_if_needed(mapper)
        assert mapper.native_to_kraken_ws["BTC-USD"] == "BTC/USD"
        assert ("BTC-USD", "kraken") in mapper.capabilities
        assert mapper.capabilities[("BTC-USD", "kraken")].can_trade is True
        SymbolMapperService.clear_instance()


class TestCapabilityQueryFunctions:
    """Tests for capability query functions: is_tradeable, is_market_data_available, etc."""

    def test_is_tradeable_paper_known_symbol_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Paper exchange returns True for symbol that has aliases.

        Given: A symbol that exists in forward maps on paper exchange,
        When: is_tradeable is called,
        Then: Returns True.
        """
        mock_mapper = MagicMock()
        mock_mapper.forward = {
            ("kraken", "ws"): {"BTC-USD": "XBT/USD"},
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_tradeable("BTC-USD", "paper") is True

    def test_is_tradeable_paper_unknown_symbol_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Paper exchange returns False for symbol without any aliases.

        Given: A nonexistent symbol on paper exchange,
        When: is_tradeable is called,
        Then: Returns False (no aliases exist).
        """
        mock_mapper = MagicMock()
        mock_mapper.forward = {
            ("kraken", "ws"): {"BTC-USD": "XBT/USD"},
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_tradeable("NONEXISTENT", "paper") is False

    def test_is_tradeable_with_capability_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return True when capability row has can_trade=True.

        Given: Mapper with (BTC-USD, kraken) can_trade=True,
        When: is_tradeable is called,
        Then: Returns True.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_tradeable("BTC-USD", "kraken") is True

    def test_is_tradeable_with_capability_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return False when capability row has can_trade=False.

        Given: Mapper with (BTC-USD, polygon) can_trade=False,
        When: is_tradeable is called,
        Then: Returns False.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "polygon"): CapabilityInfo(True, False, "polygon_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_tradeable("BTC-USD", "polygon") is False

    def test_is_tradeable_missing_row_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return False when no capability row exists (default-deny).

        Given: Mapper with no capability for (ETH-USD, kraken),
        When: is_tradeable is called,
        Then: Returns False.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {}
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_tradeable("ETH-USD", "kraken") is False

    def test_is_market_data_available_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return True when capability row has can_market_data=True.

        Given: Mapper with (BTC-USD, kraken) can_market_data=True,
        When: is_market_data_available is called,
        Then: Returns True.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_market_data_available("BTC-USD", "kraken") is True

    def test_is_market_data_available_missing_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return False when no capability row exists (default-deny).

        Given: Mapper with no capability for (ETH-USD, polygon),
        When: is_market_data_available is called,
        Then: Returns False.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {}
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_market_data_available("ETH-USD", "polygon") is False

    def test_is_market_data_available_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return False when capability row has can_market_data=False.

        Given: Mapper with (BTC-USD, zonda) can_market_data=False,
        When: is_market_data_available is called,
        Then: Returns False.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "zonda"): CapabilityInfo(False, True, "zonda_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        assert is_market_data_available("BTC-USD", "zonda") is False

    def test_get_tradeable_symbols_kraken(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return sorted tradeable symbols for a live exchange.

        Given: Mapper with capabilities for kraken,
        When: get_tradeable_symbols('kraken') is called,
        Then: Returns sorted list of symbols with can_trade=True.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
            ("ETH-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
            ("XRP-USD", "kraken"): CapabilityInfo(True, False, "kraken_updater", "restricted"),
            ("BTC-USD", "polygon"): CapabilityInfo(True, False, "polygon_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        result = get_tradeable_symbols("kraken")
        assert result == ["BTC-USD", "ETH-USD"]

    def test_get_tradeable_symbols_paper(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return union of all forward map keys for paper exchange.

        Given: Mapper with forward maps for multiple exchanges,
        When: get_tradeable_symbols('paper') is called,
        Then: Returns sorted union of all forward map keys.
        """
        mock_mapper = MagicMock()
        mock_mapper.forward = {
            ("kraken", "ws"): {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"},
            ("zonda", "ws"): {"BTC-USD": "BTC-USD"},
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        result = get_tradeable_symbols("paper")
        assert result == ["BTC-USD", "ETH-USD"]

    def test_get_tradeable_symbols_empty_exchange(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return empty list for exchange with no capabilities.

        Given: Mapper with no capabilities for walutomat,
        When: get_tradeable_symbols('walutomat') is called,
        Then: Returns empty list.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        result = get_tradeable_symbols("walutomat")
        assert result == []

    def test_get_market_data_symbols(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return sorted market data symbols for an exchange.

        Given: Mapper with capabilities for polygon,
        When: get_market_data_symbols('polygon') is called,
        Then: Returns sorted list of symbols with can_market_data=True.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "polygon"): CapabilityInfo(True, False, "polygon_updater", None),
            ("ETH-USD", "polygon"): CapabilityInfo(True, False, "polygon_updater", None),
            ("XRP-USD", "polygon"): CapabilityInfo(False, False, "polygon_updater", "removed"),
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        result = get_market_data_symbols("polygon")
        assert result == ["BTC-USD", "ETH-USD"]

    def test_get_market_data_symbols_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return empty list for exchange with no capabilities.

        Given: Mapper with no capabilities for walutomat,
        When: get_market_data_symbols('walutomat') is called,
        Then: Returns empty list.
        """
        mock_mapper = MagicMock()
        mock_mapper.capabilities = {
            ("BTC-USD", "kraken"): CapabilityInfo(True, True, "kraken_updater", None),
        }
        monkeypatch.setattr(functions, "_get_db_mapper", lambda: mock_mapper)
        result = get_market_data_symbols("walutomat")
        assert result == []
