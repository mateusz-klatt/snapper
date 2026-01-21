"""Tests for symbol mapping service and conversion functions."""

import importlib.util
from functools import lru_cache
from types import ModuleType
from typing import Any
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError

import snapper.infrastructure.symbols.mapper as symbol_mapper_module
from snapper.data.models import SymbolMapping
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
from snapper.infrastructure.symbols.functions import kraken_rest_to_native
from snapper.infrastructure.symbols.functions import kraken_websocket_to_ccxt
from snapper.infrastructure.symbols.functions import kraken_websocket_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_rest
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket
from snapper.infrastructure.symbols.functions import native_to_polygon
from snapper.infrastructure.symbols.functions import native_to_walutomat
from snapper.infrastructure.symbols.functions import native_to_walutomat_rest
from snapper.infrastructure.symbols.functions import native_to_zonda
from snapper.infrastructure.symbols.functions import polygon_to_native
from snapper.infrastructure.symbols.functions import validate_symbol
from snapper.infrastructure.symbols.functions import walutomat_rest_to_native
from snapper.infrastructure.symbols.functions import walutomat_to_native
from snapper.infrastructure.symbols.functions import zonda_to_native
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
        assert len(mapper.native_to_ws) >= 0

    def test_cache_invalidation_pattern(self) -> None:
        """Cache invalidation preserves existing mappings.

        Given: Mapper with loaded cache,
        When: trigger_cache_invalidation is called,
        Then: Cache count remains unchanged.
        """
        mapper = SymbolMapperService()
        initial_count = len(mapper.native_to_ws)
        mapper.trigger_cache_invalidation(fail_fast=False)
        assert len(mapper.native_to_ws) == initial_count

    def test_load_cache_if_needed_simple(self) -> None:
        """Cache loading is idempotent.

        Given: Mapper instance,
        When: load_cache_if_needed is called twice,
        Then: Cache count is same after both calls.
        """
        mapper = SymbolMapperService()
        mapper.load_cache_if_needed()
        first_count = len(mapper.native_to_ws)
        mapper.load_cache_if_needed()
        second_count = len(mapper.native_to_ws)
        assert second_count == first_count

    def test_safe_zmq_invalidation(self) -> None:
        """ZMQ invalidation is safe with fail_fast=False.

        Given: Mapper with loaded cache,
        When: trigger_cache_invalidation called with fail_fast=False,
        Then: Cache remains unchanged.
        """
        mapper = SymbolMapperService()
        mapper.load_cache_if_needed()
        first_count = len(mapper.native_to_ws)
        mapper.trigger_cache_invalidation(fail_fast=False)
        second_count = len(mapper.native_to_ws)
        assert second_count == first_count

    def test_load_mappings_from_db_empty(self) -> None:
        """Load mappings returns list from database.

        Given: Mapper instance,
        When: load_mappings_from_db is called,
        Then: Returns list of SymbolMapping objects.
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
        assert isinstance(mapper.native_to_ws, dict)
        assert isinstance(mapper.native_to_rest, dict)
        assert isinstance(mapper.ws_to_native, dict)
        assert isinstance(mapper.rest_to_native, dict)


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
        error = OperationalError("statement", "params", Exception("no such table: symbol_mappings"))
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
def sample_mapping() -> SymbolMapping:
    """Provide a sample BTC-USD symbol mapping."""
    return SymbolMapping(
        native_symbol="BTC-USD",
        kraken_websocket_symbol="BTC/USD",
        kraken_rest_symbol="XXBTZUSD",
        ccxt_symbol="BTC/USD",
        base_currency="BTC",
        quote_currency="USD",
    )


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
            error = Exception("no such table: symbol_mappings")
            mock_session.execute.side_effect = OperationalError(
                "no such table: symbol_mappings",
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
        self, mock_settings: MagicMock, mock_repository: MagicMock, sample_mapping: SymbolMapping
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
            mock_result.scalars.return_value.all.return_value = [sample_mapping]
            mock_session.execute.return_value = mock_result
            mapper = SymbolMapperService()
            assert "BTC-USD" in mapper.native_to_ws
            old_cache_size = len(mapper.native_to_ws)
            mapper._cache_loaded = False
            mock_session.execute.side_effect = RuntimeError("Database connection failed")
            mapper.load_cache_if_needed(fail_fast=False)
            assert "BTC-USD" in mapper.native_to_ws
            assert len(mapper.native_to_ws) == old_cache_size


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

        def counting_load(self: Any) -> list[SymbolMapping]:
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
    mapper.native_to_ws = {}
    mapper.native_to_rest = {}
    mapper.native_to_ccxt = {}
    mapper.ws_to_native = {}
    mapper.rest_to_native = {}
    mapper.ccxt_to_native = {}
    mapper.native_to_zonda = {}
    mapper.zonda_to_native = {}
    mapper.native_to_walutomat = {}
    mapper.walutomat_to_native = {}
    mapper.native_to_walutomat_rest = {}
    mapper.walutomat_rest_to_native = {}
    mapper.native_to_polygon = {}
    mapper.polygon_to_native = {}
    mapper._cache_loaded = False
    mapper.context = None
    mapper.subscriber = None
    mapper._listening = False
    mapper._listener_task = None
    return mapper


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
    def sample_mappings(self) -> list[SymbolMapping]:
        """Create sample symbol mappings for testing."""
        return [
            SymbolMapping(
                native_symbol="BTC-USD",
                kraken_websocket_symbol="BTC-USD",
                kraken_rest_symbol="XXBTZUSD",
                ccxt_symbol="BTC-USD",
                base_currency="BTC",
                quote_currency="USD",
            ),
            SymbolMapping(
                native_symbol="ETH-USD",
                kraken_websocket_symbol="ETH-USD",
                kraken_rest_symbol="XETHZUSD",
                ccxt_symbol="ETH-USD",
                base_currency="ETH",
                quote_currency="USD",
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
            assert isinstance(mapper.native_to_ws, dict)
            assert isinstance(mapper.native_to_rest, dict)
            assert isinstance(mapper.ws_to_native, dict)
            assert isinstance(mapper.rest_to_native, dict)
            assert hasattr(mapper, "_cache_loaded")

    def test_load_mappings_from_db_success(
        self,
        mock_settings: MagicMock,
        mock_repository: MagicMock,
        sample_mappings: list[SymbolMapping],
    ) -> None:
        """Load mappings from database successfully.

        Given: Mock session returning sample mappings,
        When: load_mappings_from_db is called,
        Then: Returns list of SymbolMapping objects.
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
            mock_result.scalars.return_value.all.return_value = sample_mappings
            mock_session.execute.return_value = mock_result
            with patch.object(SymbolMapperService, "trigger_cache_invalidation"):
                mapper = SymbolMapperService()
                mappings = mapper.load_mappings_from_db()
                assert len(mappings) == 2
                assert mappings[0].native_symbol == "BTC-USD"
                assert mappings[1].native_symbol == "ETH-USD"

    def test_cache_operations(
        self,
        mock_settings: MagicMock,
        mock_repository: MagicMock,
        sample_mappings: list[SymbolMapping],
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
                return_value=sample_mappings,
            ),
            patch.object(SymbolMapperService, "trigger_cache_invalidation"),
        ):
            mapper = SymbolMapperService()
            mapper.load_cache_if_needed()
            assert len(mapper.native_to_ws) >= 0
            mapper.trigger_cache_invalidation(fail_fast=False)
            assert len(mapper.native_to_ws) >= 0

    def test_load_cache_if_needed_populates_mappings(self) -> None:
        """Cache loading populates all mapping dictionaries.

        Given: Mapper with sample crypto and stock mappings,
        When: load_cache_if_needed is called,
        Then: All exchange format caches are populated.
        """
        crypto_mapping = SymbolMapping(
            native_symbol="IGNORED",
            kraken_websocket_symbol="ETH/USD",
            kraken_rest_symbol="XETHZUSD",
            ccxt_symbol="ETH/USD",
            zonda_symbol="ETH-USD",
            walutomat_symbol="ETH_USD",
            walutomat_rest_symbol="ETHUSD",
            polygon_symbol="X:ETHUSD",
            base_currency=" eth ",
            quote_currency=" usd ",
        )
        stock_mapping = SymbolMapping(
            native_symbol="AAPL",
            kraken_websocket_symbol=None,
            kraken_rest_symbol=None,
            ccxt_symbol=None,
            zonda_symbol=None,
            walutomat_symbol=None,
            walutomat_rest_symbol=None,
            polygon_symbol="AAPL",
            base_currency="AAPL",
            quote_currency=None,
        )
        mapper = _make_mapper_with_empty_cache()
        mock_loader = MagicMock(return_value=[crypto_mapping, stock_mapping])
        cast(Any, mapper).load_mappings_from_db = mock_loader
        mapper._cache_loaded = False
        assert mapper._cache_loaded is False
        _call_original_load_cache_if_needed(mapper)
        mock_loader.assert_called_once()
        assert "ETH-USD" in mapper.native_to_polygon
        assert mapper.native_to_ws["ETH-USD"] == "ETH/USD"
        assert mapper.ws_to_native["ETH/USD"] == "ETH-USD"
        assert mapper.native_to_rest["ETH-USD"] == "XETHZUSD"
        assert mapper.native_to_ccxt["ETH-USD"] == "ETH/USD"
        assert mapper.native_to_polygon["ETH-USD"] == "X:ETHUSD"
        assert mapper.native_to_polygon["AAPL"] == "AAPL"
        assert mapper.native_to_walutomat["ETH-USD"] == "ETH_USD"
        assert mapper.native_to_walutomat_rest["ETH-USD"] == "ETHUSD"
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


class TestDatabaseSymbolMapperFunctions:
    """Tests for symbol mapper conversion functions."""

    @pytest.fixture
    def mock_global_mapper(self) -> MagicMock:
        """Create mock mapper with sample cache data."""
        mapper = MagicMock()
        mapper.native_to_ws = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.native_to_rest = {"BTC-USD": "XXBTZUSD", "ETH-USD": "XETHZUSD"}
        mapper.native_to_ccxt = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.ws_to_native = {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"}
        mapper.rest_to_native = {"XXBTZUSD": "BTC-USD", "XETHZUSD": "ETH-USD"}
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

        Given: Mapper with native_to_ws cache,
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

        Given: Mapper with native_to_rest cache,
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
        mock_mapper.native_to_ws = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
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
        mock_mapper.walutomat_to_native = {"EUR_PLN": "EUR-PLN", "USD_PLN": "USD-PLN"}
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
        mock_mapper.polygon_to_native = {
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
    mapper.native_to_zonda = {"BTC-PLN": "BTC-PLN", "ETH-PLN": "ETH-PLN"}
    mapper.zonda_to_native = {"BTC-PLN": "BTC-PLN", "ETH-PLN": "ETH-PLN"}
    mapper.native_to_walutomat = {"EUR-PLN": "EUR_PLN", "USD-PLN": "USD_PLN"}
    mapper.walutomat_to_native = {"EUR_PLN": "EUR-PLN", "USD_PLN": "USD-PLN"}
    mapper.native_to_walutomat_rest = {"EUR-PLN": "EURPLN", "USD-PLN": "USDPLN"}
    mapper.walutomat_rest_to_native = {"EURPLN": "EUR-PLN", "USDPLN": "USD-PLN"}
    mapper.native_to_polygon = {"BTC-USD": "X:BTCUSD", "EUR-USD": "C:EURUSD"}
    mapper.polygon_to_native = {"X:BTCUSD": "BTC-USD", "C:EURUSD": "EUR-USD"}
    mapper.native_to_ws = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
    mapper.ws_to_native = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}
    mapper.native_to_ccxt = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
    mapper.ccxt_to_native = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}
    return mapper


class TestZondaHelpers:
    """Tests for Zonda symbol conversion helpers."""

    def test_native_to_zonda_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Zonda format.

        Given: Native symbol in Zonda mapping cache,
        When: native_to_zonda is called,
        Then: Returns Zonda format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_zonda("BTC-PLN")
            assert result == "BTC-PLN"

    def test_native_to_zonda_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Zonda.

        Given: Native symbol not in Zonda mapping,
        When: native_to_zonda is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Zonda\): INVALID-SYMBOL",
        ):
            native_to_zonda("INVALID-SYMBOL")

    def test_zonda_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Zonda symbol to native format.

        Given: Zonda symbol in mapping cache,
        When: zonda_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = zonda_to_native("BTC-PLN")
            assert result == "BTC-PLN"

    def test_zonda_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Zonda symbol.

        Given: Zonda symbol not in mapping cache,
        When: zonda_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Zonda symbol: INVALID-SYMBOL"):
            zonda_to_native("INVALID-SYMBOL")

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

    def test_native_to_walutomat_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Walutomat format.

        Given: Native symbol in Walutomat mapping,
        When: native_to_walutomat is called,
        Then: Returns Walutomat format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_walutomat("EUR-PLN")
            assert result == "EUR_PLN"

    def test_native_to_walutomat_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Walutomat.

        Given: Native symbol not in Walutomat mapping,
        When: native_to_walutomat is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Walutomat\): INVALID-SYMBOL",
        ):
            native_to_walutomat("INVALID-SYMBOL")

    def test_walutomat_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Walutomat symbol to native format.

        Given: Walutomat symbol in mapping cache,
        When: walutomat_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = walutomat_to_native("EUR_PLN")
            assert result == "EUR-PLN"

    def test_walutomat_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Walutomat symbol.

        Given: Walutomat symbol not in mapping cache,
        When: walutomat_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Walutomat symbol: INVALID_SYMBOL"):
            walutomat_to_native("INVALID_SYMBOL")

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

    def test_native_to_polygon_success(self, mock_mapper: MagicMock) -> None:
        """Convert native symbol to Polygon format.

        Given: Native symbol in Polygon mapping,
        When: native_to_polygon is called,
        Then: Returns Polygon format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = native_to_polygon("BTC-USD")
            assert result == "X:BTCUSD"

    def test_native_to_polygon_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown native symbol for Polygon.

        Given: Native symbol not in Polygon mapping,
        When: native_to_polygon is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(
            ValueError,
            match=r"Unknown native symbol \(not available on Polygon\): INVALID-SYMBOL",
        ):
            native_to_polygon("INVALID-SYMBOL")

    def test_polygon_to_native_success(self, mock_mapper: MagicMock) -> None:
        """Convert Polygon symbol to native format.

        Given: Polygon symbol in mapping cache,
        When: polygon_to_native is called,
        Then: Returns native format symbol.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ):
            result = polygon_to_native("X:BTCUSD")
            assert result == "BTC-USD"

    def test_polygon_to_native_unknown_symbol_raises_value_error(
        self, mock_mapper: MagicMock
    ) -> None:
        """Reject unknown Polygon symbol.

        Given: Polygon symbol not in mapping cache,
        When: polygon_to_native is called,
        Then: Raises ValueError.
        """
        with patch(
            "snapper.infrastructure.symbols.functions._get_db_mapper",
            return_value=mock_mapper,
        ), pytest.raises(ValueError, match=r"Unknown Polygon symbol: INVALID:SYMBOL"):
            polygon_to_native("INVALID:SYMBOL")

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
