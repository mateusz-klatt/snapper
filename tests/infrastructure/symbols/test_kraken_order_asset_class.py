"""Bind native Kraken order asset classes to active Spot catalog aliases."""

import importlib.util
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import ModuleType
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

import snapper.infrastructure.symbols.functions as functions
import snapper.infrastructure.symbols.mapper as mapper_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.symbols.mapper import SymbolMapperService

_LISTED = datetime(2026, 10, 22, 10, tzinfo=UTC)


@dataclass(frozen=True)
class CatalogProduct:
    """One catalog product and its native-to-venue alias."""

    native: str
    asset_type: str
    exchange: str
    wire: str
    channel: str = "rest"
    known_to: datetime = KNOWN_TO_MAX


@dataclass
class ClassCatalog:
    """Expose a real catalog reader over one isolated SQLite database."""

    mapper: SymbolMapperService
    sessions: sessionmaker[Session]

    def seed(self, product: CatalogProduct) -> None:
        """Persist precisely the symbol and alias used by the class query."""
        with self.sessions() as session:
            symbol = Symbol(
                native_symbol=product.native,
                base=product.native.partition("-")[0],
                quote="USD",
                asset_type=product.asset_type,
                created_at=_LISTED,
                timestamp=_LISTED,
                session_id="catalog-test",
                sequence_id=1,
            )
            session.add(symbol)
            session.flush()
            session.add(
                SymbolAlias(
                    symbol_public_id=symbol.public_id,
                    exchange=product.exchange,
                    channel=product.channel,
                    exchange_symbol=product.wire,
                    created_at=_LISTED,
                    timestamp=_LISTED,
                    known_to=product.known_to,
                    session_id="catalog-test",
                    sequence_id=1,
                )
            )
            session.commit()


@pytest.fixture
def class_catalog() -> Iterator[ClassCatalog]:
    """Create only the two real tables read by the asset-class cache."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine, tables=[Base.metadata.tables["symbols"], Base.metadata.tables["symbol_aliases"]]
    )
    sessions = sessionmaker(bind=engine)
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessions
    mapper = object.__new__(SymbolMapperService)
    mapper.repository = repository
    mapper.kraken_asset_classes = {}
    mapper._cache_loaded = False
    try:
        yield ClassCatalog(mapper, sessions)
    finally:
        engine.dispose()


def _original_mapper_module() -> ModuleType:
    """Load the production cache method without the global test alias fixture."""
    spec = importlib.util.spec_from_file_location(
        "kraken_class_mapper_original", mapper_module.__file__
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_asset_class_uses_catalog_identity_instead_of_wire_spelling(
    class_catalog: ClassCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compact, slash, and non-x token names share their attested product class.

    Given: Token, crypto, FCM-only, ordinary stock, and retired catalog products,
    When: The actual active Spot REST catalog classifies native order identities,
    Then: Tokens retain their class and non-Spot or retired identities remain unclassified.
    """
    products = [
        CatalogProduct("AAPL", "equity", "kraken", "AAPLxUSD"),
        CatalogProduct("AAPLX-EUR", "equity", "kraken", "AAPLx/EUR"),
        CatalogProduct("SPV", "equity", "kraken", "SPVUSD"),
        CatalogProduct("HEX-USD", "crypto", "kraken", "HEXUSD"),
        CatalogProduct("ES", "index", "kraken_equities", "ESZ6", "ws"),
        CatalogProduct("MSFT", "equity", "polygon", "MSFT"),
        CatalogProduct(
            "OLD", "equity", "kraken", "OLDxUSD", known_to=_LISTED + timedelta(minutes=1)
        ),
    ]
    for product in products:
        class_catalog.seed(product)
    mapper = class_catalog.mapper
    mapper.kraken_asset_classes = mapper.load_kraken_asset_classes_from_db()
    monkeypatch.setattr(functions, "_get_db_mapper", lambda: mapper)
    assert mapper.kraken_asset_classes == {
        "AAPL": "tokenized_asset",
        "AAPLX-EUR": "tokenized_asset",
        "SPV": "tokenized_asset",
        "HEX-USD": "currency",
    }
    assert functions.native_to_kraken_asset_class("SPV") == "tokenized_asset"
    assert functions.native_to_kraken_asset_class("HEX-USD") == "currency"
    for absent in ("ES", "MSFT", "OLD", "UNKNOWN"):
        with pytest.raises(ValueError, match="Unknown Kraken order asset class"):
            functions.native_to_kraken_asset_class(absent)


def test_asset_class_cache_reloads_with_current_aliases(class_catalog: ClassCatalog) -> None:
    """A cold cache and later invalidation load the same current alias and class universe.

    Given: A cold mapper followed by a new native token catalog product,
    When: The original cache loader runs before and after invalidation,
    Then: Alias and asset-class maps refresh together from the current catalog.
    """
    class_catalog.seed(CatalogProduct("AAPL", "equity", "kraken", "AAPLxUSD"))
    mapper = class_catalog.mapper
    original = _original_mapper_module()
    original.SymbolMapperService.load_cache_if_needed(mapper, fail_fast=True)
    assert mapper.native_to_kraken_rest == {"AAPL": "AAPLxUSD"}
    assert mapper.kraken_asset_classes == {"AAPL": "tokenized_asset"}
    class_catalog.seed(CatalogProduct("SPV", "equity", "kraken", "SPVUSD"))
    mapper._cache_loaded = False
    original.SymbolMapperService.load_cache_if_needed(mapper, fail_fast=True)
    assert mapper.native_to_kraken_rest == {"AAPL": "AAPLxUSD", "SPV": "SPVUSD"}
    assert mapper.kraken_asset_classes == {"AAPL": "tokenized_asset", "SPV": "tokenized_asset"}


def test_asset_class_cache_tolerates_pre_migration_tables() -> None:
    """Bootstrap can start before the symbol tables exist without fabricating classes.

    Given: An isolated database without symbol or alias tables,
    When: The asset-class loader runs before migrations,
    Then: It reports an empty catalog without inventing executable product classes.
    """
    engine = create_engine("sqlite:///:memory:")
    sessions = sessionmaker(bind=engine)
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessions
    mapper = object.__new__(SymbolMapperService)
    mapper.repository = repository
    try:
        assert mapper.load_kraken_asset_classes_from_db() == {}
    finally:
        engine.dispose()


def test_asset_class_cache_does_not_hide_unrelated_database_failures() -> None:
    """A locked or unavailable database remains an error instead of an empty catalog.

    Given: A symbol catalog read failing because the database is locked,
    When: The asset-class loader handles that operational error,
    Then: The actual failure propagates instead of masquerading as an empty catalog.
    """
    mapper = object.__new__(SymbolMapperService)
    repository = MagicMock(spec=DatabaseRepository)
    session = MagicMock()
    session.execute.side_effect = OperationalError("select", {}, Exception("database is locked"))
    repository.get_session.return_value.__enter__.return_value = session
    mapper.repository = repository
    with pytest.raises(OperationalError, match="database is locked"):
        mapper.load_kraken_asset_classes_from_db()


def test_asset_class_cache_refuses_a_mixed_alias_generation(class_catalog: ClassCatalog) -> None:
    """Product classes must match the exact REST aliases loaded in the same cache cycle.

    Given: A current token catalog alias that differs from an earlier alias observation,
    When: The class loader binds its result to the earlier cache alias tuples,
    Then: That product remains unclassified instead of mixing old routing with new class evidence.
    """
    class_catalog.seed(CatalogProduct("AAPL", "equity", "kraken", "AAPLxUSD"))
    assert (
        class_catalog.mapper.load_kraken_asset_classes_from_db(
            [
                ("AAPL", "kraken", "rest", "AAPLUSD"),
                ("AAPL", "polygon", "rest", "AAPL"),
                ("AAPL", "kraken", "ws", "AAPLx/USD"),
            ]
        )
        == {}
    )
    assert class_catalog.mapper.load_kraken_asset_classes_from_db(
        [
            ("AAPL", "kraken", "rest", "AAPLxUSD"),
        ]
    ) == {"AAPL": "tokenized_asset"}


def test_asset_class_cache_tolerates_a_missing_alias_table() -> None:
    """Bootstrap distinguishes a missing alias table from unrelated operational failures.

    Given: An isolated bootstrap database containing only the symbol table,
    When: The catalog class loader joins the not-yet-created alias table,
    Then: It reports an empty catalog without inventing a product class.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[Base.metadata.tables["symbols"]])
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessionmaker(bind=engine)
    mapper = object.__new__(SymbolMapperService)
    mapper.repository = repository
    try:
        assert mapper.load_kraken_asset_classes_from_db() == {}
    finally:
        engine.dispose()
