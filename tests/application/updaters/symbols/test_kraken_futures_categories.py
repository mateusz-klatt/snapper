"""Published Kraken Futures categories and stable catalog identity witnesses.

The fixtures preserve relevant fields from the official public instruments
endpoint captured on 2026-10-22 at 23:21 UTC:
https://futures.kraken.com/derivatives/api/v3/instruments
These categories classify catalog exposure, not account eligibility or trading
readiness. Tests never infer product ownership or permissions from a category.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.kraken_futures import KrakenFuturesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import _classify_asset_type
from snapper.core.types import AssetTypeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema

_CATEGORIES = (
    ("xStocks", AssetTypeEnum.EQUITY),
    ("Equities", AssetTypeEnum.EQUITY),
    ("Pre-IPO", AssetTypeEnum.EQUITY),
    ("Commodities", AssetTypeEnum.COMMODITY),
    ("Forex", AssetTypeEnum.FOREX),
    ("Indices", AssetTypeEnum.INDEX),
)
_CAPTURED: tuple[tuple[dict[str, object], AssetTypeEnum], ...] = (
    (
        {
            "symbol": "PF_AAPLXUSD",
            "category": "xStocks",
            "tradfi": False,
            "base": "AAPLx",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 2,
            "tickSize": 0.01,
            "tradeable": True,
        },
        AssetTypeEnum.EQUITY,
    ),
    (
        {
            "symbol": "PF_NBISUSD",
            "category": "Equities",
            "tradfi": False,
            "base": "NBIS",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 2,
            "tickSize": 0.01,
            "tradeable": True,
        },
        AssetTypeEnum.EQUITY,
    ),
    (
        {
            "symbol": "PF_OPENAIXUSD",
            "category": "Pre-IPO",
            "tradfi": False,
            "base": "OPENAIx",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 2,
            "tickSize": 0.01,
            "tradeable": True,
        },
        AssetTypeEnum.EQUITY,
    ),
    (
        {
            "symbol": "PF_XAUUSD",
            "category": "Commodities",
            "tradfi": False,
            "base": "XAU",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 3,
            "tickSize": 0.01,
            "tradeable": True,
        },
        AssetTypeEnum.COMMODITY,
    ),
    (
        {
            "symbol": "PF_EURUSD",
            "category": "Forex",
            "tradfi": True,
            "base": "EUR",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 0,
            "tickSize": 1e-05,
            "tradeable": True,
        },
        AssetTypeEnum.FOREX,
    ),
    (
        {
            "symbol": "PF_US100USD",
            "category": "Indices",
            "tradfi": False,
            "base": "US100",
            "quote": "USD",
            "type": "flexible_futures",
            "contractSize": 1,
            "contractValueTradePrecision": 4,
            "tickSize": 1,
            "tradeable": True,
        },
        AssetTypeEnum.INDEX,
    ),
)
_OBSERVED_AT = datetime(2026, 10, 22, 23, 21, tzinfo=UTC)


def _schema(**overrides: object) -> KrakenFuturesInstrumentSchema:
    """Build complete metadata while deliberately avoiding ticker-based class evidence."""
    raw: dict[str, object] = {
        "symbol": "opaque-market-id",
        "type": "flexible_futures",
        "base": "XBT",
        "quote": "USD",
        "contractSize": 1,
        "contractValueTradePrecision": 2,
        "tickSize": 0.01,
        "tradeable": True,
    }
    raw.update(overrides)
    return KrakenFuturesInstrumentSchema.model_validate(raw)


@pytest.fixture
def catalog_session() -> Iterator[Session]:
    """Provide an isolated real schema for temporal catalog identity assertions.

    Yields:
        A real synchronous session containing no seeded market instruments.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            yield session
    finally:
        engine.dispose()


@pytest.mark.parametrize(("category", "expected"), _CATEGORIES)
@pytest.mark.parametrize("tradfi", (False, True))
@pytest.mark.parametrize("symbol", ("opaque-market-id", "PF_XBTUSD"))
def test_published_categories_override_broad_flags_and_ordinary_ticker_spelling(
    category: str, expected: AssetTypeEnum, tradfi: bool, symbol: str
) -> None:
    """Published categories establish class even when broad flags or names disagree.

    Given: A recognized category with either tradfi flag and an opaque or crypto-like ticker,
    When: The updater interprets the venue's category,
    Then: Its exact catalog class dominates both unrelated pieces of metadata.
    """
    assert (
        _classify_asset_type(_schema(category=category, tradfi=tradfi, symbol=symbol)) == expected
    )


@pytest.mark.parametrize(("category", "expected"), _CATEGORIES)
def test_category_whitespace_and_case_do_not_change_published_meaning(
    category: str, expected: AssetTypeEnum
) -> None:
    """Normalize only category spelling rather than ticker or product semantics.

    Given: A supported category with surrounding whitespace and mixed casing,
    When: The updater resolves its catalog class,
    Then: It retains the same exact category mapping.
    """
    assert _classify_asset_type(_schema(category=f" \t{category.swapcase()}\n")) == expected


@pytest.mark.parametrize("symbol", ("rr_xbtusd", "RR_CUSTOM", "in_xbtusd", "IN_CUSTOM"))
@pytest.mark.parametrize("category", (None, "xStocks", "Forex", "Commodities"))
@pytest.mark.parametrize("tradfi", (False, True))
def test_reference_and_index_feeds_keep_precedence_over_product_categories(
    symbol: str, category: str | None, tradfi: bool
) -> None:
    """Reference feed identities remain index data independently of contract categories.

    Given: A recognized reference/index feed prefix and contradictory product metadata,
    When: The updater classifies the feed,
    Then: It remains INDEX before any category or broad-flag fallback.
    """
    assert (
        _classify_asset_type(_schema(symbol=symbol, category=category, tradfi=tradfi))
        == AssetTypeEnum.INDEX
    )


@pytest.mark.parametrize(
    "category", (None, "", "  ", "DeFi", "unpublished", "xStock", "Commodities extra")
)
@pytest.mark.parametrize("tradfi", (False, True))
def test_missing_unknown_and_similar_category_names_preserve_legacy_fallback(
    category: str | None, tradfi: bool
) -> None:
    """Unknown classifications do not gain speculative category semantics.

    Given: An absent, unknown or merely similar category spelling,
    When: The explicit category map has no authoritative match,
    Then: The existing tradfi INDEX and ordinary CRYPTO fallback remain compatible.
    """
    expected = AssetTypeEnum.INDEX if tradfi else AssetTypeEnum.CRYPTO
    assert _classify_asset_type(_schema(category=category, tradfi=tradfi)) == expected


@pytest.mark.parametrize(("raw", "expected"), _CAPTURED)
def test_current_public_catalog_contradictions_use_the_published_category(
    raw: dict[str, object], expected: AssetTypeEnum
) -> None:
    """Current xStocks, stocks, pre-IPO, commodities, Forex and indices classify correctly.

    Given: Relevant fields retained from six officially published product definitions,
    When: The actual schema and updater classify the captured metadata,
    Then: The category corrects the current tradfi-only misclassification.
    """
    assert _classify_asset_type(KrakenFuturesInstrumentSchema.model_validate(raw)) == expected


@pytest.mark.parametrize(("raw", "expected"), _CAPTURED)
def test_category_correction_revises_symbol_class_without_replacing_durable_identity(
    catalog_session: Session, raw: dict[str, object], expected: AssetTypeEnum
) -> None:
    """A real catalog correction retains symbol/instrument IDs and blocked execution.

    Given: A persisted legacy fallback classification for an exact current product,
    When: The same instrument is refreshed with its authoritative category,
    Then: Only temporal symbol classification changes while aliases, UUIDs and trade refusal remain.
    """
    updater = KrakenFuturesSymbolUpdaterService()
    legacy = KrakenFuturesInstrumentSchema.model_validate({**raw, "category": None})
    symbol_id = updater._persist_instrument_schema(catalog_session, legacy, _OBSERVED_AT)
    catalog_session.commit()
    before = catalog_session.execute(select(Instrument)).scalar_one()
    instrument_id = before.public_id
    original_spec_id = catalog_session.execute(select(InstrumentSpec)).scalar_one().public_id
    aliases = catalog_session.execute(
        select(SymbolAlias.public_id, SymbolAlias.channel, SymbolAlias.exchange_symbol).order_by(
            SymbolAlias.id
        )
    ).all()
    refreshed_at = _OBSERVED_AT + timedelta(seconds=1)
    resolved_id = updater._persist_instrument_schema(
        catalog_session, KrakenFuturesInstrumentSchema.model_validate(raw), refreshed_at
    )
    catalog_session.commit()
    catalog_session.expire_all()
    versions = list(catalog_session.scalars(select(Symbol).order_by(Symbol.timestamp)))
    assert resolved_id == symbol_id
    assert len(versions) == 2
    assert {row.public_id for row in versions} == {symbol_id}
    assert len({(row.native_symbol, row.base, row.quote, row.created_at) for row in versions}) == 1
    assert versions[0].known_to == refreshed_at
    assert versions[1].asset_type == expected
    assert versions[1].known_to == KNOWN_TO_MAX
    assert catalog_session.execute(select(Instrument)).scalar_one().public_id == instrument_id
    assert (
        catalog_session.execute(
            select(
                SymbolAlias.public_id, SymbolAlias.channel, SymbolAlias.exchange_symbol
            ).order_by(SymbolAlias.id)
        ).all()
        == aliases
    )
    spec = catalog_session.execute(
        select(InstrumentSpec).where(InstrumentSpec.known_to == KNOWN_TO_MAX)
    ).scalar_one()
    assert spec.public_id == original_spec_id
    assert spec.native_order_quantity_unit == "base_asset"
    capabilities = list(catalog_session.scalars(select(SymbolExchangeCapability)))
    assert capabilities
    assert all(row.can_trade is False for row in capabilities)
    assert all(row.can_market_data is True for row in capabilities)
