"""Persist Kraken fiat identity without changing aliases or temporal history."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.kraken import _KrakenPairInfo
from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.core.types import AssetTypeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository

_FIRST = datetime(2026, 10, 1, 12, tzinfo=UTC)


@dataclass
class FiatCatalog:
    """Bind the actual updater writer to isolated synchronous SQLite sessions."""

    updater: KrakenSymbolUpdaterService
    sessions: sessionmaker[Session]

    def rest_record(self, base: str, quote: str) -> KrakenSymbolRecord:
        """Resolve raw Kraken legs through the actual REST mapping builder."""
        pair = _KrakenPairInfo(base=base, quote=quote, ccxt_symbol=f"{base}/{quote}")
        mappings, _ = self.updater._build_mappings_from_rest({f"{base}{quote}": pair})
        return next(iter(mappings.values()))

    async def write(self, record: KrakenSymbolRecord, at: datetime) -> None:
        """Persist one mapping with a deterministic producer timestamp."""
        with patch("snapper.application.updaters.symbols.kraken.datetime") as clock:
            clock.now.return_value = at
            await self.updater._update_database([dict(record)])

    def history(self) -> list[Symbol]:
        """Read all durable symbol versions in their creation order."""
        with self.sessions() as session:
            return list(session.scalars(select(Symbol).order_by(Symbol.timestamp)))

    def seed(self, record: KrakenSymbolRecord, asset_type: AssetTypeEnum) -> str:
        """Seed an earlier writer version using the shared symbol upsert."""
        with self.sessions() as session:
            public_id = self.updater._upsert_symbol(
                session,
                record["native_symbol"],
                record["base_currency"],
                record["quote_currency"],
                asset_type,
                _FIRST,
                session_id="previous-writer",
                sequence_id=1,
            )
            session.commit()
            return str(public_id)


@pytest.fixture
def catalog() -> Iterator[FiatCatalog]:
    """Provide a real isolated catalog without connecting to a venue or broker."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessions
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
    updater.repository = repository
    try:
        yield FiatCatalog(updater, sessions)
    finally:
        engine.dispose()


@pytest.mark.parametrize(("base", "quote"), [("ZEUR", "ZUSD"), ("ZGBP", "ZJPY")])
async def test_rest_fiat_pair_persists_forex(catalog: FiatCatalog, base: str, quote: str) -> None:
    """Recognize both normalized fiat legs at the durable writer boundary.

    Given: Raw Kraken REST metadata for two supported fiat currencies,
    When: The actual mapping builder and SQLite writer process the pair,
    Then: Its normalized symbol is durably classified as forex.
    """
    record = catalog.rest_record(base, quote)
    await catalog.write(record, _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == AssetTypeEnum.FOREX
    assert (symbol.base, symbol.quote) == (base[1:], quote[1:])
    assert symbol.native_symbol == f"{base[1:]}-{quote[1:]}"


@pytest.mark.parametrize("prior_type", [AssetTypeEnum.CRYPTO, AssetTypeEnum.FOREX])
async def test_fiat_correction_preserves_identity_and_history(
    catalog: FiatCatalog, prior_type: AssetTypeEnum
) -> None:
    """Correct one classification while retaining history and avoiding repeat churn.

    Given: An earlier crypto or forex EUR-USD version from the shared writer,
    When: Kraken persists the resolved fiat pair twice at increasing timestamps,
    Then: Only a required correction creates a successor with stable public identity.
    """
    record = catalog.rest_record("ZEUR", "ZUSD")
    public_id = catalog.seed(record, prior_type)
    correction_at = _FIRST + timedelta(minutes=1)
    await catalog.write(record, correction_at)
    first_history = catalog.history()
    assert first_history[-1].asset_type == AssetTypeEnum.FOREX
    await catalog.write(record, _FIRST + timedelta(minutes=2))
    history = catalog.history()
    assert [row.id for row in history] == [row.id for row in first_history]
    assert {row.public_id for row in history} == {public_id}
    assert history[0].asset_type == prior_type
    assert history[0].timestamp == _FIRST
    assert history[-1].known_to == KNOWN_TO_MAX
    if prior_type == AssetTypeEnum.CRYPTO:
        assert len(history) == 2
        assert history[0].known_to == correction_at
        assert history[1].timestamp == correction_at
    else:
        assert len(history) == 1


def _assert_aliases_and_capability(
    catalog: FiatCatalog, expected_aliases: set[tuple[str, str]], can_trade: bool
) -> None:
    """Verify persisted routing identifiers and the original trading capability."""
    with catalog.sessions() as session:
        aliases: list[SymbolAlias] = list(session.scalars(select(SymbolAlias)))
        assert {(row.channel, row.exchange_symbol) for row in aliases} == expected_aliases
        assert len(aliases) == len(expected_aliases)
        assert all(row.known_to == KNOWN_TO_MAX for row in aliases)
        capabilities: list[SymbolExchangeCapability] = list(
            session.scalars(select(SymbolExchangeCapability))
        )
        (capability,) = capabilities
        assert capability.can_market_data is True
        assert capability.can_trade is can_trade
        assert capability.source == "kraken_updater"
        assert {row.symbol_public_id for row in aliases} == {capability.symbol_public_id}
        assert capability.symbol_public_id == catalog.history()[-1].public_id


@pytest.mark.parametrize(
    ("base", "quote", "expected"),
    [
        ("XXBT", "ZUSD", AssetTypeEnum.CRYPTO),
        ("ZEUR", "USDT", AssetTypeEnum.CRYPTO),
        ("USD", "USDT", AssetTypeEnum.CRYPTO),
        ("XETH", "XXBT", AssetTypeEnum.CRYPTO),
        ("BOB", "USD", AssetTypeEnum.CRYPTO),
        ("XYZ", "EUR", AssetTypeEnum.CRYPTO),
        ("USD", "XYZ", AssetTypeEnum.CRYPTO),
        ("ZCAD", "ZCHF", AssetTypeEnum.FOREX),
        ("ZAUD", "ZGBP", AssetTypeEnum.FOREX),
    ],
)
async def test_rest_classification_preserves_routing_and_capability(
    catalog: FiatCatalog, base: str, quote: str, expected: AssetTypeEnum
) -> None:
    """Keep crypto and unknown legs distinct from supported fiat pairs.

    Given: REST pairs spanning known fiat, crypto, stablecoin and unknown codes,
    When: Their actual mapping is persisted,
    Then: Both-leg classification preserves all aliases and tradable capability.
    """
    record = catalog.rest_record(base, quote)
    await catalog.write(record, _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == expected
    assert (symbol.base, symbol.quote) == (record["base_currency"], record["quote_currency"])
    _assert_aliases_and_capability(
        catalog,
        {
            ("ws", record["kraken_websocket_symbol"]),
            ("rest", record["kraken_rest_symbol"]),
            ("ccxt", record["ccxt_symbol"]),
        },
        True,
    )


async def test_tokenized_fiat_named_equity_takes_precedence(catalog: FiatCatalog) -> None:
    """Preserve tokenized equity even when resolved legs resemble a fiat pair.

    Given: A tokenized EURx/USD REST record whose resolved base is EUR,
    When: The actual tokenized resolver and writer persist it,
    Then: Equity classification wins while its original routing aliases remain.
    """
    pair = _KrakenPairInfo(base="EURx", quote="ZUSD", asset_class="tokenized_asset")
    mappings, _ = catalog.updater._build_mappings_from_rest({"EURxUSD": pair})
    record = mappings["EUR"]
    assert (record["base_currency"], record["quote_currency"]) == ("EUR", "USD")
    await catalog.write(record, _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == AssetTypeEnum.EQUITY
    assert symbol.native_symbol == "EUR"
    _assert_aliases_and_capability(catalog, {("ws", "EURx/USD"), ("rest", "EURxUSD")}, True)


@pytest.mark.parametrize(
    ("base", "quote", "expected"),
    [
        ("EUR", "USD", AssetTypeEnum.FOREX),
        ("GBP", "JPY", AssetTypeEnum.FOREX),
        ("ZEUR", "ZUSD", AssetTypeEnum.CRYPTO),
        ("eur", "usd", AssetTypeEnum.CRYPTO),
        ("EURx", "USD", AssetTypeEnum.EQUITY),
        ("EUR", "USDT", AssetTypeEnum.CRYPTO),
        ("BOB", "USD", AssetTypeEnum.CRYPTO),
    ],
)
async def test_ws_mapping_preserves_raw_leg_contract(
    catalog: FiatCatalog, base: str, quote: str, expected: AssetTypeEnum
) -> None:
    """Classify canonical WS legs without introducing new normalization.

    Given: WS-only discovery with canonical, raw, tokenized or crypto legs,
    When: The real WS builder and writer persist the mapping,
    Then: Its classification and raw alias preserve the market-data-only contract.
    """
    wire = f"{base}/{quote}"
    record = catalog.updater._build_ws_only_mapping({"symbol": wire, "base": base, "quote": quote})
    await catalog.write(record, _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == expected
    assert (symbol.base, symbol.quote) == (base, quote)
    _assert_aliases_and_capability(catalog, {("ws", wire)}, False)


@pytest.mark.parametrize(
    ("wire", "expected"),
    [("EUR/USD:BTNL", AssetTypeEnum.FOREX), ("BTC/USD:BTNL", AssetTypeEnum.CRYPTO)],
)
async def test_btnl_discovery_uses_uniform_classification(
    catalog: FiatCatalog, wire: str, expected: AssetTypeEnum
) -> None:
    """Apply the same leg rule to relayed spot records without changing eligibility.

    Given: An isolated fake ticker capture containing one BTNL wire symbol,
    When: Actual BTNL discovery and the SQLite writer process that symbol,
    Then: Classification preserves the suffix, WS alias and market-data-only capability.
    """
    client = MagicMock()
    client.collect_raw_ticker_symbols = AsyncMock(return_value={wire})
    with patch.object(catalog.updater, "_create_exchange_client", return_value=client):
        mappings = await catalog.updater.discover_btnl_symbols()
    (record,) = mappings.values()
    await catalog.write(record, _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == expected
    assert symbol.native_symbol == wire.replace("/", "-").replace(":", "-")
    _assert_aliases_and_capability(catalog, {("ws", wire)}, False)
    with catalog.sessions() as session:
        capabilities: list[SymbolExchangeCapability] = list(
            session.scalars(select(SymbolExchangeCapability))
        )
        (capability,) = capabilities
        assert capability.reason is not None
        assert "Bitnomial spot venue" in capability.reason


@pytest.mark.parametrize("currency", ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"])
@pytest.mark.parametrize("as_base", [True, False])
async def test_each_supported_fiat_code_works_on_either_leg(
    catalog: FiatCatalog, currency: str, as_base: bool
) -> None:
    """Use precisely the established fiat vocabulary on either pair leg.

    Given: Each of the seven existing supported fiat codes paired with USD,
    When: The real REST mapper and SQLite writer see it on either leg,
    Then: The symbol is classified as forex without adding any currency codes.
    """
    base, quote = (currency, "USD") if as_base else ("USD", currency)
    await catalog.write(catalog.rest_record(base, quote), _FIRST)
    (symbol,) = catalog.history()
    assert symbol.asset_type == AssetTypeEnum.FOREX
