"""Keep tokenized spot products, precision evidence, and order routing distinct."""

from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import replace
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

import snapper.infrastructure.symbols.functions as symbol_functions
from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.kraken import _rest_decimal_places
from snapper.application.updaters.symbols.kraken import _rest_positive_number
from snapper.application.updaters.symbols.kraken import _tokenized_precision_complete
from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_rest_egress import KrakenRestOperationKind
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget
from snapper.infrastructure.symbols.mapper import SymbolMapperService

_OBSERVED = datetime(2026, 10, 22, 10, tzinfo=UTC)


def _pair(**changes: JsonValue) -> JsonObject:
    """Return venue-shaped metadata for one explicitly tokenized REST product."""
    return {
        "base": "AAPLx",
        "quote": "ZUSD",
        "tick_size": "0.01",
        "pair_decimals": 2,
        "lot_decimals": 6,
        "ordermin": "0.001",
        "costmin": "0.5",
        "cost_decimals": 4,
        "status": "online",
        **changes,
    }


@dataclass
class TokenCatalog:
    """Bind the actual catalog extractor and writer to isolated SQLite state."""

    updater: KrakenSymbolUpdaterService
    sessions: sessionmaker[Session]

    def record(self, payload: JsonObject, rest_alias: str = "AAPLxUSD") -> KrakenSymbolRecord:
        """Build a research record through the actual raw REST parsing path."""
        pair = self.updater._extract_tokenized_pair(rest_alias, payload, _OBSERVED)
        assert pair is not None
        records, _ = self.updater._build_mappings_from_rest({rest_alias: pair})
        return next(iter(records.values()))

    async def write(self, records: list[KrakenSymbolRecord], minutes: int = 0) -> None:
        """Persist one atomic observation with a deterministic producer timestamp."""
        with patch("snapper.application.updaters.symbols.kraken.datetime") as clock:
            clock.now.return_value = _OBSERVED + timedelta(minutes=minutes)
            await self.updater._update_database([dict(record) for record in records])


@pytest.fixture
def token_catalog() -> Iterator[TokenCatalog]:
    """Provide the real schema without exchange, broker, or production access."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessions
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService()
    updater.repository = repository
    try:
        yield TokenCatalog(updater, sessions)
    finally:
        engine.dispose()


async def test_token_quotes_keep_separate_routes_and_legacy_usd_identity(
    token_catalog: TokenCatalog,
) -> None:
    """Adding EUR cannot overwrite an existing USD xStock's symbol or aliases.

    Given: An existing USD token product and a newly discovered EUR quote,
    When: The real writer refreshes both products together,
    Then: Their quote identities and aliases remain distinct with stable USD identity.
    """
    usd = token_catalog.record(_pair())
    await token_catalog.write([usd])
    with token_catalog.sessions() as session:
        legacy_public_id = session.scalars(select(Symbol)).one().public_id
    eur = token_catalog.record(_pair(quote="ZEUR"), "AAPLxEUR")
    await token_catalog.write([eur, usd], minutes=1)
    with token_catalog.sessions() as session:
        symbols = {row.native_symbol: row for row in session.scalars(select(Symbol))}
        assert set(symbols) == {"AAPL", "AAPLX-EUR"}
        assert symbols["AAPL"].public_id == legacy_public_id
        assert (symbols["AAPL"].base, symbols["AAPL"].quote) == ("AAPL", "USD")
        assert (symbols["AAPLX-EUR"].base, symbols["AAPLX-EUR"].quote) == ("AAPLx", "EUR")
        aliases = list(session.scalars(select(SymbolAlias)))
        assert {(row.channel, row.exchange_symbol) for row in aliases} == {
            ("ws", "AAPLx/USD"),
            ("rest", "AAPLxUSD"),
            ("ws", "AAPLx/EUR"),
            ("rest", "AAPLxEUR"),
        }
        assert all(row.known_to == KNOWN_TO_MAX for row in aliases)


async def test_token_metadata_retains_venue_minima_and_distinct_provenance(
    token_catalog: TokenCatalog,
) -> None:
    """Direct REST precision and cost minima cannot masquerade as CCXT certification.

    Given: Complete raw tokenized AssetPairs constraints,
    When: The extractor and writer persist the product,
    Then: Venue precision and quote minimum retain distinct un-certified provenance.
    """
    await token_catalog.write([token_catalog.record(_pair())])
    with token_catalog.sessions() as session:
        spec = session.scalars(select(InstrumentSpec)).one()
        assert (spec.tick_size, spec.lot_size, spec.min_order_size) == (0.01, 0.000001, 0.001)
        assert (spec.qty_decimals, spec.cost_decimals, spec.status) == (6, 4, "online")
        assert spec.spec_source == "kraken:rest.AssetPairs:tokenized_asset"
        assert spec.spec_observed_at == _OBSERVED
        assert spec.quantity_unit == "base_asset"
        assert spec.unit_certified is False
        assert spec.native_order_quantity_unit is None
        capability = session.scalars(select(InstrumentOrderCapability)).one()
        assert capability.min_notional == 0.5
        assert capability.supported_order_types == ["market", "limit", "stop", "stop_limit"]
        assert capability.supports_leverage is False
        assert capability.supports_short_selling is False
        assert capability.top_of_book_quality == "unknown"
        symbol_capability = session.scalars(select(SymbolExchangeCapability)).one()
        assert symbol_capability.can_trade is False
        assert symbol_capability.reason is not None
        assert "rebase accounting is not integrated" in symbol_capability.reason


@pytest.mark.parametrize("status", ["offline", "cancel_only", "post_only", "limit_only", None, 3])
async def test_token_non_online_status_replaces_rebase_reason_with_metadata_reason(
    token_catalog: TokenCatalog, status: JsonValue
) -> None:
    """An unavailable product reports its current status barrier alongside its execution freeze.

    Given: An online product whose next catalog status is not online,
    When: The new observation is committed,
    Then: The current status barrier replaces the rebase reason without losing market data.
    """
    await token_catalog.write([token_catalog.record(_pair())])
    await token_catalog.write([token_catalog.record(_pair(status=status))], minutes=1)
    with token_catalog.sessions() as session:
        rows = list(
            session.scalars(
                select(SymbolExchangeCapability).order_by(SymbolExchangeCapability.timestamp)
            )
        )
        assert len(rows) == 2
        assert rows[0].can_trade is False
        assert rows[0].reason is not None
        assert "rebase accounting is not integrated" in rows[0].reason
        assert rows[0].known_to == _OBSERVED + timedelta(minutes=1)
        assert rows[1].can_trade is False
        assert rows[1].can_market_data is True
        assert rows[1].reason is not None
        assert "online status and complete order metadata" in rows[1].reason


@pytest.mark.parametrize(
    "field", ["tick_size", "lot_decimals", "ordermin", "costmin", "cost_decimals", "status"]
)
async def test_token_incomplete_metadata_never_advertises_trading(
    token_catalog: TokenCatalog, field: str
) -> None:
    """Missing authoritative constraints remain explicit absence and revoke readiness.

    Given: A token catalog response missing one required execution constraint,
    When: Its metadata and capability are persisted,
    Then: It remains un-certified and cannot advertise order readiness.
    """
    payload = _pair()
    payload.pop(field)
    if field == "tick_size":
        payload.pop("pair_decimals")
    await token_catalog.write([token_catalog.record(payload)])
    with token_catalog.sessions() as session:
        assert session.scalars(select(SymbolExchangeCapability)).one().can_trade is False
        assert session.scalars(select(InstrumentSpec)).one().unit_certified is False


@pytest.mark.parametrize("field", ["tick_size", "ordermin", "costmin"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", "1e999", "1e-999", True, "bad"])
def test_token_invalid_amounts_cannot_produce_preview_precision(
    token_catalog: TokenCatalog, field: str, value: JsonValue
) -> None:
    """Invalid venue amounts cannot be replaced by plausible defaults or positive infinity.

    Given: A token tick or minimum containing an unusable boundary value,
    When: The real catalog parser evaluates preview precision,
    Then: It refuses complete precision instead of substituting an apparently valid amount.
    """
    record = token_catalog.record(_pair(**{field: value}))
    if field == "costmin":
        assert record["min_notional"] is None
    else:
        attribute = "tick_size" if field == "tick_size" else "min_order_size"
        assert getattr(record["metadata"], attribute) is None
    assert _tokenized_precision_complete(record) is False


@pytest.mark.parametrize("value", [None, True, -1, 257, "6", 1.5])
def test_token_decimal_counts_are_strict_and_bounded(value: JsonValue) -> None:
    """Untrusted decimal counts cannot request unbounded quantization or bool precision.

    Given: A noninteger, boolean, negative, or excessively large decimal count,
    When: The REST precision parser reads it,
    Then: It reports explicit absence rather than constructing an unsafe increment.
    """
    assert _rest_decimal_places(value) is None


def test_token_decimal_boundaries_remain_exact() -> None:
    """The accepted precision boundaries include integral and very small quantities.

    Given: Valid endpoints of the accepted precision range and a fractional minimum,
    When: The numeric parsers inspect them,
    Then: The complete supported range and positive small quantity remain usable.
    """
    assert _rest_decimal_places(0) == 0
    assert _rest_decimal_places(256) == 256
    assert _rest_positive_number("0.000001") == 0.000001


def test_token_tick_fallback_uses_authoritative_decimal_count(token_catalog: TokenCatalog) -> None:
    """An omitted tick size can use venue pair decimals without inventing precision.

    Given: A catalog product reporting pair decimals and no tick-size field,
    When: The metadata extractor derives the documented decimal quantum,
    Then: That venue-sourced increment preserves the product's preview precision.
    """
    payload = _pair()
    payload.pop("tick_size")
    record = token_catalog.record(payload)
    assert record["metadata"].tick_size == 0.01
    assert _tokenized_precision_complete(record) is True


def test_tokenized_catalog_attests_non_suffix_products(token_catalog: TokenCatalog) -> None:
    """An explicit tokenized catalog product need not follow the xStock suffix convention.

    Given: SPV explicitly listed by the tokenized AssetPairs endpoint,
    When: The real extractor resolves its product identity,
    Then: The catalog attestation retains its tokenized class without an x suffix.
    """
    record = token_catalog.record(_pair(base="SPV"), "SPVUSD")
    assert record["native_symbol"] == "SPV"
    assert record["asset_class"] == "tokenized_asset"
    assert record["kraken_rest_symbol"] == "SPVUSD"


async def test_internal_token_asset_preserves_exact_catalog_wire_names(
    token_catalog: TokenCatalog,
) -> None:
    """Internal asset ids and displayed WS products must not be conflated.

    Given: A raw AAPLSPV asset with distinct REST id, altname, and WS display alias,
    When: The actual catalog extractor and writer persist its research identity,
    Then: Exact wire aliases survive without stripping SPV or certifying token quantity units.
    """
    record = token_catalog.record(
        _pair(base="AAPLSPV", wsname="AAPLx/USD", altname="AAPLxUSD"), "AAPLSPVUSD"
    )
    assert record["native_symbol"] == "AAPLSPV"
    assert record["base_currency"] == "AAPLSPV"
    assert record["kraken_websocket_symbol"] == "AAPLx/USD"
    assert record["kraken_rest_symbol"] == "AAPLSPVUSD"
    await token_catalog.write([record])
    with token_catalog.sessions() as session:
        assert {row.exchange_symbol for row in session.scalars(select(SymbolAlias))} == {
            "AAPLx/USD",
            "AAPLSPVUSD",
        }
        assert session.scalars(select(InstrumentSpec)).one().native_order_quantity_unit is None
        assert session.scalars(select(SymbolExchangeCapability)).one().can_trade is False


@pytest.mark.parametrize(
    "wsname",
    [
        None,
        3,
        "",
        "AAPLx",
        "AAPLx/",
        "/USD",
        "AAPLx/USD/EXTRA",
        "AAPLx/EUR",
        " AAPLx/USD",
        "AAPLx/ USD",
        "AAPLx/USD\x00",
        "AAPLx/USD\u200b",
    ],
)
def test_invalid_supplied_token_ws_alias_cannot_be_replaced_by_a_guess(
    token_catalog: TokenCatalog, wsname: JsonValue
) -> None:
    """Malformed display aliases and contradictory quotes cannot publish invented routes.

    Given: An explicit malformed WS alias or one contradicting the raw quote,
    When: The real token catalog resolver processes the pair,
    Then: Neither a mapping nor a guessed subscription alias is produced.
    """
    pair = token_catalog.updater._extract_tokenized_pair(
        "AAPLSPVUSD", _pair(base="AAPLSPV", wsname=wsname), _OBSERVED
    )
    assert pair is not None
    mappings, wanted = token_catalog.updater._build_mappings_from_rest({"AAPLSPVUSD": pair})
    assert mappings == {}
    assert wanted == set()


def test_ordinary_spot_catalog_attests_native_base_quantity_only(
    token_catalog: TokenCatalog,
) -> None:
    """Currency Spot quantities have an explicit native convention unlike unproven token units.

    Given: An ordinary currency Spot market and a token catalog product,
    When: Their actual metadata extractors process the catalog observations,
    Then: Only ordinary Spot advertises a native base-asset order quantity unit.
    """
    ordinary = token_catalog.updater._extract_ccxt_market_pair(
        "BTC/USD", {"id": "XXBTZUSD", "base": "BTC", "quote": "USD"}, _OBSERVED
    )
    assert ordinary is not None
    assert ordinary["metadata"].native_order_quantity_unit == "base_asset"
    assert token_catalog.record(_pair())["metadata"].native_order_quantity_unit is None


async def test_token_order_capability_deduplicates_and_versions_cost_minimum(
    token_catalog: TokenCatalog,
) -> None:
    """Repeated constraints keep one identity while changed minima retain temporal history.

    Given: Two identical observations followed by a changed quote minimum,
    When: The token capability writer applies them in order,
    Then: Only the changed minimum creates a successor with the original public identity.
    """
    record = token_catalog.record(_pair())
    await token_catalog.write([record])
    await token_catalog.write([record], minutes=1)
    await token_catalog.write([token_catalog.record(_pair(costmin="1.25"))], minutes=2)
    with token_catalog.sessions() as session:
        rows = list(
            session.scalars(
                select(InstrumentOrderCapability).order_by(InstrumentOrderCapability.timestamp)
            )
        )
        assert len(rows) == 2
        assert rows[0].public_id == rows[1].public_id
        assert rows[0].known_to == _OBSERVED + timedelta(minutes=2)
        assert rows[0].min_notional == 0.5
        assert rows[1].min_notional == 1.25


def test_token_without_metadata_has_incomplete_precision(token_catalog: TokenCatalog) -> None:
    """A legacy mapping alone cannot promise current venue precision or trading status.

    Given: A token mapping with its authoritative metadata removed,
    When: The precision predicate evaluates the legacy record,
    Then: Missing precision and status evidence cannot grant execution permission.
    """
    record = token_catalog.record(_pair())
    record.pop("metadata")
    assert _tokenized_precision_complete(record) is False


def _seed_alias_owner(
    catalog: TokenCatalog,
    native: str,
    aliases: list[tuple[str, str]],
    quote: str | None = "USD",
    base: str | None = None,
) -> str:
    """Persist an earlier product owner without inventing any fill or account state."""
    with catalog.sessions() as session:
        symbol = Symbol(
            native_symbol=native,
            base=base or ("AAPLx" if "-" in native else "AAPL"),
            quote=quote,
            asset_type="equity",
            created_at=_OBSERVED - timedelta(days=1),
            timestamp=_OBSERVED - timedelta(days=1),
            session_id="earlier-catalog",
            sequence_id=1,
        )
        session.add(symbol)
        session.flush()
        for channel, alias in aliases:
            session.add(
                SymbolAlias(
                    symbol_public_id=symbol.public_id,
                    exchange="kraken",
                    channel=channel,
                    exchange_symbol=alias,
                    created_at=_OBSERVED - timedelta(days=1),
                    timestamp=_OBSERVED - timedelta(days=1),
                    session_id="earlier-catalog",
                    sequence_id=1,
                )
            )
        session.commit()
        return str(symbol.public_id)


async def test_complete_token_precision_revokes_legacy_permission_until_rebase_accounting(
    token_catalog: TokenCatalog,
) -> None:
    """Current precision cannot preserve an earlier unsafe token trading permission.

    Given: A legacy token alias owner advertising trading permission,
    When: Complete token metadata refreshes its current capability,
    Then: Its stable identity survives while execution is frozen for missing rebase accounting.
    """
    public_id = _seed_alias_owner(token_catalog, "AAPL", [("ws", "AAPLx/USD")])
    with token_catalog.sessions() as session:
        session.add(
            SymbolExchangeCapability(
                symbol_public_id=public_id,
                exchange="kraken",
                can_market_data=True,
                can_trade=True,
                source="earlier-catalog",
                created_at=_OBSERVED - timedelta(days=1),
                timestamp=_OBSERVED - timedelta(days=1),
                session_id="earlier-catalog",
                sequence_id=1,
            )
        )
        session.commit()
    await token_catalog.write([token_catalog.record(_pair())])
    with token_catalog.sessions() as session:
        rows = list(
            session.scalars(
                select(SymbolExchangeCapability).order_by(SymbolExchangeCapability.timestamp)
            )
        )
        assert len(rows) == 2
        assert rows[0].can_trade is True
        assert rows[0].known_to == _OBSERVED
        assert rows[1].can_trade is False
        assert rows[1].symbol_public_id == public_id
        assert rows[1].reason is not None
        assert "rebase accounting is not integrated" in rows[1].reason


async def test_existing_canonical_ws_owner_keeps_public_identity(
    token_catalog: TokenCatalog,
) -> None:
    """A REST-discovered product upgrades its existing WS-only identity instead of stealing its alias.

    Given: A canonical WS-only product already owning the token's wire alias,
    When: REST discovery adds a supported order route,
    Then: Both aliases belong to its unchanged native and public identity.
    """
    public_id = _seed_alias_owner(token_catalog, "AAPLX-USD", [("ws", "AAPLx/USD")])
    await token_catalog.write([token_catalog.record(_pair())])
    with token_catalog.sessions() as session:
        symbol = session.scalars(select(Symbol)).one()
        assert symbol.native_symbol == "AAPLX-USD"
        assert symbol.public_id == public_id
        assert symbol.base == "AAPLx"
        assert {row.symbol_public_id for row in session.scalars(select(SymbolAlias))} == {public_id}
        assert {row.exchange_symbol for row in session.scalars(select(SymbolAlias))} == {
            "AAPLx/USD",
            "AAPLxUSD",
        }


@pytest.mark.parametrize(("native", "base"), [("AAPL", "AAPL"), ("AAPLSPV-USD", "AAPLSPV")])
async def test_internal_and_display_token_aliases_keep_an_existing_owner(
    token_catalog: TokenCatalog, native: str, base: str
) -> None:
    """An attested raw/display alias relationship cannot create a second historical identity.

    Given: An existing owner identified by either legacy display or raw internal base,
    When: REST discovers its raw SPV id and exact display WS alias together,
    Then: The existing native, base, and public identity remain unchanged.
    """
    public_id = _seed_alias_owner(token_catalog, native, [("ws", "AAPLx/USD")], base=base)
    record = token_catalog.record(
        _pair(base="AAPLSPV", wsname="AAPLx/USD", altname="AAPLxUSD"), "AAPLSPVUSD"
    )
    await token_catalog.write([record])
    with token_catalog.sessions() as session:
        symbol = session.scalars(select(Symbol)).one()
        assert (symbol.native_symbol, symbol.base, symbol.public_id) == (native, base, public_id)
        assert {row.symbol_public_id for row in session.scalars(select(SymbolAlias))} == {public_id}


async def test_internal_token_aliases_cannot_reassign_conflicting_owners(
    token_catalog: TokenCatalog,
) -> None:
    """Separate owners of internal REST and display WS aliases require explicit reconciliation.

    Given: Different active product identities already owning the two attested aliases,
    When: A raw/display token catalog observation would join them,
    Then: The writer refuses instead of reassigning either historical identity.
    """
    _seed_alias_owner(token_catalog, "AAPL", [("ws", "AAPLx/USD")])
    _seed_alias_owner(token_catalog, "AAPLSPV", [("rest", "AAPLSPVUSD")], base="AAPLSPV")
    record = token_catalog.record(_pair(base="AAPLSPV", wsname="AAPLx/USD"), "AAPLSPVUSD")
    with pytest.raises(ValueError, match="Conflicting tokenized alias owners"):
        await token_catalog.write([record])


async def test_ordinary_underlying_identity_is_not_relabeled_as_token(
    token_catalog: TokenCatalog,
) -> None:
    """An existing ordinary AAPL symbol keeps its base and quote while a token gets a distinct product.

    Given: An ordinary underlying ticker without the discovered token's Spot aliases,
    When: Tokenized USD discovery encounters the occupied legacy ticker,
    Then: A separate token product is added without rewriting the ordinary stock identity.
    """
    stock_id = _seed_alias_owner(token_catalog, "AAPL", [], quote=None)
    await token_catalog.write([token_catalog.record(_pair())])
    with token_catalog.sessions() as session:
        symbols = {row.native_symbol: row for row in session.scalars(select(Symbol))}
        assert set(symbols) == {"AAPL", "AAPLX-USD"}
        assert symbols["AAPL"].public_id == stock_id
        assert symbols["AAPL"].quote is None
        assert symbols["AAPLX-USD"].base == "AAPLx"
        assert symbols["AAPLX-USD"].quote == "USD"


async def test_conflicting_alias_owners_refuse_atomic_catalog_update(
    token_catalog: TokenCatalog,
) -> None:
    """Split active WS and REST owners require identity repair before discovery can proceed.

    Given: Different active symbols owning the token's WS and REST aliases,
    When: A new catalog observation tries to resolve the product,
    Then: The transaction refuses the contradiction without reassigning either alias.
    """
    _seed_alias_owner(token_catalog, "AAPL", [("rest", "AAPLxUSD")])
    _seed_alias_owner(token_catalog, "AAPLX-USD", [("ws", "AAPLx/USD")])
    with pytest.raises(ValueError, match="Conflicting tokenized alias owners"):
        await token_catalog.write([token_catalog.record(_pair())])
    with token_catalog.sessions() as session:
        assert len(list(session.scalars(select(Symbol)))) == 2
        assert len(list(session.scalars(select(SymbolAlias)))) == 2
        assert list(session.scalars(select(InstrumentSpec))) == []


async def test_alias_owner_with_contradictory_quote_is_refused(token_catalog: TokenCatalog) -> None:
    """An alias cannot quietly change the quote denomination of a referenced product.

    Given: An existing alias owner whose stored quote contradicts the venue product,
    When: The token updater resolves its identity,
    Then: It refuses the update instead of rewriting a referenced quote denomination.
    """
    _seed_alias_owner(token_catalog, "AAPL", [("ws", "AAPLx/USD")], quote="EUR")
    with pytest.raises(ValueError, match="Conflicting tokenized quote identity"):
        await token_catalog.write([token_catalog.record(_pair())])


async def test_alias_owner_with_contradictory_base_is_refused(token_catalog: TokenCatalog) -> None:
    """An existing alias cannot relabel a different issuer as the discovered token.

    Given: AAPL token aliases owned by a symbol naming a different base asset,
    When: The catalog resolves that pre-existing owner,
    Then: Its conflicting base identity refuses the complete update.
    """
    _seed_alias_owner(token_catalog, "AAPL", [("ws", "AAPLx/USD")], base="MSFT")
    with pytest.raises(ValueError, match="Conflicting tokenized base identity"):
        await token_catalog.write([token_catalog.record(_pair())])


async def test_occupied_canonical_identity_is_not_reassigned(token_catalog: TokenCatalog) -> None:
    """An occupied fallback identity cannot be silently rewritten as a token.

    Given: Both the legacy ticker and canonical token ticker occupied without token aliases,
    When: A new token discovery attempts to allocate a safe identity,
    Then: It refuses the conflict instead of modifying either referenced symbol.
    """
    _seed_alias_owner(token_catalog, "AAPL", [], quote=None)
    _seed_alias_owner(token_catalog, "AAPLX-USD", [], quote=None)
    with pytest.raises(ValueError, match="Conflicting tokenized canonical identity"):
        await token_catalog.write([token_catalog.record(_pair())])


@pytest.mark.parametrize(
    ("source", "observed_at"),
    [
        ("kraken:ccxt.load_markets", _OBSERVED),
        ("kraken:rest.AssetPairs:tokenized_asset", None),
    ],
)
def test_token_precision_requires_attested_current_provenance(
    token_catalog: TokenCatalog, source: str, observed_at: datetime | None
) -> None:
    """Precision values require their actual token catalog observation provenance.

    Given: Complete token amounts with an incorrect source or missing observation time,
    When: The preview-precision predicate inspects their provenance,
    Then: Values without the required catalog attestation cannot advertise complete precision.
    """
    record = token_catalog.record(_pair())
    record["metadata"] = replace(
        record["metadata"], spec_source=source, spec_observed_at=observed_at
    )
    assert _tokenized_precision_complete(record) is False


@pytest.mark.parametrize("reverse", [False, True])
def test_direct_token_catalog_wins_over_duplicate_ccxt_route(
    token_catalog: TokenCatalog, reverse: bool
) -> None:
    """The direct product class and constraints win independently of upstream dictionary order.

    Given: CCXT and direct tokenized catalog entries naming the same WS product,
    When: Mapping construction processes either upstream dictionary order,
    Then: One direct token route retains its authoritative class and precision.
    """
    direct = token_catalog.updater._extract_tokenized_pair("SPVUSD", _pair(base="SPV"), _OBSERVED)
    ccxt = token_catalog.updater._extract_ccxt_market_pair(
        "SPV/USD",
        {
            "id": "SPV/USD",
            "base": "SPV",
            "quote": "USD",
            "active": True,
            "precision": {"price": 0.1, "amount": 0.01, "cost": 0.01},
        },
        _OBSERVED,
    )
    assert direct is not None and ccxt is not None
    entries = [("SPVUSD", direct), ("SPV/USD", ccxt)]
    mappings, wanted = token_catalog.updater._build_mappings_from_rest(
        dict(reversed(entries) if reverse else entries)
    )
    assert wanted == {"SPV/USD"}
    assert set(mappings) == {"SPV"}
    assert mappings["SPV"]["kraken_rest_symbol"] == "SPVUSD"
    assert mappings["SPV"]["metadata"].tick_size == 0.01
    assert mappings["SPV"]["asset_class"] == "tokenized_asset"


@pytest.mark.parametrize("reverse", [False, True])
def test_duplicate_rest_alias_selection_is_deterministic(
    token_catalog: TokenCatalog, reverse: bool
) -> None:
    """Equivalent compact and slash aliases produce one stable product route.

    Given: Equivalent direct REST aliases supplied in either dictionary order,
    When: The mapping builder deduplicates their shared WS product,
    Then: A deterministic compact REST route represents the single token identity.
    """
    direct = token_catalog.updater._extract_tokenized_pair("AAPLxUSD", _pair(), _OBSERVED)
    assert direct is not None
    entries = [("AAPLxUSD", direct), ("AAPLx/USD", direct)]
    mappings, _ = token_catalog.updater._build_mappings_from_rest(
        dict(reversed(entries) if reverse else entries)
    )
    assert set(mappings) == {"AAPL"}
    assert mappings["AAPL"]["kraken_rest_symbol"] == "AAPLxUSD"


@pytest.mark.parametrize(
    ("base", "quote", "rest_alias"),
    [
        ("AAPLx", "ZUSD", "AAPLxUSD"),
        ("AAPLx", "ZEUR", "AAPLx/EUR"),
        ("SPV", "ZUSD", "SPVUSD"),
        ("AAPLSPV", "ZUSD", "AAPLSPVUSD"),
    ],
)
async def test_catalog_identity_reaches_native_order_asset_class(
    token_catalog: TokenCatalog,
    monkeypatch: pytest.MonkeyPatch,
    base: str,
    quote: str,
    rest_alias: str,
) -> None:
    """The real extractor, writer, mapper, and submit adapter retain the tokenized class.

    Given: A tokenized product with a compact, slash, or non-x REST alias,
    When: Its actual catalog identity reaches the native order adapter with a fake SDK,
    Then: AddOrder receives the correct alias, client identity, and tokenized asset class.
    """
    payload = _pair(base=base, quote=quote)
    if base == "AAPLSPV":
        payload["wsname"] = "AAPLx/USD"
    record = token_catalog.record(payload, rest_alias)
    await token_catalog.write([record])
    mapper = object.__new__(SymbolMapperService)
    mapper.repository = token_catalog.updater.repository
    mapper._populate_maps_from_aliases(mapper.load_mappings_from_db())
    mapper.kraken_asset_classes = mapper.load_kraken_asset_classes_from_db()
    monkeypatch.setattr(symbol_functions, "_get_db_mapper", lambda: mapper)
    sdk = MagicMock()
    sdk.create_order.return_value = {"txid": ["token-order-identity"]}

    async def dispatch(
        *,
        operation: str,
        kind: KrakenRestOperationKind,
        target: RestProxyTarget,
        sync_call: Callable[[], JsonObject]
    ) -> JsonObject:
        """Run the injected SDK double without any network or credential access."""
        assert operation == "native_create_order"
        assert kind == "private_mutation"
        return sync_call()

    client = KrakenExchangeClient(api_key="test-key", api_secret="test-secret")
    monkeypatch.setattr(client, "_get_trade_client", lambda: sdk)
    monkeypatch.setattr(client, "_dispatch_routed_rest", dispatch)
    monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
    order = await client.create_order(
        ExchangeOrderRequest(
            symbol=record["native_symbol"],
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=150.0,
            client_order_id="token-client-order-identity",
        )
    )
    assert order.id == "token-order-identity"
    sdk.create_order.assert_called_once()
    sent = sdk.create_order.call_args.kwargs
    assert sent["pair"] == rest_alias
    assert sent["extra_params"] == {
        "cl_ord_id": "token-client-order-identity",
        "asset_class": "tokenized_asset",
    }


async def test_unclassified_native_order_never_reaches_the_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent catalog class cannot default a token-shaped alias to currency.

    Given: A mapped native REST alias with no corresponding asset-class attestation,
    When: The actual submit adapter resolves that native-only product,
    Then: It refuses before the routed SDK mutation or database order write.
    """
    mapper = object.__new__(SymbolMapperService)
    mapper._populate_maps_from_aliases([("UNCLASSIFIED", "kraken", "rest", "AAPLxUSD")])
    mapper.kraken_asset_classes = {}
    monkeypatch.setattr(symbol_functions, "_get_db_mapper", lambda: mapper)
    client = KrakenExchangeClient(api_key="test-key", api_secret="test-secret")
    sdk = MagicMock()
    dispatch = AsyncMock()
    log_order = AsyncMock()
    monkeypatch.setattr(client, "_get_trade_client", lambda: sdk)
    monkeypatch.setattr(client, "_dispatch_routed_rest", dispatch)
    monkeypatch.setattr(client, "_log_order_to_db", log_order)
    with pytest.raises(ValueError, match="Unknown Kraken order asset class"):
        await client.create_order(
            ExchangeOrderRequest(
                symbol="UNCLASSIFIED",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=1.0,
                price=150.0,
                client_order_id="unclassified-token-client-id",
            )
        )
    sdk.create_order.assert_not_called()
    dispatch.assert_not_awaited()
    log_order.assert_not_awaited()


async def test_missing_new_metadata_revokes_trading_while_preserving_prior_spec(
    token_catalog: TokenCatalog,
) -> None:
    """Preserving an earlier precision observation cannot preserve stale order permission.

    Given: A token precision observation followed by a legacy mapping without new metadata,
    When: The writer preserves the earlier spec's provenance,
    Then: Its symbol trading permission still becomes false for the incomplete observation.
    """
    record = token_catalog.record(_pair())
    await token_catalog.write([record])
    record.pop("metadata")
    await token_catalog.write([record], minutes=1)
    with token_catalog.sessions() as session:
        current = session.scalars(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).one()
        assert current.can_trade is False
        spec = session.scalars(select(InstrumentSpec)).one()
        assert spec.status == "online"
        assert spec.spec_observed_at == _OBSERVED
