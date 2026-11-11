"""Kraken catalog identity (spec 2026-11-11, items C1 to C4) on real SQLite catalogs.

The catalog is seeded the way production stood on 2026-11-11: the Frankencoin
pairs catalogued as the forex symbols ``CHF-USD`` and ``CHF-CHF`` holding the
ZCHF REST and CCXT names, polygon's real franc on ``CHF-USD``, and the
WebSocket-only ``ZCHF-*`` rows. Every run goes through the actual
``_update_database`` with a patched clock.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from snapper.application.updaters.symbols import kraken as kraken_module
from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.kraken import _KrakenPairInfo
from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.core.types import AliasChannelEnum
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository

_SEEDED = datetime(2026, 10, 11, 12, tzinfo=UTC)
_FORWARD = datetime(2026, 11, 12, 9, tzinfo=UTC)
_REPEAT = _FORWARD + timedelta(hours=6)
_INVERSE = _FORWARD + timedelta(hours=12)
_FORWARD_AGAIN = _FORWARD + timedelta(hours=18)

type _AliasKey = tuple[str, str, str, str]
"""(native symbol, exchange, channel, alias string) of one active alias."""


@dataclass
class Catalog:
    """Bind the actual Kraken updater to one isolated synchronous SQLite catalog."""

    updater: KrakenSymbolUpdaterService
    sessions: sessionmaker[Session]

    async def run(self, records: list[KrakenSymbolRecord], at: datetime) -> None:
        """Run one updater transaction with a fixed clock.

        Args:
            records: Records to persist.
            at: Transaction time.
        """
        with patch("snapper.application.updaters.symbols.kraken.datetime") as clock:
            clock.now.return_value = at
            await self.updater._update_database([dict(record) for record in records])

    def active_aliases(self, at: datetime) -> set[_AliasKey]:
        """Read every alias active at ``at``, with its owner's native symbol.

        Args:
            at: Point in time.

        Returns:
            The active aliases.
        """
        with self.sessions() as session:
            rows = session.execute(
                select(
                    Symbol.native_symbol,
                    SymbolAlias.exchange,
                    SymbolAlias.channel,
                    SymbolAlias.exchange_symbol,
                )
                .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
                .where(
                    SymbolAlias.timestamp <= at,
                    SymbolAlias.known_to > at,
                    Symbol.timestamp <= at,
                    Symbol.known_to > at,
                )
            ).all()
        return {(row[0], row[1], row[2], row[3]) for row in rows}

    def alias_rows(self, exchange_symbol: str, channel: str) -> list[SymbolAlias]:
        """Read every version of one Kraken alias string, oldest first.

        Args:
            exchange_symbol: The alias string.
            channel: Its channel.

        Returns:
            All its rows.
        """
        with self.sessions() as session:
            return list(
                session.scalars(
                    select(SymbolAlias)
                    .where(
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == channel,
                        SymbolAlias.exchange_symbol == exchange_symbol,
                    )
                    .order_by(SymbolAlias.id)
                )
            )

    def public_id(self, native: str) -> str:
        """Return the public id of a native symbol's active version.

        Args:
            native: Native symbol.

        Returns:
            Its public id.
        """
        with self.sessions() as session:
            return str(
                session.scalars(
                    select(Symbol.public_id).where(Symbol.native_symbol == native)
                ).first()
            )

    def kraken_capability(self, native: str, at: datetime) -> tuple[bool, bool, str | None]:
        """Read a symbol's Kraken capability active at ``at``.

        Args:
            native: Native symbol.
            at: Point in time.

        Returns:
            Market data, trading, and reason.
        """
        with self.sessions() as session:
            row = session.execute(
                select(
                    SymbolExchangeCapability.can_market_data,
                    SymbolExchangeCapability.can_trade,
                    SymbolExchangeCapability.reason,
                )
                .join(Symbol, Symbol.public_id == SymbolExchangeCapability.symbol_public_id)
                .where(
                    Symbol.native_symbol == native,
                    SymbolExchangeCapability.exchange == "kraken",
                    SymbolExchangeCapability.timestamp <= at,
                    SymbolExchangeCapability.known_to > at,
                )
            ).one()
        return row[0], row[1], row[2]

    def instruments(self, native: str) -> set[str]:
        """Return the public ids of a native symbol's instruments.

        Args:
            native: Native symbol.

        Returns:
            Instrument public ids.
        """
        with self.sessions() as session:
            return set(
                session.scalars(
                    select(Instrument.public_id)
                    .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
                    .where(Symbol.native_symbol == native)
                )
            )

    def add_polygon_franc(self) -> None:
        """Give CHF-USD polygon's real franc alias and market-data capability."""
        with self.sessions() as session:
            owner = self.public_id("CHF-USD")
            self.updater._upsert_alias(
                session, owner, "polygon", AliasChannelEnum.REST, "C:CHFUSD", _SEEDED, "seed", 1
            )
            self.updater._upsert_capability(
                session,
                owner,
                "polygon",
                True,
                False,
                "polygon_updater",
                None,
                _SEEDED,
                session_id="seed",
                sequence_id=2,
            )
            session.commit()


@pytest.fixture
def catalog() -> Iterator[Catalog]:
    """Provide a real isolated catalog without connecting to any venue."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    repository = MagicMock(spec=DatabaseRepository)
    repository.get_session.side_effect = sessions
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
    updater.repository = repository
    try:
        yield Catalog(updater, sessions)
    finally:
        engine.dispose()


def _rest(native: str, legs: tuple[str, str], aliases: tuple[str, str, str]) -> KrakenSymbolRecord:
    """Build a REST record as the updater persists it.

    Args:
        native: Native symbol.
        legs: Base and quote currency.
        aliases: WebSocket, REST and CCXT alias strings.

    Returns:
        The record.
    """
    return KrakenSymbolRecord(
        native_symbol=native,
        base_currency=legs[0],
        quote_currency=legs[1],
        kraken_websocket_symbol=aliases[0],
        kraken_rest_symbol=aliases[1],
        ccxt_symbol=aliases[2],
        asset_class="currency",
    )


def _ws_only(native: str, legs: tuple[str, str], wire: str) -> KrakenSymbolRecord:
    """Build a WebSocket-only record as discovery produces it.

    Args:
        native: Native symbol.
        legs: Base and quote currency.
        wire: WebSocket alias string.

    Returns:
        The record.
    """
    return KrakenSymbolRecord(
        native_symbol=native,
        base_currency=legs[0],
        quote_currency=legs[1],
        kraken_websocket_symbol=wire,
        kraken_rest_symbol="",
        ccxt_symbol="",
        asset_class="crypto",
        ws_only="true",
    )


_PRE_C1 = [
    _rest("CHF-USD", ("CHF", "USD"), ("CHF/USD", "ZCHFUSD", "ZCHF/USD")),
    _rest("CHF-CHF", ("CHF", "CHF"), ("CHF/CHF", "ZCHFCHF", "ZCHF/CHF")),
    _ws_only("ZCHF-USD", ("ZCHF", "USD"), "ZCHF/USD"),
    _ws_only("ZCHF-CHF", ("ZCHF", "CHF"), "ZCHF/CHF"),
]
"""The Kraken records of the old normalization, as production was catalogued."""


def _c1_records(updater: KrakenSymbolUpdaterService) -> list[KrakenSymbolRecord]:
    """Resolve the two ZCHF pairs through the actual REST mapping builder.

    Args:
        updater: The updater under test.

    Returns:
        The records C1 produces.
    """
    mappings, _ = updater._build_mappings_from_rest(
        {
            "ZCHFUSD": _KrakenPairInfo(base="ZCHF", quote="ZUSD", ccxt_symbol="ZCHF/USD"),
            "ZCHFCHF": _KrakenPairInfo(base="ZCHF", quote="CHF", ccxt_symbol="ZCHF/CHF"),
        }
    )
    return list(mappings.values())


async def _seed_production(catalog: Catalog) -> None:
    """Seed the pre-C1 production state, polygon's franc included.

    Args:
        catalog: The catalog to seed.
    """
    await catalog.run(_PRE_C1, _SEEDED)
    catalog.add_polygon_franc()


async def _forward(catalog: Catalog, at: datetime) -> int:
    """Run the C1 records and count the transfer warnings.

    Args:
        catalog: The catalog to update.
        at: Transaction time.

    Returns:
        The number of alias transfers logged.
    """
    with patch("snapper.application.updaters.symbols.kraken_alias_transfer.logger") as transfer_log:
        await catalog.run(_c1_records(catalog.updater), at)
    return int(transfer_log.warning.call_count)


_ZCHF_ALIASES = {
    ("ZCHF-USD", "kraken", "ws", "ZCHF/USD"),
    ("ZCHF-USD", "kraken", "rest", "ZCHFUSD"),
    ("ZCHF-USD", "kraken", "ccxt", "ZCHF/USD"),
    ("ZCHF-CHF", "kraken", "ws", "ZCHF/CHF"),
    ("ZCHF-CHF", "kraken", "rest", "ZCHFCHF"),
    ("ZCHF-CHF", "kraken", "ccxt", "ZCHF/CHF"),
}
"""The Kraken aliases the ZCHF symbols own after C1."""


def test_c1_resolves_the_venue_identities() -> None:
    """ZCHF keeps its own code, and the fiat franc pair keeps its fiat legs.

    Given: The ZCHFUSD, ZCHFCHF and USDCHF REST pairs as Kraken lists them,
    When: The actual REST mapping builder resolves them,
    Then: They become ZCHF-USD, ZCHF-CHF and USD-CHF, with the venue's wsname as
        their WebSocket alias.
    """
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
    mappings, _ = updater._build_mappings_from_rest(
        {
            "ZCHFUSD": _KrakenPairInfo(base="ZCHF", quote="ZUSD", ccxt_symbol="ZCHF/USD"),
            "ZCHFCHF": _KrakenPairInfo(base="ZCHF", quote="CHF", ccxt_symbol="ZCHF/CHF"),
            "USDCHF": _KrakenPairInfo(base="ZUSD", quote="CHF", ccxt_symbol="USD/CHF"),
        }
    )
    assert {native: record["kraken_websocket_symbol"] for native, record in mappings.items()} == {
        "ZCHF-USD": "ZCHF/USD",
        "ZCHF-CHF": "ZCHF/CHF",
        "USD-CHF": "USD/CHF",
    }


async def test_first_run_transfers_four_aliases_and_delists_the_chf_symbols(
    catalog: Catalog,
) -> None:
    """C1 with C2 completes on the production catalog and fixes its ownership.

    Given: The catalog as production stood, polygon's franc on CHF-USD included,
    When: The first updater run with C1 persists the two ZCHF pairs,
    Then: It completes; the ZCHF symbols own all six Kraken aliases; the Kraken
        capabilities of CHF-USD and CHF-CHF are delisted and their Kraken aliases
        closed; polygon's franc stays; each closed source row keeps its owner
        and identity, each destination row has its own; four transfers are
        logged; and the ZCHF symbol and instrument identities are unchanged.
    """
    await _seed_production(catalog)
    zchf_ids = {native: catalog.public_id(native) for native in ("ZCHF-USD", "ZCHF-CHF")}
    zchf_instruments = {native: catalog.instruments(native) for native in zchf_ids}
    chf_usd = catalog.public_id("CHF-USD")

    transfers = await _forward(catalog, _FORWARD)

    assert transfers == 4
    active = catalog.active_aliases(_FORWARD)
    assert {alias for alias in active if alias[0].startswith("ZCHF")} == _ZCHF_ALIASES
    assert {alias for alias in active if alias[0] in ("CHF-USD", "CHF-CHF")} == {
        ("CHF-USD", "polygon", "rest", "C:CHFUSD")
    }
    for native in ("CHF-USD", "CHF-CHF"):
        assert catalog.kraken_capability(native, _FORWARD) == (
            False,
            False,
            "Delisted: not seen in updater run",
        )
    for native in ("ZCHF-USD", "ZCHF-CHF"):
        assert catalog.kraken_capability(native, _FORWARD) == (True, True, None)
    closed, current = catalog.alias_rows("ZCHFUSD", "rest")
    assert (closed.symbol_public_id, closed.known_to) == (chf_usd, _FORWARD)
    assert current.symbol_public_id == zchf_ids["ZCHF-USD"]
    assert current.public_id != closed.public_id
    assert {native: catalog.public_id(native) for native in zchf_ids} == zchf_ids
    assert {native: catalog.instruments(native) for native in zchf_ids} == zchf_instruments


async def test_repeat_run_transfers_nothing(catalog: Catalog) -> None:
    """A second run with C1 is idempotent.

    Given: The catalog after the first C1 run,
    When: The same records run again,
    Then: Nothing is transferred and the active aliases and their row counts
        are unchanged.
    """
    await _seed_production(catalog)
    await _forward(catalog, _FORWARD)
    with catalog.sessions() as session:
        rows_before = session.scalar(select(func.count()).select_from(SymbolAlias))

    assert await _forward(catalog, _REPEAT) == 0

    assert catalog.active_aliases(_REPEAT) == catalog.active_aliases(_FORWARD)
    with catalog.sessions() as session:
        assert session.scalar(select(func.count()).select_from(SymbolAlias)) == rows_before


async def test_inverse_transfers_restore_the_old_catalog_and_forward_runs_again(
    catalog: Catalog,
) -> None:
    """The whitelisted inverse transfers carry a C1 rollback, and forward runs again.

    Given: The catalog after the first C1 run,
    When: A run with the old normalization follows, then C1 again, then C1 once more,
    Then: The four aliases move back to the CHF symbols, then forward again, and
        the last run transfers nothing.
    """
    await _seed_production(catalog)
    await _forward(catalog, _FORWARD)
    with patch("snapper.application.updaters.symbols.kraken_alias_transfer.logger") as transfer_log:
        await catalog.run(_PRE_C1, _INVERSE)
    assert transfer_log.warning.call_count == 4
    restored = catalog.active_aliases(_INVERSE)
    assert {
        ("CHF-USD", "kraken", "rest", "ZCHFUSD"),
        ("CHF-USD", "kraken", "ccxt", "ZCHF/USD"),
        ("CHF-CHF", "kraken", "rest", "ZCHFCHF"),
        ("CHF-CHF", "kraken", "ccxt", "ZCHF/CHF"),
    } <= restored
    assert await _forward(catalog, _FORWARD_AGAIN) == 4
    assert await _forward(catalog, _FORWARD_AGAIN + timedelta(hours=1)) == 0


async def test_destination_keeps_its_own_alias_identity(catalog: Catalog) -> None:
    """A destination that already owns another alias on the channel keeps its identity.

    Given: The production catalog, with ZCHF-USD already owning another Kraken REST alias,
    When: The first C1 run transfers ZCHFUSD,
    Then: CHF-USD's row is closed under its own identity, and ZCHF-USD's new row
        continues the identity of its earlier REST alias, never CHF-USD's.
    """
    await _seed_production(catalog)
    zchf_usd = catalog.public_id("ZCHF-USD")
    with catalog.sessions() as session:
        catalog.updater._upsert_alias(
            session, zchf_usd, "kraken", AliasChannelEnum.REST, "ZCHFUSD-PREV", _SEEDED, "seed", 3
        )
        session.commit()
    (previous,) = catalog.alias_rows("ZCHFUSD-PREV", "rest")

    await _forward(catalog, _FORWARD)

    closed, current = catalog.alias_rows("ZCHFUSD", "rest")
    assert closed.symbol_public_id == catalog.public_id("CHF-USD")
    assert (current.symbol_public_id, current.public_id) == (zchf_usd, previous.public_id)
    assert current.created_at == previous.created_at


async def test_unlisted_transfer_aborts_the_whole_run(catalog: Catalog) -> None:
    """A collision outside the whitelist fails closed and leaves the catalog unchanged.

    Given: FOO-USD owning the Kraken REST alias FOOUSD,
    When: A run would write FOOUSD for BAR-USD,
    Then: The run raises, nothing of it is committed, and the claims are empty again.
    """
    await catalog.run([_rest("FOO-USD", ("FOO", "USD"), ("FOO/USD", "FOOUSD", "FOO/USD"))], _SEEDED)
    before = catalog.active_aliases(_SEEDED)

    with pytest.raises(ValueError, match="not a whitelisted transfer"):
        await catalog.run(
            [_rest("BAR-USD", ("BAR", "USD"), ("BAR/USD", "FOOUSD", "BAR/USD"))], _FORWARD
        )

    assert catalog.active_aliases(_FORWARD) == before
    assert catalog.updater._alias_claims.owners == {}


@pytest.mark.parametrize(
    ("order", "message"),
    [(("FIRST", "SECOND"), "claimed twice"), (("SECOND", "FIRST"), "not a whitelisted")],
)
async def test_competing_claims_in_one_run_fail_in_either_order(
    catalog: Catalog, order: tuple[str, str], message: str
) -> None:
    """Two symbols cannot both claim one alias string in a run.

    Given: FIRST already owning the WebSocket alias DUP/USD, so its write is unchanged,
    When: A run writes DUP/USD for FIRST and SECOND, in either order,
    Then: The run raises before committing anything.
    """
    records = {
        "FIRST": _rest("FIRST-USD", ("FIRST", "USD"), ("DUP/USD", "FIRSTUSD", "FIRST/USD")),
        "SECOND": _rest("SECOND-USD", ("SECOND", "USD"), ("DUP/USD", "SECONDUSD", "SECOND/USD")),
    }
    await catalog.run([records["FIRST"]], _SEEDED)

    with pytest.raises(ValueError, match=message):
        await catalog.run([records[name] for name in order], _FORWARD)

    assert ("SECOND-USD", "kraken", "rest", "SECONDUSD") not in catalog.active_aliases(_FORWARD)


@pytest.mark.parametrize(
    "records",
    [
        [
            _ws_only("WSO-USD", ("WSO", "USD"), "CROSS/USD"),
            _rest("RST-USD", ("RST", "USD"), ("CROSS/USD", "RSTUSD", "RST/USD")),
        ],
        [
            KrakenSymbolRecord(
                native_symbol="ODD-USD-BTNL",
                base_currency="ODD",
                quote_currency="USD",
                kraken_websocket_symbol="ODD/USD:BTNL",
                is_btnl="true",
            ),
            _rest("ODD-USD", ("ODD", "USD"), ("ODD/USD", "ODD/USD:BTNL", "ODD/USD")),
        ],
    ],
)
async def test_claims_span_persist_paths(
    catalog: Catalog, records: list[KrakenSymbolRecord]
) -> None:
    """Claims cover every persist path and channel.

    Given: A WebSocket-only and a REST record sharing a WebSocket alias, or a
        Bitnomial and a REST record sharing a REST alias,
    When: Both run in one transaction,
    Then: The second claim raises, and a later valid run starts with no claims.
    """
    with pytest.raises(ValueError, match="claimed twice"):
        await catalog.run(records, _FORWARD)

    await catalog.run(records[:1], _REPEAT)
    assert catalog.updater._alias_claims.owners == {}


async def test_tokenized_ccxt_collision_stays_fail_closed(catalog: Catalog) -> None:
    """A tokenized record whose CCXT alias another symbol holds aborts the run.

    Given: TOKP-USD owning the Kraken CCXT alias TOK/USD,
    When: A tokenized record carrying that CCXT alias runs,
    Then: The run raises instead of moving the alias.
    """
    await catalog.run(
        [_rest("TOKP-USD", ("TOKP", "USD"), ("TOKP/USD", "TOKPUSD", "TOK/USD"))], _SEEDED
    )
    mappings, _ = catalog.updater._build_mappings_from_rest(
        {
            "TOKxUSD": _KrakenPairInfo(
                base="TOKx", quote="ZUSD", asset_class="tokenized_asset", wsname="TOKx/USD"
            )
        }
    )
    (record,) = mappings.values()
    record["ccxt_symbol"] = "TOK/USD"

    with pytest.raises(ValueError, match="not a whitelisted transfer"):
        await catalog.run([record], _FORWARD)


def _token_mappings(updater: KrakenSymbolUpdaterService) -> dict[str, KrakenSymbolRecord]:
    """Resolve the tokenized AAPLx/USD REST product through the actual builder.

    Args:
        updater: The updater under test.

    Returns:
        Mappings keyed by native symbol.
    """
    mappings, _ = updater._build_mappings_from_rest(
        {
            "AAPLxUSD": _KrakenPairInfo(
                base="AAPLx", quote="ZUSD", asset_class="tokenized_asset", wsname="AAPLx/USD"
            ),
            "XXBTZUSD": _KrakenPairInfo(base="XXBT", quote="ZUSD", ccxt_symbol="BTC/USD"),
        }
    )
    return mappings


@pytest.mark.parametrize(
    ("instrument", "verified", "kept"),
    [
        ({"symbol": "AAPLXYZ/USD", "base": "AAPLx", "quote": "USD"}, {"AAPLx/USD"}, True),
        ({"symbol": "AAPLSPV/USD", "base": "AAPLX", "quote": "USD"}, {"AAPLx/USD"}, True),
        ({"symbol": "AAPLSPV/USD", "base": "AAPLx", "quote": "USD"}, set(), True),
        ({"symbol": "BTCSPV/USD", "base": "BTCx", "quote": "USD"}, {"BTC/USD"}, True),
        ({"symbol": "AAPLx/USD:BTNL", "base": "AAPLx", "quote": "USD"}, {"AAPLx/USD"}, True),
        ({"symbol": "AAPLSPV/EUR", "base": "AAPLx", "quote": "EUR"}, {"AAPLx/USD"}, True),
        ({"symbol": "AAPLSPV/USD", "base": "AAPLx", "quote": "USD"}, {"AAPLx/USD"}, False),
    ],
)
def test_spv_skip_is_bounded_to_the_observed_shape(
    instrument: dict[str, str], verified: set[str], kept: bool
) -> None:
    """Only the exact SPV wire name of a verified token is skipped.

    Given: A tokenized AAPLx/USD product and a currency BTC/USD pair from REST,
        and one WebSocket-only candidate,
    When: The candidates are merged,
    Then: A same-leg non-SPV name, a case mismatch, an unverified twin, a standard
        alias, a Bitnomial-shaped name and a non-USD quote are kept; only
        AAPLSPV/USD next to a verified AAPLx/USD is skipped.
    """
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
    mappings = _token_mappings(updater)

    added, skipped = updater._merge_ws_only_instruments(mappings, [instrument], verified)

    assert (added, skipped) == ((1, 0) if kept else (0, 1))


def test_repeated_spv_events_count_once() -> None:
    """Repeated instrument events of one SPV wire name count as one skip.

    Given: The SPV wire name of a verified token delivered three times,
    When: The candidates are merged,
    Then: Nothing is added and one distinct wire name is counted.
    """
    with patch("snapper.application.updaters.symbols.base.get_settings"):
        updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
    spv = {"symbol": "AAPLSPV/USD", "base": "AAPLx", "quote": "USD"}

    assert updater._merge_ws_only_instruments(
        _token_mappings(updater), [spv, spv, spv], {"AAPLx/USD"}
    ) == (0, 1)


async def test_seeded_spv_symbol_is_delisted_by_the_next_run(catalog: Catalog) -> None:
    """An SPV duplicate stored earlier is delisted once discovery skips it.

    Given: A catalog holding the token AAPL and its SPV duplicate AAPLX-USD,
    When: The next run processes AAPL only,
    Then: AAPLX-USD's Kraken capability is delisted and its alias closed.
    """
    token = _token_mappings(catalog.updater)["AAPL"]
    await catalog.run([token, _ws_only("AAPLX-USD", ("AAPLx", "USD"), "AAPLSPV/USD")], _SEEDED)

    await catalog.run([token], _FORWARD)

    assert catalog.kraken_capability("AAPLX-USD", _FORWARD)[:2] == (False, False)
    assert not {alias for alias in catalog.active_aliases(_FORWARD) if alias[0] == "AAPLX-USD"}


@pytest.mark.parametrize(("kept", "stale", "delisted"), [(1, 2, False), (1, 1, True), (2, 1, True)])
async def test_reconciliation_guard_boundary(
    catalog: Catalog, kept: int, stale: int, delisted: bool
) -> None:
    """The 0.5 guard skips below the boundary and passes at and above it.

    Given: ``kept`` symbols that the next run processes and ``stale`` ones it drops,
    When: The next run completes,
    Then: The stale symbols are delisted exactly when kept / (kept + stale) is at
        least one half.
    """
    keep = [
        _rest(f"K{i}-USD", (f"K{i}", "USD"), (f"K{i}/USD", f"K{i}USD", f"K{i}/USD"))
        for i in range(kept)
    ]
    drop = [_ws_only(f"S{i}-USD", (f"S{i}", "USD"), f"S{i}/USD") for i in range(stale)]
    await catalog.run(keep + drop, _SEEDED)

    await catalog.run(keep, _FORWARD)

    assert catalog.kraken_capability("S0-USD", _FORWARD)[0] is not delisted


async def test_guard_passes_with_179_stale_owners(catalog: Catalog) -> None:
    """The release's expected stale set passes the guard on its own.

    Given: 179 kept symbols and 179 stale ones, the size of the expected stale set,
    When: The next run processes the kept ones only,
    Then: The ratio is exactly one half and all 179 are delisted.
    """
    keep = [
        _rest(f"K{i}-USD", (f"K{i}", "USD"), (f"K{i}/USD", f"K{i}USD", f"K{i}/USD"))
        for i in range(179)
    ]
    drop = [_ws_only(f"S{i}-USD", (f"S{i}", "USD"), f"S{i}/USD") for i in range(179)]
    await catalog.run(keep + drop, _SEEDED)

    await catalog.run(keep, _FORWARD)

    assert all(not catalog.kraken_capability(f"S{i}-USD", _FORWARD)[0] for i in range(179))


_RENDER = KrakenSymbolRecord(
    native_symbol="RENDER-USD-BTNL",
    base_currency="RENDER",
    quote_currency="USD",
    kraken_websocket_symbol="RENDER/USD:BTNL",
    is_btnl="true",
)
"""A Bitnomial relay record as discovery produces it."""


async def test_bitnomial_rest_alias_survives_a_path_that_no_longer_writes_it(
    catalog: Catalog,
) -> None:
    """Dropping the REST write alone does not withdraw the alias.

    Given: A Bitnomial product persisted with its WebSocket and REST aliases,
    When: A later run reaches it through a path that writes the WebSocket alias only,
    Then: Its REST alias stays active, because its market data stays enabled.
    """
    await catalog.run([_RENDER], _SEEDED)

    await catalog.run([_ws_only("RENDER-USD-BTNL", ("RENDER", "USD"), "RENDER/USD:BTNL")], _FORWARD)

    assert ("RENDER-USD-BTNL", "kraken", "rest", "RENDER/USD:BTNL") in catalog.active_aliases(
        _FORWARD
    )


async def test_scoped_rollback_closes_only_the_bitnomial_rest_alias(
    catalog: Catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reviewed C4 rollback closes the REST alias and nothing else.

    Given: A Bitnomial product with both aliases and an ordinary REST product,
    When: A run follows with the Bitnomial REST identity switched off,
    Then: Only the Bitnomial REST alias is closed; its WebSocket alias, its
        market-data capability and the other product's aliases stay.
    """
    other = _rest("BTC-USD", ("BTC", "USD"), ("BTC/USD", "XXBTZUSD", "BTC/USD"))
    await catalog.run([_RENDER, other], _SEEDED)
    monkeypatch.setattr(kraken_module, "_BTNL_REST_IDENTITY_ENABLED", False)

    await catalog.run([_RENDER, other], _FORWARD)

    active = catalog.active_aliases(_FORWARD)
    assert ("RENDER-USD-BTNL", "kraken", "rest", "RENDER/USD:BTNL") not in active
    assert ("RENDER-USD-BTNL", "kraken", "ws", "RENDER/USD:BTNL") in active
    assert {alias for alias in active if alias[0] == "BTC-USD"} == {
        ("BTC-USD", "kraken", "ws", "BTC/USD"),
        ("BTC-USD", "kraken", "rest", "XXBTZUSD"),
        ("BTC-USD", "kraken", "ccxt", "BTC/USD"),
    }
    assert catalog.kraken_capability("RENDER-USD-BTNL", _FORWARD)[:2] == (True, False)
