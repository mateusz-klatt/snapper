"""Tests for the Phase-5B crypto→USD plane repository read.

Pins the read that prices a held crypto currency off our own finalized candle
plane: the spot / venue-certified / real-venue / exact-USD-quote eligibility
proof, the as-of version threading (later corrections never back-applied),
deterministic ordering, and the candle VERSION identity fields carried for audit
provenance. Each eligibility rule is excluded one at a time so a single relaxed
filter cannot silently admit an ineligible price plane, and the ADMISSIONS are
pinned just as hard: a margin-capable spot listing, and a certified spec whose
lifecycle ``status`` is inactive or null, must both stay eligible, so
reintroducing a funding-model or lifecycle predicate fails loudly here rather
than silently withholding every real crypto plane.

The same-venue ambiguity rule is asserted at BOTH layers, because the repository
deliberately returns colliding rows undeduplicated and only the valuator refuses:
an assertion on one layer alone cannot tell "no ambiguity" from "ambiguity that
was silently resolved".
"""

from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.application.portfolio.basket_valuation import CryptoUsdCandle
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.basket_valuation import value_currency
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlCryptoUsdPlaneRow

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_TS = _NOW - timedelta(hours=6)
_M = _NOW - timedelta(hours=1)
_FUTURE = _NOW + timedelta(days=1)
_CORRECTION_AT = _M + timedelta(minutes=30)
_SESSION = "00000000-0000-7000-8000-000000000901"


@dataclass(frozen=True)
class _Ver:
    """SCD2 version window (timestamp, known_to) for one seeded row."""

    timestamp: datetime = _TS
    known_to: datetime = KNOWN_TO_MAX


@dataclass(frozen=True)
class _CandleSpec:
    """Optional candle attributes beyond its instrument, minute and close."""

    public_id: str | None = None
    timeframe: str = "1m"
    complete: bool = True
    timestamp: datetime | None = None
    known_to: datetime = KNOWN_TO_MAX


def _symbol(
    public_id: str, native_symbol: str, base: str, quote: str, ver: _Ver = _Ver()
) -> Symbol:
    """Build one crypto symbol carrying the base/quote legs the read matches on."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base=base,
        quote=quote,
        asset_type="crypto",
        created_at=ver.timestamp,
        timestamp=ver.timestamp,
        known_to=ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    source_exchange: str | None = None,
    ver: _Ver = _Ver(),
) -> Instrument:
    """Build one instrument listing a symbol on a venue."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange=source_exchange,
        requires_ai_review=False,
        timestamp=ver.timestamp,
        known_to=ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


@dataclass(frozen=True)
class _Prov:
    """Author-time certification and lifecycle attributes of one seeded spec.

    ``certified`` drives all three provenance columns together, which is what
    ``ck_instrument_specs_provenance`` demands: a spec block either came from
    venue metadata in full or not at all. ``status`` is lifecycle only and is
    deliberately NOT part of the plane's eligibility proof.
    """

    certified: bool = True
    status: str | None = None


def _spec(
    instrument_public_id: str,
    instrument_kind: str | None = "spot",
    funding_type: str | None = None,
    ver: _Ver = _Ver(),
    prov: _Prov = _Prov(),
) -> InstrumentSpec:
    """Build one instrument spec proving the spot classification and certification.

    Provenance defaults to CERTIFIED because the plane requires a non-null
    ``spec_source``; an uncertified default would silently blind every eligibility
    assertion in this module while leaving the exclusion assertions passing for
    entirely the wrong reason.
    """
    return InstrumentSpec(
        instrument_public_id=instrument_public_id,
        instrument_kind=instrument_kind,
        funding_type=funding_type,
        status=prov.status,
        spec_source="kraken:ccxt.load_markets" if prov.certified else None,
        spec_version="ccxt-1" if prov.certified else None,
        spec_observed_at=ver.timestamp if prov.certified else None,
        timestamp=ver.timestamp,
        known_to=ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _candle(
    instrument_public_id: str,
    open_at: datetime,
    close: float,
    spec: _CandleSpec = _CandleSpec(),
) -> Candle:
    """Build one candle for the crypto→USD series."""
    candle = Candle(
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timeframe=spec.timeframe,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1.0,
        source="native",
        complete=spec.complete,
        timestamp=spec.timestamp or open_at,
        known_to=spec.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )
    if spec.public_id is not None:
        candle.public_id = spec.public_id
    return candle


def _evidence(rows: Sequence[PnlCryptoUsdPlaneRow]) -> ValuationEvidence:
    """Index plane rows into valuation evidence exactly as the snapshotter does.

    Mirrors ``PortfolioPnlSnapshotter._load_crypto_planes``: the candle opening at
    ``open_at`` prices the grid minute one minute later. The fiat maps are empty
    because these assertions only exercise the crypto plane.
    """
    planes: dict[tuple[str, datetime], list[CryptoUsdCandle]] = {}
    for row in rows:
        candle = CryptoUsdCandle(
            base=row["base"],
            quote=row["quote"],
            exchange=row["exchange"],
            native_symbol=row["native_symbol"],
            instrument_public_id=row["instrument_public_id"],
            candle_id=row["candle_id"],
            candle_public_id=row["candle_public_id"],
            candle_open_at=row["open_at"],
            candle_timestamp=row["candle_timestamp"],
            close=row["close"],
        )
        planes.setdefault((row["base"], row["open_at"] + timedelta(minutes=1)), []).append(candle)
    return ValuationEvidence(fiat_rates={}, fiat_venues={}, fiat_versions={}, crypto_planes=planes)


async def _seed_certified_twins(repository: SQLAlchemyRepository) -> None:
    """Seed two CERTIFIED instruments pricing one ``(DUP, USD, kraken)`` plane.

    Certification cannot separate these two, so the collision survives into the
    valuator. Shared by the DAL and valuator assertions of the same behaviour.
    """
    async with repository.session() as session:
        session.add_all(
            [
                _symbol("sym-dup-a", "DUP-USD", "DUP", "USD"),
                _symbol("sym-dup-b", "DUPX-USD", "DUP", "USD"),
                _instrument("ins-dup-a", "sym-dup-a", "kraken"),
                _instrument("ins-dup-b", "sym-dup-b", "kraken"),
                _spec("ins-dup-a"),
                _spec("ins-dup-b"),
                _candle("ins-dup-a", _M, 111.0),
                _candle("ins-dup-b", _M, 222.0),
            ]
        )
        await session.commit()


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with eligible and ineligible planes."""
    db_path = tmp_path / "pnl-crypto.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    InstrumentSpec.__table__.create(schema_engine)
    Candle.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                _symbol("sym-btcusd", "BTC-USD", "BTC", "USD"),
                _symbol("sym-btcusd-btnl", "BTC-USD-BTNL", "BTC", "USD"),
                _symbol("sym-ethusd", "ETH-USD", "ETH", "USD"),
                _symbol("sym-solusd", "SOL-USD", "SOL", "USD"),
                _symbol("sym-btceur", "BTC-EUR", "BTC", "EUR"),
                _symbol("sym-ltcusd", "LTC-USD", "LTC", "USD"),
                _symbol("sym-xrpusd", "XRP-USD", "XRP", "USD", _Ver(known_to=_M)),
                _instrument("ins-btc-kraken", "sym-btcusd", "kraken"),
                _instrument("ins-btc-binance", "sym-btcusd", "binance"),
                _instrument("ins-btc-btnl", "sym-btcusd-btnl", "kraken"),
                _instrument("ins-btc-perp", "sym-btcusd", "deribit"),
                _instrument("ins-btc-paper", "sym-btcusd", "paper", source_exchange="kraken"),
                _instrument("ins-eth-kraken", "sym-ethusd", "kraken"),
                _instrument("ins-sol-kraken", "sym-solusd", "kraken"),
                _instrument("ins-btceur-kraken", "sym-btceur", "kraken"),
                _instrument("ins-ltc-kraken", "sym-ltcusd", "kraken"),
                _instrument("ins-xrp-kraken", "sym-xrpusd", "kraken"),
                _spec("ins-btc-kraken", funding_type="spot_margin_rollover"),
                _spec("ins-btc-binance"),
                _spec("ins-btc-btnl", prov=_Prov(certified=False)),
                _spec("ins-btc-perp", instrument_kind="perpetual"),
                _spec("ins-btc-paper"),
                _spec("ins-eth-kraken"),
                _spec("ins-sol-kraken"),
                _spec("ins-btceur-kraken"),
                _spec("ins-xrp-kraken"),
                _candle("ins-btc-kraken", _M, 60_000.0),
                _candle("ins-btc-binance", _M, 60_500.0),
                _candle("ins-btc-btnl", _M, 64_485.0),
                _candle("ins-btc-perp", _M, 58_000.0),
                _candle("ins-btc-paper", _M, 57_000.0),
                _candle("ins-eth-kraken", _M, 3_000.0),
                _candle("ins-ltc-kraken", _M, 90.0),
                _candle("ins-xrp-kraken", _M, 0.5),
                _candle("ins-btceur-kraken", _M, 55_000.0),
                _candle("ins-btc-kraken", _M - timedelta(minutes=1), 59_900.0),
                _candle("ins-btc-kraken", _M + timedelta(hours=3), 99_999.0),
                _candle("ins-btc-kraken", _M, 77_777.0, _CandleSpec(timeframe="5m")),
                _candle(
                    "ins-btc-kraken",
                    _M + timedelta(minutes=1),
                    88_888.0,
                    _CandleSpec(complete=False),
                ),
                _candle(
                    "ins-sol-kraken",
                    _M,
                    140.0,
                    _CandleSpec(public_id="cdl-sol-v1", timestamp=_M, known_to=_CORRECTION_AT),
                ),
                _candle(
                    "ins-sol-kraken",
                    _M,
                    145.0,
                    _CandleSpec(public_id="cdl-sol-v1", timestamp=_CORRECTION_AT),
                ),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


class TestEligiblePlanes:
    """Cover the planes the read must admit."""

    async def test_admits_spot_usd_real_venue_planes(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A spot, venue-certified, real-venue BTC-USD plane resolves with its close."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        planes = {(row["exchange"], row["close"]) for row in rows}
        assert planes == {("binance", 60_500.0), ("kraken", 60_000.0)}
        assert all(row["base"] == "BTC" and row["quote"] == "USD" for row in rows)

    async def test_margin_capable_spot_listing_is_eligible(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The production topology: the sole kraken spot listing is margin-capable.

        Kraken offers leverage on its one spot order book, so the venue's only
        ``BTC-USD`` spot instrument carries ``funding_type='spot_margin_rollover'``
        while classifying ``instrument_kind='spot'``. Its candles are ordinary spot
        prices; excluding them withheld every real crypto plane in production.
        """
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        kraken = [row for row in rows if row["exchange"] == "kraken"]
        assert [(row["instrument_public_id"], row["close"]) for row in kraken] == [
            ("ins-btc-kraken", 60_000.0)
        ]

    async def test_multiple_currencies_and_venues_are_sorted(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Rows walk (base, quote, exchange, open_at) deterministically."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC", "ETH"], _M, _M, _FUTURE)
        assert [(row["base"], row["exchange"]) for row in rows] == [
            ("BTC", "binance"),
            ("BTC", "kraken"),
            ("ETH", "kraken"),
        ]

    async def test_carries_candle_version_identity(self, repository: SQLAlchemyRepository) -> None:
        """Every row carries the immutable candle id, public id and version time."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["ETH"], _M, _M, _FUTURE)
        assert len(rows) == 1
        row = rows[0]
        assert row["instrument_public_id"] == "ins-eth-kraken"
        assert row["native_symbol"] == "ETH-USD"
        assert isinstance(row["candle_id"], int) and row["candle_id"] > 0
        assert row["candle_public_id"] != ""
        assert row["open_at"] == _M
        assert row["candle_timestamp"] == _M

    async def test_certified_spec_with_non_active_status_is_eligible(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Lifecycle ``status`` is not certification, so inactive and null both price.

        ``status`` carries no vocabulary contract — no CHECK constraint, writers
        producing ``active``/``inactive``/null against a column comment suggesting
        ``online``/``offline`` — and a delisted market's historical candles stay
        perfectly good prices. This is the author-time tripwire against a future
        "harden it with status" change, which would otherwise pass every other
        assertion in this module.
        """
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-kkk", "KKK-USD", "KKK", "USD"),
                    _symbol("sym-lll", "LLL-USD", "LLL", "USD"),
                    _instrument("ins-kkk-kraken", "sym-kkk", "kraken"),
                    _instrument("ins-lll-kraken", "sym-lll", "kraken"),
                    _spec("ins-kkk-kraken", prov=_Prov(status="inactive")),
                    _spec("ins-lll-kraken", prov=_Prov(status=None)),
                    _candle("ins-kkk-kraken", _M, 12.0),
                    _candle("ins-lll-kraken", _M, 13.0),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_crypto_usd_plane_candles(["KKK", "LLL"], _M, _M, _FUTURE)
        assert [row["close"] for row in rows] == [12.0, 13.0]

    async def test_empty_request_skips_the_query(self, repository: SQLAlchemyRepository) -> None:
        """No requested currencies short-circuit without touching the database."""
        assert await repository.get_pnl_crypto_usd_plane_candles([], _M, _M, _FUTURE) == []


class TestEligibilityExclusions:
    """Cover each ineligibility that must withhold a plane."""

    async def test_uncertified_relay_instrument_is_excluded(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A market-data-only relay with null spec provenance never prices a plane.

        ``BTC-USD-BTNL`` relays another venue's book under ``exchange='kraken'``
        with a structurally uncertified spec. It shares the certified listing's
        ``(base, quote, exchange)`` triple and its own diverging close, so were it
        eligible it would make every colliding minute ambiguous.
        """
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        assert "ins-btc-btnl" not in {row["instrument_public_id"] for row in rows}
        assert 64_485.0 not in {row["close"] for row in rows}

    async def test_uncertified_relay_creates_no_valuation_ambiguity(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """With the relay withheld, the surviving kraken plane prices the leg."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        leg = value_currency("kraken", "BTC", 2.0, _M + timedelta(minutes=1), _evidence(rows))
        assert leg.reason is None
        assert leg.usd_value == 120_000.0

    async def test_derivative_instrument_is_excluded(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A perpetual (non-spot) instrument is never an eligible plane."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        assert "deribit" not in {row["exchange"] for row in rows}

    async def test_paper_venue_is_excluded(self, repository: SQLAlchemyRepository) -> None:
        """A paper-family instrument can never price a live basket."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        assert "paper" not in {row["exchange"] for row in rows}

    async def test_non_usd_quote_is_excluded(self, repository: SQLAlchemyRepository) -> None:
        """A BTC-EUR spot listing is not a USD plane even on a real venue."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        assert all(row["quote"] == "USD" for row in rows)
        assert 55_000.0 not in {row["close"] for row in rows}

    async def test_instrument_without_spec_is_excluded(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A missing spec cannot prove the spot classification, so LTC withholds."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["LTC"], _M, _M, _FUTURE)
        assert rows == []

    async def test_inactive_symbol_is_excluded(self, repository: SQLAlchemyRepository) -> None:
        """A symbol not active at the horizon cannot supply a plane."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["XRP"], _M, _M, _FUTURE)
        assert rows == []

    async def test_window_timeframe_and_finality_are_enforced(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Only finalized one-minute candles inside the window survive."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(
            ["BTC"], _M - timedelta(minutes=1), _M + timedelta(minutes=1), _FUTURE
        )
        assert sorted(row["close"] for row in rows) == [59_900.0, 60_000.0, 60_500.0]


class TestAsOfCorrection:
    """Cover the bitemporal knowledge-horizon threading."""

    async def test_correction_is_not_back_applied_before_it_is_known(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Reading before a candle correction returns the pre-correction close."""
        before = _CORRECTION_AT - timedelta(seconds=1)
        rows = await repository.get_pnl_crypto_usd_plane_candles(["SOL"], _M, _M, before)
        assert [row["close"] for row in rows] == [140.0]

    async def test_correction_is_seen_once_it_is_known(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Reading after the correction returns the superseding close."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["SOL"], _M, _M, _FUTURE)
        assert [row["close"] for row in rows] == [145.0]


class TestVersionCorrections:
    """Cover author-time ownership plus as-of unanimity over each owner version."""

    async def test_perpetual_respecced_to_spot_never_admits_the_candle(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A candle written while perpetual is never a retroactively eligible spot plane."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-aaa", "AAA-USD", "AAA", "USD"),
                    _instrument("ins-aaa-kraken", "sym-aaa", "kraken"),
                    _spec("ins-aaa-kraken", "perpetual", ver=_Ver(known_to=_CORRECTION_AT)),
                    _spec("ins-aaa-kraken", "spot", ver=_Ver(timestamp=_CORRECTION_AT)),
                    _candle("ins-aaa-kraken", _M, 500.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["AAA"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["AAA"], _M, _M, _FUTURE)
        assert before == []
        assert after == []

    async def test_spot_respecced_to_perpetual_preserves_earlier_evidence(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """An earlier as_of still sees the candle that was spot when written."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-bbb", "BBB-USD", "BBB", "USD"),
                    _instrument("ins-bbb-kraken", "sym-bbb", "kraken"),
                    _spec("ins-bbb-kraken", "spot", ver=_Ver(known_to=_CORRECTION_AT)),
                    _spec("ins-bbb-kraken", "perpetual", ver=_Ver(timestamp=_CORRECTION_AT)),
                    _candle("ins-bbb-kraken", _M, 700.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["BBB"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["BBB"], _M, _M, _FUTURE)
        assert [row["close"] for row in before] == [700.0]
        assert after == []

    async def test_symbol_redenomination_is_not_back_applied(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A later base correction excludes the plane once the conflict is known."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-ccc", "CCC-USD", "CCC", "USD", _Ver(known_to=_CORRECTION_AT)),
                    _symbol("sym-ccc", "CCC-USD", "DDD", "USD", _Ver(timestamp=_CORRECTION_AT)),
                    _instrument("ins-ccc-kraken", "sym-ccc", "kraken"),
                    _spec("ins-ccc-kraken"),
                    _candle("ins-ccc-kraken", _M, 900.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["CCC"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["CCC"], _M, _M, _FUTURE)
        assert [row["close"] for row in before] == [900.0]
        assert after == []

    async def test_instrument_inactive_at_candle_time_is_excluded(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A candle written after its instrument was delisted has no author-time owner."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-eee", "EEE-USD", "EEE", "USD"),
                    _instrument(
                        "ins-eee-kraken",
                        "sym-eee",
                        "kraken",
                        ver=_Ver(known_to=_M - timedelta(minutes=1)),
                    ),
                    _spec("ins-eee-kraken"),
                    _candle("ins-eee-kraken", _M, 300.0),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_crypto_usd_plane_candles(["EEE"], _M, _M, _FUTURE)
        assert rows == []

    async def test_later_decertification_does_not_de_plane_earlier_candle(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Certification is author-time only, so a later null provenance changes nothing.

        Provenance legitimately changes across versions. Quantifying it over every
        known version would let one metadata refresh retroactively de-plane an
        instrument's entire candle history — the exact harm the unanimity EXISTS
        was built to prevent, which is why it keeps quantifying kind alone.
        """
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-fff", "FFF-USD", "FFF", "USD"),
                    _instrument("ins-fff-kraken", "sym-fff", "kraken"),
                    _spec("ins-fff-kraken", ver=_Ver(known_to=_CORRECTION_AT)),
                    _spec(
                        "ins-fff-kraken",
                        ver=_Ver(timestamp=_CORRECTION_AT),
                        prov=_Prov(certified=False),
                    ),
                    _candle("ins-fff-kraken", _M, 400.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["FFF"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["FFF"], _M, _M, _FUTURE)
        assert [row["close"] for row in before] == [400.0]
        assert [row["close"] for row in after] == [400.0]

    async def test_later_delisting_does_not_de_plane_earlier_candle(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """An ``active`` to ``inactive`` transition never withdraws earlier prices."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-ggg", "GGG-USD", "GGG", "USD"),
                    _instrument("ins-ggg-kraken", "sym-ggg", "kraken"),
                    _spec(
                        "ins-ggg-kraken",
                        ver=_Ver(known_to=_CORRECTION_AT),
                        prov=_Prov(status="active"),
                    ),
                    _spec(
                        "ins-ggg-kraken",
                        ver=_Ver(timestamp=_CORRECTION_AT),
                        prov=_Prov(status="inactive"),
                    ),
                    _candle("ins-ggg-kraken", _M, 450.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["GGG"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["GGG"], _M, _M, _FUTURE)
        assert [row["close"] for row in before] == [450.0]
        assert [row["close"] for row in after] == [450.0]

    async def test_later_certification_never_admits_an_uncertified_candle(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Admission stays monotonic: nothing later makes a historical candle eligible."""
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-hhh", "HHH-USD", "HHH", "USD"),
                    _instrument("ins-hhh-kraken", "sym-hhh", "kraken"),
                    _spec(
                        "ins-hhh-kraken",
                        ver=_Ver(known_to=_CORRECTION_AT),
                        prov=_Prov(certified=False),
                    ),
                    _spec("ins-hhh-kraken", ver=_Ver(timestamp=_CORRECTION_AT)),
                    _candle("ins-hhh-kraken", _M, 600.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["HHH"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["HHH"], _M, _M, _FUTURE)
        assert before == []
        assert after == []

    async def test_funding_type_changes_never_alter_eligibility(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A funding-model change in either direction leaves a spot plane eligible.

        A venue enabling or withdrawing leverage on its spot order book rewrites
        ``funding_type`` without touching the candle stream, so neither direction
        may move the plane in or out of eligibility while every version stays spot.
        """
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-iii", "III-USD", "III", "USD"),
                    _symbol("sym-jjj", "JJJ-USD", "JJJ", "USD"),
                    _instrument("ins-iii-kraken", "sym-iii", "kraken"),
                    _instrument("ins-jjj-kraken", "sym-jjj", "kraken"),
                    _spec("ins-iii-kraken", ver=_Ver(known_to=_CORRECTION_AT)),
                    _spec(
                        "ins-iii-kraken",
                        funding_type="spot_margin_rollover",
                        ver=_Ver(timestamp=_CORRECTION_AT),
                    ),
                    _spec(
                        "ins-jjj-kraken",
                        funding_type="spot_margin_rollover",
                        ver=_Ver(known_to=_CORRECTION_AT),
                    ),
                    _spec("ins-jjj-kraken", ver=_Ver(timestamp=_CORRECTION_AT)),
                    _candle("ins-iii-kraken", _M, 800.0),
                    _candle("ins-jjj-kraken", _M, 850.0),
                ]
            )
            await session.commit()
        before = await repository.get_pnl_crypto_usd_plane_candles(
            ["III", "JJJ"], _M, _M, _CORRECTION_AT - timedelta(seconds=1)
        )
        after = await repository.get_pnl_crypto_usd_plane_candles(["III", "JJJ"], _M, _M, _FUTURE)
        assert [row["close"] for row in before] == [800.0, 850.0]
        assert [row["close"] for row in after] == [800.0, 850.0]


class TestAmbiguousVenuePlane:
    """Cover the DAL surfacing a same-venue plane ambiguity for the valuator.

    Both layers are asserted deliberately. SQL-level de-duplication is forbidden
    in either of its shapes: a ``GROUP BY``/``HAVING count = 1`` would erase the
    plane and downgrade a real evidentiary condition to a retryable missing rate,
    while ``DISTINCT ON`` would silently elect one of two genuinely different
    order books. A DAL-only assertion could not distinguish "no ambiguity" from
    "ambiguity resolved by row order".
    """

    async def test_two_instruments_on_one_venue_plane_both_surface(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Two distinct CERTIFIED instruments on one venue plane both surface."""
        await _seed_certified_twins(repository)
        rows = await repository.get_pnl_crypto_usd_plane_candles(["DUP"], _M, _M, _FUTURE)
        assert {(row["exchange"], row["instrument_public_id"], row["close"]) for row in rows} == {
            ("kraken", "ins-dup-a", 111.0),
            ("kraken", "ins-dup-b", 222.0),
        }

    async def test_certified_twins_withhold_the_leg_as_ambiguous_plane(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The valuator refuses to elect a price when certification cannot separate them."""
        await _seed_certified_twins(repository)
        rows = await repository.get_pnl_crypto_usd_plane_candles(["DUP"], _M, _M, _FUTURE)
        leg = value_currency("kraken", "DUP", 1.0, _M + timedelta(minutes=1), _evidence(rows))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "ambiguous_plane"
