"""Tests for the Phase-5B crypto→USD plane repository read.

Pins the read that prices a held crypto currency off our own finalized candle
plane: the spot / non-margin / real-venue / exact-USD-quote eligibility proof,
the as-of version threading (later corrections never back-applied), deterministic
ordering, and the candle VERSION identity fields carried for audit provenance.
Each eligibility rule is excluded one at a time so a single relaxed filter cannot
silently admit an ineligible price plane.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository

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


def _spec(
    instrument_public_id: str,
    instrument_kind: str | None = "spot",
    funding_type: str | None = None,
    ver: _Ver = _Ver(),
) -> InstrumentSpec:
    """Build one instrument spec proving the spot / funding classification."""
    return InstrumentSpec(
        instrument_public_id=instrument_public_id,
        instrument_kind=instrument_kind,
        funding_type=funding_type,
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
                _symbol("sym-ethusd", "ETH-USD", "ETH", "USD"),
                _symbol("sym-solusd", "SOL-USD", "SOL", "USD"),
                _symbol("sym-btceur", "BTC-EUR", "BTC", "EUR"),
                _symbol("sym-ltcusd", "LTC-USD", "LTC", "USD"),
                _symbol("sym-xrpusd", "XRP-USD", "XRP", "USD", _Ver(known_to=_M)),
                _instrument("ins-btc-kraken", "sym-btcusd", "kraken"),
                _instrument("ins-btc-binance", "sym-btcusd", "binance"),
                _instrument("ins-btc-margin", "sym-btcusd", "kraken_margin"),
                _instrument("ins-btc-perp", "sym-btcusd", "deribit"),
                _instrument("ins-btc-paper", "sym-btcusd", "paper", source_exchange="kraken"),
                _instrument("ins-eth-kraken", "sym-ethusd", "kraken"),
                _instrument("ins-sol-kraken", "sym-solusd", "kraken"),
                _instrument("ins-btceur-kraken", "sym-btceur", "kraken"),
                _instrument("ins-ltc-kraken", "sym-ltcusd", "kraken"),
                _instrument("ins-xrp-kraken", "sym-xrpusd", "kraken"),
                _spec("ins-btc-kraken"),
                _spec("ins-btc-binance"),
                _spec("ins-btc-margin", funding_type="spot_margin_rollover"),
                _spec("ins-btc-perp", instrument_kind="perpetual"),
                _spec("ins-btc-paper"),
                _spec("ins-eth-kraken"),
                _spec("ins-sol-kraken"),
                _spec("ins-btceur-kraken"),
                _spec("ins-xrp-kraken"),
                _candle("ins-btc-kraken", _M, 60_000.0),
                _candle("ins-btc-binance", _M, 60_500.0),
                _candle("ins-btc-margin", _M, 59_000.0),
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
        """A spot, non-margin, real-venue BTC-USD plane resolves with its close."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        planes = {(row["exchange"], row["close"]) for row in rows}
        assert planes == {("binance", 60_500.0), ("kraken", 60_000.0)}
        assert all(row["base"] == "BTC" and row["quote"] == "USD" for row in rows)

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

    async def test_empty_request_skips_the_query(self, repository: SQLAlchemyRepository) -> None:
        """No requested currencies short-circuit without touching the database."""
        assert await repository.get_pnl_crypto_usd_plane_candles([], _M, _M, _FUTURE) == []


class TestEligibilityExclusions:
    """Cover each ineligibility that must withhold a plane."""

    async def test_spot_margin_instrument_is_excluded(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A spot-margin funding model can never certify an unleveraged plane."""
        rows = await repository.get_pnl_crypto_usd_plane_candles(["BTC"], _M, _M, _FUTURE)
        assert "kraken_margin" not in {row["exchange"] for row in rows}

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


class TestAmbiguousVenuePlane:
    """Cover the DAL surfacing a same-venue plane ambiguity for the valuator."""

    async def test_two_instruments_on_one_venue_plane_both_surface(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Two distinct instruments pricing one venue plane both surface, undeduplicated."""
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
        rows = await repository.get_pnl_crypto_usd_plane_candles(["DUP"], _M, _M, _FUTURE)
        assert {(row["exchange"], row["instrument_public_id"], row["close"]) for row in rows} == {
            ("kraken", "ins-dup-a", 111.0),
            ("kraken", "ins-dup-b", 222.0),
        }
