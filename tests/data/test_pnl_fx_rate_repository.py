"""Tests for the P&L FX rate plane repository reads.

Pins the read that prices a foreign-currency flow off our own candle plane:
forex-only venue discovery, exact pinned-plane filtering, both orientations,
the finalized/timeframe/window filters, and as-of denomination proof.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import pytest
from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import tuple_
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable
from sqlalchemy.sql import Select

from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.pnl_timeline_service import load_basket_fiat_evidence
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import FxConversionElection
from snapper.data.models import FxConversionProof
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _pnl_fx_symbol_proof_filters
from snapper.data.repository import where_active
from snapper.data.repository_types import PnlFxRatePlane

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_TS = _NOW - timedelta(hours=6)
_M = _NOW - timedelta(hours=1)
_SESSION = "00000000-0000-7000-8000-000000000801"


def _symbol(
    public_id: str,
    native_symbol: str,
    base: str,
    quote: str,
    asset_type: str = "forex",
    timestamp: datetime = _TS,
    known_to: datetime = KNOWN_TO_MAX,
) -> Symbol:
    """Build one active symbol carrying the currency legs the read matches on."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base=base,
        quote=quote,
        asset_type=asset_type,
        created_at=timestamp,
        timestamp=timestamp,
        known_to=known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    timestamp: datetime = _TS,
) -> Instrument:
    """Build one active instrument listing a symbol on a venue."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        requires_ai_review=False,
        timestamp=timestamp,
        session_id=_SESSION,
        sequence_id=1,
    )


def _candle(
    instrument_public_id: str,
    open_at: datetime,
    close: float,
    *,
    timeframe: str = "1m",
    complete: bool = True,
) -> Candle:
    """Build one candle for the FX series."""
    return Candle(
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timeframe=timeframe,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1.0,
        source="native",
        complete=complete,
        timestamp=open_at,
        session_id=_SESSION,
        sequence_id=1,
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with FX pairs on two venues."""
    db_path = tmp_path / "pnl-fx.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Candle.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    async with repo.session() as s:
        s.add_all(
            [
                _symbol("sym-eurusd", "EUR-USD", "EUR", "USD"),
                _symbol("sym-usdeur", "USD-EUR", "USD", "EUR"),
                _symbol("sym-usdpln", "USD-PLN", "USD", "PLN"),
                _symbol(
                    "sym-eurusd-perp",
                    "EUR-USD-PERP",
                    "EUR",
                    "USD",
                    "crypto",
                ),
                _instrument("ins-eurusd-kraken", "sym-eurusd", "kraken"),
                _instrument("ins-eurusd-walutomat", "sym-eurusd", "walutomat"),
                _instrument("ins-usdeur-kraken", "sym-usdeur", "kraken"),
                _instrument("ins-usdpln-kraken", "sym-usdpln", "kraken"),
                _instrument(
                    "ins-eurusd-kraken-futures",
                    "sym-eurusd-perp",
                    "kraken_futures",
                ),
                _candle("ins-eurusd-kraken", _M, 1.25),
                _candle("ins-eurusd-walutomat", _M, 1.30),
                _candle("ins-usdeur-kraken", _M, 0.8),
                _candle("ins-usdpln-kraken", _M, 4.0),
                _candle("ins-eurusd-kraken-futures", _M, 1.99),
                _candle("ins-eurusd-kraken", _M - timedelta(minutes=1), 1.20),
                _candle("ins-eurusd-kraken", _M + timedelta(hours=3), 9.99),
                _candle("ins-eurusd-kraken", _M, 7.77, timeframe="5m"),
                _candle("ins-eurusd-kraken", _M + timedelta(minutes=1), 8.88, complete=False),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_real_repository_basket_shadow_matches_returned_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Enabled basket collection pins the same manifest, plane, and close it returns."""
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[])
    with activate_fx_shadow_context(context):
        rates, venues, _ = await load_basket_fiat_evidence(
            repository,
            frozenset({"EUR"}),
            _M + timedelta(minutes=1),
            _M + timedelta(minutes=1),
            _NOW + timedelta(days=1),
        )
    await context.flush(repository)
    async with repository.session() as session:
        election = (await session.execute(select(FxConversionElection))).scalars().one()
        proof = (await session.execute(select(FxConversionProof))).scalars().one()
    assert venues == {("EUR", "USD"): ("EUR", "USD", "kraken")}
    assert rates[("EUR", "USD", "kraken", _M + timedelta(minutes=1))] == 1.25
    assert election.calculation_version == "5B.2"
    assert election.selected_source_exchange == "kraken"
    assert proof.conversion_minute == _M + timedelta(minutes=1)
    assert proof.raw_close_decimal == "1.25"


class TestGetPnlFxRateExchanges:
    """Cover bounded spot-FX plane discovery."""

    async def test_discovers_each_concrete_forex_venue_plane(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Two spot venues on one pair remain distinct discovery candidates."""
        planes = await repository.get_pnl_fx_rate_exchanges(
            [("EUR", "USD")], _M, _M, _NOW + timedelta(days=1)
        )
        assert planes == [
            ("EUR", "USD", "kraken"),
            ("EUR", "USD", "walutomat"),
        ]

    async def test_both_orientations_are_discovered_on_one_venue(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Direct and inverse listings remain eligible on the selected venue."""
        planes = await repository.get_pnl_fx_rate_exchanges(
            [("EUR", "USD"), ("USD", "EUR")],
            _M,
            _M,
            _NOW + timedelta(days=1),
        )
        assert planes == [
            ("EUR", "USD", "kraken"),
            ("EUR", "USD", "walutomat"),
            ("USD", "EUR", "kraken"),
        ]

    async def test_requires_bounded_finalized_one_minute_evidence(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """An unfinalized minute alone cannot advertise a usable venue plane."""
        planes = await repository.get_pnl_fx_rate_exchanges(
            [("EUR", "USD")],
            _M + timedelta(minutes=1),
            _M + timedelta(minutes=1),
            _NOW + timedelta(days=1),
        )
        assert planes == []

    async def test_empty_request_skips_the_query(self, repository: SQLAlchemyRepository) -> None:
        """No requested currency legs return no candidate planes."""
        assert await repository.get_pnl_fx_rate_exchanges([], _M, _M, _NOW) == []

    async def test_perpetual_with_matching_legs_is_never_a_candidate(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A derivatives series cannot certify a spot conversion rate."""
        planes = await repository.get_pnl_fx_rate_exchanges(
            [("EUR", "USD")], _M, _M, _NOW + timedelta(days=1)
        )
        assert ("EUR", "USD", "kraken_futures") not in planes


def _original_discovery_query(
    pairs: list[tuple[str, str]], start: datetime, end: datetime, as_of: datetime
) -> Select[tuple[str, str | None, str]]:
    """Retain the exact be9 candle-join discovery expression as an equivalence oracle."""
    return (
        select(Symbol.base, Symbol.quote, Instrument.exchange)
        .select_from(Candle)
        .join(
            Instrument,
            and_(
                Candle.instrument_public_id == Instrument.public_id,
                Instrument.timestamp <= Candle.timestamp,
                Instrument.known_to > Candle.timestamp,
            ),
        )
        .join(
            Symbol,
            and_(
                Instrument.symbol_public_id == Symbol.public_id,
                Symbol.timestamp <= Candle.timestamp,
                Symbol.known_to > Candle.timestamp,
            ),
        )
        .where(
            tuple_(Symbol.base, Symbol.quote).in_(list(dict.fromkeys(pairs))),
            *_pnl_fx_symbol_proof_filters(as_of),
            Candle.timeframe == "1m",
            Candle.open_at >= start,
            Candle.open_at <= end,
            Candle.complete.is_(True),
            *where_active(Candle, as_of),
        )
        .distinct()
        .order_by(Symbol.base.asc(), Symbol.quote.asc(), Instrument.exchange.asc())
    )


async def _original_planes(
    repository: SQLAlchemyRepository, pair: tuple[str, str], minute: datetime, as_of: datetime
) -> list[PnlFxRatePlane]:
    """Execute the frozen original query against the same isolated fixture evidence."""
    async with repository.session() as session:
        result = await session.execute(_original_discovery_query([pair], minute, minute, as_of))
        return [(base, cast(str, quote), exchange) for base, quote, exchange in result.all()]


class TestDiscoveryCandleExistence:
    """Preserve candidate evidence while avoiding candle-row expansion before DISTINCT."""

    async def test_postgresql_prefilter_uses_one_typed_array_expression(
        self, repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PostgreSQL receives an array operand rather than another semijoin.

        Given: The real query builder under the PostgreSQL dialect branch.
        When: Its actual statement is captured before any database connection.
        Then: One typed UUID array limits scan keys inside the original EXISTS.
        """
        statements: list[Select[tuple[str, str, str]]] = []
        result = Mock(spec=Result)
        result.all.return_value = []

        def dialect(_repository: SQLAlchemyRepository) -> str:
            """Select the real PostgreSQL expression path without opening a DB."""
            return "postgresql"

        async def capture(
            _session: AsyncSession, statement: Executable
        ) -> Result[tuple[str, str, str]]:
            """Retain the real SQLAlchemy statement at its driver boundary."""
            assert isinstance(statement, Select)
            statements.append(statement)
            return cast(Result[tuple[str, str, str]], result)

        monkeypatch.setattr(SQLAlchemyRepository, "dialect_name", property(dialect))
        monkeypatch.setattr(AsyncSession, "execute", capture)
        assert await repository.get_pnl_fx_rate_exchanges([("EUR", "USD")], _M, _M, _NOW) == []
        assert len(statements) == 1
        compiled = str(statements[0].compile(dialect=postgresql.dialect()))
        assert "= ANY (CAST((SELECT array_agg(" in compiled
        assert "AS UUID[]" in compiled
        assert "EXISTS (SELECT *" in compiled

    async def test_candle_scan_prefilters_distinct_eligible_instruments(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Discard unrelated scan keys inside the same discovery statement.

        Given: Forex and unrelated perpetual instruments share the candle table.
        When: Discovery asks for one pair under its original historical proof.
        Then: One statement prefilters distinct instrument IDs and preserves planes.
        """
        statements: list[str] = []

        def capture(*args: object) -> None:
            """Record executed SQL without changing the repository or its rows."""
            statement = args[2]
            assert isinstance(statement, str)
            statements.append(" ".join(statement.split()))

        event.listen(repository.engine.sync_engine, "before_cursor_execute", capture)
        try:
            actual = await repository.get_pnl_fx_rate_exchanges([("EUR", "USD")], _M, _M, _NOW)
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", capture)
        assert len(statements) == 1
        assert "WITH pnl_fx_candidate_instruments AS" in statements[0]
        assert "SELECT DISTINCT instruments.public_id AS public_id" in statements[0]
        assert (
            "candles.instrument_public_id IN (SELECT pnl_fx_candidate_instruments.public_id"
            in statements[0]
        )
        assert "EXISTS (SELECT * FROM candles" in statements[0]
        assert actual == await _original_planes(repository, ("EUR", "USD"), _M, _NOW)

    async def test_executes_metadata_driven_candle_existence_query(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A real discovery query tests candle presence under historical metadata.

        Given: Multiple finalized candles and multiple venues for one currency pair.
        When: Candidate discovery executes against the actual fixture database.
        Then: Metadata drives a correlated candle EXISTS and matches the original rowset.
        """
        statements: list[str] = []

        def capture(*args: object) -> None:
            """Capture executed SQL without replacing the repository or database."""
            statement = args[2]
            assert isinstance(statement, str)
            statements.append(" ".join(statement.split()))

        event.listen(repository.engine.sync_engine, "before_cursor_execute", capture)
        try:
            actual = await repository.get_pnl_fx_rate_exchanges([("EUR", "USD")], _M, _M, _NOW)
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", capture)
        assert len(statements) == 1
        assert "FROM instruments JOIN symbols" in statements[0]
        assert "EXISTS (SELECT * FROM candles" in statements[0]
        assert actual == await _original_planes(repository, ("EUR", "USD"), _M, _NOW)

    async def test_expired_metadata_and_old_forex_classification_remain_candidates(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A current metadata shortcut cannot remove a valid historical candle author.

        Given: Expired forex metadata followed by crypto classification and another venue.
        When: The candle remains known at a later horizon within its original author bands.
        Then: Only the original venue qualifies, exactly as the original query returned.
        """
        changed_at = _M + timedelta(minutes=1)
        old_instrument = _instrument("ins-history", "sym-history", "walutomat")
        old_instrument.known_to = changed_at
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-history", "GBP-X", "GBP", "USD", known_to=changed_at),
                    _symbol("sym-history", "GBP-X", "GBP", "USD", "crypto", timestamp=changed_at),
                    old_instrument,
                    _instrument("ins-history", "sym-history", "kraken", timestamp=changed_at),
                    _candle("ins-history", _M, 1.2),
                ]
            )
            await session.commit()
        expected = [("GBP", "USD", "walutomat")]
        assert await _original_planes(repository, ("GBP", "USD"), _M, _NOW) == expected
        assert (
            await repository.get_pnl_fx_rate_exchanges([("GBP", "USD")], _M, _M, _NOW) == expected
        )

    async def test_nonoverlapping_author_bands_do_not_create_a_candidate(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Matching logical identifiers cannot replace a common candle author instant.

        Given: Symbol and instrument bands that meet at an exclusive upper boundary.
        When: A candle exists outside every common author interval.
        Then: Both query shapes return no candidate despite known forex classification.
        """
        boundary = _M - timedelta(minutes=1)
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-gap", "AUD-X", "AUD", "CAD", known_to=boundary),
                    _symbol("sym-gap", "AUD-X", "AUD", "CAD", timestamp=_M + timedelta(minutes=1)),
                    _instrument("ins-gap", "sym-gap", "walutomat", timestamp=boundary),
                    _candle("ins-gap", _M, 1.2),
                ]
            )
            await session.commit()
        assert await _original_planes(repository, ("AUD", "CAD"), _M, _NOW) == []
        assert await repository.get_pnl_fx_rate_exchanges([("AUD", "CAD")], _M, _M, _NOW) == []

    async def test_distinct_scan_key_retains_both_historical_author_planes(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Deduplicating scan keys cannot elect one metadata version early.

        Given: One logical instrument has two author bands with in-band candles.
        When: One broad discovery window includes both historical venues.
        Then: Both planes survive the shared instrument-ID prefilter.
        """
        changed_at = _M + timedelta(minutes=1)
        later_minute = _M + timedelta(minutes=2)
        earlier = _instrument("ins-two-bands", "sym-two-bands", "walutomat")
        earlier.known_to = changed_at
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-two-bands", "GBP-X", "GBP", "USD", known_to=changed_at),
                    _symbol("sym-two-bands", "GBP-X", "GBP", "USD", "crypto", timestamp=changed_at),
                    earlier,
                    _instrument("ins-two-bands", "sym-two-bands", "kraken", timestamp=changed_at),
                    _candle("ins-two-bands", _M, 1.2),
                    _candle("ins-two-bands", later_minute, 1.3),
                ]
            )
            await session.commit()
        assert await repository.get_pnl_fx_rate_exchanges(
            [("GBP", "USD")], _M, later_minute, _NOW
        ) == [("GBP", "USD", "kraken"), ("GBP", "USD", "walutomat")]

    @pytest.mark.parametrize(
        ("authored_offset", "retired_offset", "eligible"),
        [(0, None, True), (1, None, False), (-1, 0, False), (-1, 1, True)],
    )
    async def test_candle_version_horizon_boundaries_remain_exact(
        self,
        repository: SQLAlchemyRepository,
        authored_offset: int,
        retired_offset: int | None,
        eligible: bool,
    ) -> None:
        """Candidate existence retains inclusive knowledge and exclusive retirement bounds.

        Given: A candle authored or retired immediately around the response horizon.
        When: Discovery reads that fixed horizon.
        Then: Both queries preserve the exact active-version boundary behavior.
        """
        candle = _candle("ins-boundary", _M, 0.0)
        candle.timestamp = _NOW + timedelta(seconds=authored_offset)
        if retired_offset is not None:
            candle.known_to = _NOW + timedelta(seconds=retired_offset)
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-boundary", "CAD-X", "CAD", "CHF"),
                    _instrument("ins-boundary", "sym-boundary", "walutomat"),
                    candle,
                ]
            )
            await session.commit()
        expected = [("CAD", "CHF", "walutomat")] if eligible else []
        assert await _original_planes(repository, ("CAD", "CHF"), _M, _NOW) == expected
        assert (
            await repository.get_pnl_fx_rate_exchanges([("CAD", "CHF")], _M, _M, _NOW) == expected
        )

    @pytest.mark.parametrize("close", [0.0, -1.0])
    async def test_discovery_does_not_add_a_close_validity_filter(
        self, repository: SQLAlchemyRepository, close: float
    ) -> None:
        """Candidate presence remains distinct from downstream usable price coverage.

        Given: A finalized candle whose close is not a usable positive FX rate.
        When: Its plane is discovered for the original inclusive minute window.
        Then: The candidate remains present exactly as in the original query.
        """
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol("sym-price", "NZD-X", "NZD", "CHF"),
                    _instrument("ins-price", "sym-price", "walutomat"),
                    _candle("ins-price", _M, close),
                ]
            )
            await session.commit()
        expected = [("NZD", "CHF", "walutomat")]
        assert await _original_planes(repository, ("NZD", "CHF"), _M, _NOW) == expected
        assert (
            await repository.get_pnl_fx_rate_exchanges([("NZD", "CHF")], _M, _M, _NOW) == expected
        )


class TestGetPnlFxRateCandlesAt:
    """Cover the exact-minute variant a sparse consumer must use."""

    async def test_matches_only_the_requested_opens(self, repository: SQLAlchemyRepository) -> None:
        """Return the stored open and nothing for the absent one.

        Given a store holding a candle at one minute
        When exactly that open is requested alongside an unrelated one
        Then the read returns the stored row and nothing for the absent minute.
        """
        rows = await repository.get_pnl_fx_rate_candles_at(
            [("EUR", "USD", "kraken")],
            [_M, _M + timedelta(minutes=5)],
            _NOW + timedelta(days=1),
        )
        assert [(row["open_at"], row["close"]) for row in rows] == [(_M, 1.25)]

    async def test_skips_a_minute_the_caller_never_asked_for(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A minute outside the requested set is never returned, even if stored."""
        rows = await repository.get_pnl_fx_rate_candles_at(
            [("EUR", "USD", "kraken")],
            [_M + timedelta(minutes=5)],
            _NOW + timedelta(days=1),
        )
        assert rows == []

    async def test_empty_inputs_skip_the_query(self, repository: SQLAlchemyRepository) -> None:
        """Neither an empty plane list nor an empty minute list touches the store."""
        horizon = _NOW + timedelta(days=1)
        assert await repository.get_pnl_fx_rate_candles_at([], [_M], horizon) == []
        assert (
            await repository.get_pnl_fx_rate_candles_at([("EUR", "USD", "kraken")], [], horizon)
            == []
        )

    async def test_batches_a_minute_list_beyond_one_bind_group(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Issue every batch without losing or duplicating a row.

        Given more minutes than one bound-parameter batch carries
        When the exact-minute read runs
        Then every batch is issued and the stored row is still returned once.
        """
        minutes = [_M + timedelta(minutes=offset) for offset in range(900)]
        rows = await repository.get_pnl_fx_rate_candles_at(
            [("EUR", "USD", "kraken")], minutes, _NOW + timedelta(days=1)
        )
        opens = [row["open_at"] for row in rows]
        assert set(opens) <= set(minutes)
        assert len(opens) == len(set(opens))
        assert (_M, 1.25) in [(row["open_at"], row["close"]) for row in rows]
        assert opens == sorted(opens)


class TestGetPnlFxRateCandles:
    """Cover the pinned FX rate candle read."""

    async def test_sparse_intervals_intersect_bounds_and_keep_one_proven_query(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Sparse predicates narrow one query without weakening FX truth filters.

        Given: Direct, inverse, rival, perpetual, unfinalized, and other-timeframe rows.
        When: Two intervals include the original window and an out-of-bounds later row.
        Then: One query returns only proven original-window spot closes in stable order.
        """
        statements: list[str] = []

        def capture(*args: object) -> None:
            """Record actual statement count without substituting database results."""
            statement = args[2]
            assert isinstance(statement, str)
            statements.append(statement)

        event.listen(repository.engine.sync_engine, "before_cursor_execute", capture)
        try:
            rows = await repository.get_pnl_fx_rate_candles(
                [
                    ("EUR", "USD", "kraken"),
                    ("EUR", "USD", "walutomat"),
                    ("USD", "EUR", "kraken"),
                    ("EUR", "USD", "kraken_futures"),
                ],
                _M - timedelta(minutes=1),
                _M + timedelta(minutes=1),
                _NOW + timedelta(days=1),
                open_intervals=(
                    (_M - timedelta(minutes=1), _M + timedelta(minutes=1)),
                    (_M + timedelta(hours=3), _M + timedelta(hours=3)),
                ),
            )
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", capture)
        assert len(statements) == 1
        assert [
            (row["open_at"], row["base"], row["quote"], row["exchange"], row["close"])
            for row in rows
        ] == [
            (_M - timedelta(minutes=1), "EUR", "USD", "kraken", 1.20),
            (_M, "EUR", "USD", "kraken", 1.25),
            (_M, "EUR", "USD", "walutomat", 1.30),
            (_M, "USD", "EUR", "kraken", 0.8),
        ]

    async def test_matches_exact_currency_legs_and_exchange(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The requested plane resolves through all three identity dimensions."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken")], _M, _M, _NOW + timedelta(days=1)
        )
        assert [(row["base"], row["quote"], row["exchange"], row["close"]) for row in rows] == [
            ("EUR", "USD", "kraken", 1.25)
        ]

    async def test_carries_candle_version_identity(self, repository: SQLAlchemyRepository) -> None:
        """Each row carries the candle version identity a fiat leg records for provenance."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken")], _M, _M, _NOW + timedelta(days=1)
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["instrument_public_id"] == "ins-eurusd-kraken"
        assert row["native_symbol"] == "EUR-USD"
        assert isinstance(row["candle_id"], int)
        assert row["candle_id"] > 0
        assert row["candle_public_id"] != ""
        assert row["candle_timestamp"] == _M

    async def test_pinned_exchange_ignores_rival_at_same_pair_and_minute(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A rival venue cannot enter a request pinned to Walutomat."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "walutomat")], _M, _M, _NOW + timedelta(days=1)
        )
        assert [(row["exchange"], row["close"]) for row in rows] == [("walutomat", 1.30)]

    async def test_both_orientations_load_on_the_same_selected_venue(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """One venue pin can request its direct and inverse spot listings."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken"), ("USD", "EUR", "kraken")],
            _M,
            _M,
            _NOW + timedelta(days=1),
        )
        by_pair = {(row["base"], row["quote"]): row["close"] for row in rows}
        assert by_pair == {("EUR", "USD"): 1.25, ("USD", "EUR"): 0.8}

    async def test_unlisted_plane_yields_nothing(self, repository: SQLAlchemyRepository) -> None:
        """An unavailable exchange plane returns no substitute venue's rows."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "bitstamp")], _M, _M, _NOW + timedelta(days=1)
        )
        assert rows == []

    async def test_empty_request_skips_the_query(self, repository: SQLAlchemyRepository) -> None:
        """No requested planes short-circuit without touching the database."""
        assert await repository.get_pnl_fx_rate_candles([], _M, _M, _NOW) == []

    async def test_window_timeframe_and_finality_are_enforced(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Only finalized one-minute rows inside the pinned plane window survive."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken")],
            _M - timedelta(minutes=1),
            _M + timedelta(minutes=1),
            _NOW + timedelta(days=1),
        )
        assert sorted(row["close"] for row in rows) == [1.20, 1.25]

    async def test_rows_are_sorted_by_open_at_then_plane(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Ordering walks minutes forward before the plane identity."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken")],
            _M - timedelta(minutes=1),
            _M,
            _NOW + timedelta(days=1),
        )
        assert [row["open_at"] for row in rows] == sorted(row["open_at"] for row in rows)

    @pytest.mark.parametrize("sparse", [False, True])
    async def test_rate_candle_requires_as_of_denomination_unanimity(
        self, repository: SQLAlchemyRepository, sparse: bool
    ) -> None:
        """Quote conflicts withhold both labels with or without interval pruning.

        Given: Known denomination disagreement and separate denomination-preserving churn.
        When: Original or interval-filtered candle reads use horizons before and after revision.
        Then: Both disputed labels are refused while proven historical metadata remains usable.
        """
        revised_at = _M + timedelta(minutes=30)
        crypto_at = _M - timedelta(seconds=20)
        forex_at = _M + timedelta(seconds=20)
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(
                        "sym-gbpx",
                        "GBP-X",
                        "GBP",
                        "PLN",
                        known_to=revised_at,
                    ),
                    _symbol(
                        "sym-gbpx",
                        "GBP-X",
                        "GBP",
                        "USD",
                        timestamp=revised_at,
                    ),
                    _instrument("ins-gbpx", "sym-gbpx", "walutomat"),
                    _candle("ins-gbpx", _M, 5.0),
                    _symbol(
                        "sym-audx",
                        "AUD-X",
                        "AUD",
                        "USD",
                        known_to=crypto_at,
                    ),
                    _symbol(
                        "sym-audx",
                        "AUD-X",
                        "AUD",
                        "USD",
                        "crypto",
                        timestamp=crypto_at,
                        known_to=forex_at,
                    ),
                    _symbol(
                        "sym-audx",
                        "AUD-X",
                        "AUD",
                        "USD",
                        timestamp=forex_at,
                    ),
                    _instrument("ins-audx", "sym-audx", "walutomat"),
                    _candle("ins-audx", _M, 0.65),
                ]
            )
            await session.commit()
        before_revision = revised_at - timedelta(seconds=1)
        intervals = ((_M, _M),) if sparse else ()
        before_planes = await repository.get_pnl_fx_rate_exchanges(
            [("GBP", "PLN"), ("GBP", "USD")],
            _M,
            _M,
            before_revision,
        )
        before_rows = await repository.get_pnl_fx_rate_candles(
            [("GBP", "PLN", "walutomat"), ("GBP", "USD", "walutomat")],
            _M,
            _M,
            before_revision,
            open_intervals=intervals,
        )
        assert before_planes == [("GBP", "PLN", "walutomat")]
        assert [(row["base"], row["quote"], row["close"]) for row in before_rows] == [
            ("GBP", "PLN", 5.0)
        ]
        after_planes = await repository.get_pnl_fx_rate_exchanges(
            [("GBP", "PLN"), ("GBP", "USD")], _M, _M, _NOW
        )
        old_label_rows = await repository.get_pnl_fx_rate_candles(
            [("GBP", "PLN", "walutomat")], _M, _M, _NOW, open_intervals=intervals
        )
        new_label_rows = await repository.get_pnl_fx_rate_candles(
            [("GBP", "USD", "walutomat")], _M, _M, _NOW, open_intervals=intervals
        )
        assert after_planes == []
        assert old_label_rows == []
        assert new_label_rows == []
        churn_planes = await repository.get_pnl_fx_rate_exchanges([("AUD", "USD")], _M, _M, _NOW)
        churn_rows = await repository.get_pnl_fx_rate_candles(
            [("AUD", "USD", "walutomat")], _M, _M, _NOW, open_intervals=intervals
        )
        assert churn_planes == [("AUD", "USD", "walutomat")]
        assert [(row["base"], row["quote"], row["close"]) for row in churn_rows] == [
            ("AUD", "USD", 0.65)
        ]

    async def test_perpetual_is_rejected_even_when_explicitly_requested(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The forex guard rejects a matching derivatives exchange plane."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD", "kraken_futures")],
            _M,
            _M,
            _NOW + timedelta(days=1),
        )
        assert rows == []
