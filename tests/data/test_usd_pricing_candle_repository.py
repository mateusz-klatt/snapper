"""Repository tests for clock-free active 1m candle USD pricing queries.

Pins both new reads on :class:`SQLAlchemyRepository`:
    - :meth:`get_active_latest_1m_candle_price`
    - :meth:`get_active_usd_fx_candle_candidates`

Also compiles the FX candle candidate query against the PostgreSQL dialect so
SQLite runtime behavior and PostgreSQL query structure are both verified.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect

from snapper.data import repository as repository_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


@dataclass(frozen=True)
class _FxPlane:
    """Seed controls for one forex metadata and candle plane."""

    identity: str
    base: str = "EUR"
    quote: str = "USD"
    exchange: str = "kraken"
    asset_type: str = "forex"
    can_market_data: bool = True
    closed: str = ""
    capability_exchange: str = "kraken"
    timeframe: str = "1m"
    has_candle: bool = True


@pytest.fixture
async def repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Yield an isolated in-memory repository with full schema."""
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@dataclass(frozen=True)
class _CandleSpec:
    """Seed parameters for one persisted Candle row."""

    public_id: str
    instrument_public_id: str
    open_at: datetime
    close: float
    timeframe: str = "1m"
    complete: bool = True
    source: str = "calculated"
    known_to: datetime = KNOWN_TO_MAX
    timestamp: datetime | None = None


async def _add_candle(repo: SQLAlchemyRepository, spec: _CandleSpec) -> None:
    """Persist one Candle row with explicit temporal and provenance fields."""
    async with repo.session() as session:
        session.add(
            Candle(
                public_id=spec.public_id,
                instrument_public_id=spec.instrument_public_id,
                open_at=spec.open_at,
                timeframe=spec.timeframe,
                open=spec.close,
                high=spec.close,
                low=spec.close,
                close=spec.close,
                volume=1.0,
                vwap=spec.close,
                trades=1,
                source=spec.source,
                complete=spec.complete,
                timestamp=spec.timestamp or spec.open_at,
                known_to=spec.known_to,
                session_id="test",
                sequence_id=1,
            )
        )
        await session.commit()


async def _seed_fx_plane(repo: SQLAlchemyRepository, plane: _FxPlane) -> None:
    """Persist Symbol, Instrument, capability, and optional candle for one plane."""
    symbol = Symbol(
        public_id=f"sym-{plane.identity}",
        native_symbol=f"NS-{plane.identity}",
        base=plane.base,
        quote=plane.quote,
        asset_type=plane.asset_type,
        created_at=_NOW,
        timestamp=_NOW,
        known_to=_NOW if plane.closed == "symbol" else KNOWN_TO_MAX,
        session_id="test",
        sequence_id=1,
    )
    instrument = Instrument(
        public_id=plane.identity,
        symbol_public_id=symbol.public_id,
        exchange=plane.exchange,
        timestamp=_NOW,
        known_to=_NOW if plane.closed == "instrument" else KNOWN_TO_MAX,
        session_id="test",
        sequence_id=1,
    )
    capability = SymbolExchangeCapability(
        public_id=f"cap-{plane.identity}",
        symbol_public_id=symbol.public_id,
        exchange=plane.capability_exchange,
        can_market_data=plane.can_market_data,
        created_at=_NOW,
        timestamp=_NOW,
        known_to=_NOW if plane.closed == "capability" else KNOWN_TO_MAX,
        session_id="test",
        sequence_id=1,
    )
    async with repo.session() as session:
        session.add_all([symbol, instrument, capability])
        await session.commit()
    if plane.has_candle:
        await _add_candle(
            repo,
            _CandleSpec(
                public_id=f"candle-{plane.identity}",
                instrument_public_id=plane.identity,
                open_at=_NOW - timedelta(minutes=1),
                close=1.25,
                timeframe=plane.timeframe,
                known_to=_NOW if plane.closed == "candle" else KNOWN_TO_MAX,
            ),
        )


@pytest.mark.asyncio
async def test_get_active_latest_1m_candle_price_selects_newest_active_1m(
    repo: SQLAlchemyRepository,
) -> None:
    """Newest active 1m candle wins, ignoring 5m bars, closed rows, and bus-time lag.

    Given: no candles (returns None), then an older active 1m bar, a newer 5m
        bar, a newer closed 1m bar, and a newest active provisional 1m bar
        whose ``timestamp`` is ahead of wall clock,
    When: ``get_active_latest_1m_candle_price`` runs,
    Then: absent returns ``None``, and present returns the newest active 1m row.
    """
    assert await repo.get_active_latest_1m_candle_price("missing-inst") is None
    await _add_candle(
        repo,
        _CandleSpec(
            public_id="c-old",
            instrument_public_id="inst-1",
            open_at=_NOW - timedelta(minutes=5),
            close=100.0,
        ),
    )
    await _add_candle(
        repo,
        _CandleSpec(
            public_id="c-5m",
            instrument_public_id="inst-1",
            open_at=_NOW,
            close=999.0,
            timeframe="5m",
        ),
    )
    await _add_candle(
        repo,
        _CandleSpec(
            public_id="c-closed",
            instrument_public_id="inst-1",
            open_at=_NOW + timedelta(minutes=1),
            close=888.0,
            known_to=_NOW,
        ),
    )
    await _add_candle(
        repo,
        _CandleSpec(
            public_id="c-newest",
            instrument_public_id="inst-1",
            open_at=_NOW - timedelta(minutes=1),
            close=105.5,
            complete=False,
            timestamp=_NOW + timedelta(seconds=30),
        ),
    )
    row = await repo.get_active_latest_1m_candle_price("inst-1")
    assert row is not None
    assert row["candle_public_id"] == "c-newest"
    assert row["instrument_public_id"] == "inst-1"
    assert row["close"] == 105.5
    assert row["open_at"] == _NOW - timedelta(minutes=1)
    assert row["open_at"].tzinfo is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"closed": "symbol"},
        {"closed": "instrument"},
        {"closed": "capability"},
        {"closed": "candle"},
        {"has_candle": False},
        {"timeframe": "5m"},
        {"exchange": "paper", "capability_exchange": "paper"},
        {"asset_type": "crypto"},
        {"can_market_data": False},
        {"base": "GBP"},
        {"capability_exchange": "walutomat"},
    ],
)
async def test_get_active_usd_fx_candle_candidates_enforces_eligibility_and_latest_per_plane(
    repo: SQLAlchemyRepository,
    change: dict[str, object],
) -> None:
    """Include direct and inverse FX planes with newest active 1m candle only.

    Given: eligible direct (EUR-USD) and inverse (USD-EUR) FX planes where the
        direct plane also has an older 1m candle, plus one excluded plane,
    When: ``get_active_usd_fx_candle_candidates('EUR')`` runs,
    Then: only the two eligible planes are returned with their newest active
        1m candle close and open_at.
    """
    await _seed_fx_plane(repo, _FxPlane("direct"))
    await _add_candle(
        repo,
        _CandleSpec(
            public_id="candle-direct-older",
            instrument_public_id="direct",
            open_at=_NOW - timedelta(minutes=5),
            close=1.10,
        ),
    )
    await _seed_fx_plane(repo, _FxPlane("inverse", base="USD", quote="EUR", exchange="walutomat"))
    await _seed_fx_plane(repo, replace(_FxPlane("excluded"), **change))

    rows = await repo.get_active_usd_fx_candle_candidates("EUR")
    assert [row["instrument_public_id"] for row in rows] == ["direct", "inverse"]
    assert {row["candle_public_id"] for row in rows} == {"candle-direct", "candle-inverse"}
    assert all(row["open_at"] == _NOW - timedelta(minutes=1) for row in rows)
    assert all(row["open_at"].tzinfo is not None for row in rows)


def test_fx_candle_query_compiles_for_postgresql_with_latest_subquery() -> None:
    """PostgreSQL compilation binds active sentinel and correlated latest 1m subquery.

    Given: the SQLAlchemy query built for ``get_active_usd_fx_candle_candidates``,
    When: compiled against the PostgreSQL dialect,
    Then: the SQL selects from ``instruments``, ``symbols``, ``symbol_exchange_capabilities``,
        and ``candles`` with the correlated ``ORDER BY ... DESC LIMIT 1`` subquery.
    """
    query = repository_module._active_usd_fx_candle_candidates_query("EUR")
    compiled = " ".join(str(query.compile(dialect=postgresql_dialect())).split())
    assert "FROM instruments JOIN symbols" in compiled
    assert "JOIN symbol_exchange_capabilities" in compiled
    assert "JOIN candles" in compiled
    assert "latest_fx_candle.timeframe =" in compiled
    assert "ORDER BY latest_fx_candle.open_at DESC, latest_fx_candle.id DESC LIMIT" in compiled
