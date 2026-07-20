"""Tests for the P&L FX rate candle read.

Pins the read that prices a foreign-currency flow off our own candle plane:
pair matching on the symbol's own ``base``/``quote`` legs, both orientations
answerable, the finalized/timeframe/window filters, and — the one that protects
reproducibility — a deterministic ordering when several venues list the same pair
at the same minute.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_TS = _NOW - timedelta(hours=6)
_M = _NOW - timedelta(hours=1)
_SESSION = "00000000-0000-7000-8000-000000000801"


def _symbol(public_id: str, native_symbol: str, base: str, quote: str) -> Symbol:
    """Build one active symbol carrying the currency legs the read matches on."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base=base,
        quote=quote,
        asset_type="forex",
        created_at=_TS,
        timestamp=_TS,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(public_id: str, symbol_public_id: str, exchange: str) -> Instrument:
    """Build one active instrument listing a symbol on a venue."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        requires_ai_review=False,
        timestamp=_TS,
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


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with FX pairs on two venues."""
    db_path = tmp_path / "pnl-fx.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Candle.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                _symbol("sym-eurusd", "EUR-USD", "EUR", "USD"),
                _symbol("sym-usdpln", "USD-PLN", "USD", "PLN"),
                _instrument("ins-eurusd-kraken", "sym-eurusd", "kraken"),
                _instrument("ins-eurusd-walutomat", "sym-eurusd", "walutomat"),
                _instrument("ins-usdpln-kraken", "sym-usdpln", "kraken"),
                _candle("ins-eurusd-kraken", _M, 1.25),
                _candle("ins-eurusd-walutomat", _M, 1.30),
                _candle("ins-usdpln-kraken", _M, 4.0),
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


class TestGetPnlFxRateCandles:
    """Cover the FX rate candle read."""

    async def test_matches_pair_on_currency_legs(self, repository: SQLAlchemyRepository) -> None:
        """A requested ``(base, quote)`` resolves through the symbol's own legs."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD")], _M, _M, _NOW + timedelta(days=1)
        )
        assert {(r["base"], r["quote"]) for r in rows} == {("EUR", "USD")}

    async def test_multiple_venues_are_ordered_deterministically(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Two venues quoting one pair at one minute return in a stable order.

        Row order decides which rate a caller folds into its map, so an unstable
        order would make the same request return different money on each call.
        """
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD")], _M, _M, _NOW + timedelta(days=1)
        )
        assert [(r["exchange"], r["close"]) for r in rows] == [
            ("kraken", 1.25),
            ("walutomat", 1.30),
        ]

    async def test_both_orientations_resolve_independently(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Requesting several pairs returns each one's own legs and closes."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD"), ("USD", "PLN")], _M, _M, _NOW + timedelta(days=1)
        )
        by_pair = {(r["base"], r["quote"]): r["close"] for r in rows if r["exchange"] == "kraken"}
        assert by_pair == {("EUR", "USD"): 1.25, ("USD", "PLN"): 4.0}

    async def test_unlisted_pair_yields_nothing(self, repository: SQLAlchemyRepository) -> None:
        """A pair no venue lists returns no rows rather than a substitute."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("JPY", "USD")], _M, _M, _NOW + timedelta(days=1)
        )
        assert rows == []

    async def test_empty_request_skips_the_query(self, repository: SQLAlchemyRepository) -> None:
        """No requested pairs short-circuits without touching the database."""
        assert await repository.get_pnl_fx_rate_candles([], _M, _M, _NOW) == []

    async def test_window_timeframe_and_finality_are_enforced(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Only finalized 1m candles inside the window contribute a rate.

        The fixture seeds an out-of-window candle, a 5m candle, and an unfinalized
        one at the same pair; none may leak into a conversion.
        """
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD")],
            _M - timedelta(minutes=1),
            _M + timedelta(minutes=1),
            _NOW + timedelta(days=1),
        )
        closes = sorted(r["close"] for r in rows)
        assert closes == [1.20, 1.25, 1.30]

    async def test_rows_are_sorted_by_open_at_then_pair(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Ordering is by candle time first so a fold walks the series forward."""
        rows = await repository.get_pnl_fx_rate_candles(
            [("EUR", "USD")],
            _M - timedelta(minutes=1),
            _M,
            _NOW + timedelta(days=1),
        )
        assert [r["open_at"] for r in rows] == sorted(r["open_at"] for r in rows)
