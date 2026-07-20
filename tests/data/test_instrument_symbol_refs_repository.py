"""Tests for P&L timeline repository reads (Phase 5A).

Pins canonical source-venue symbol resolution, batched finalized candle ranges,
and exact wallet/mode fill-shard discovery used by the timeline service.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event as sa_event

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000901"
_SPID_USD = "00000000-0000-7000-8000-000000000a01"
_SPID_EUR = "00000000-0000-7000-8000-000000000a02"
_SPID_EQ = "00000000-0000-7000-8000-000000000a03"
_INST_USD = "00000000-0000-7000-8000-000000000b01"
_INST_EUR = "00000000-0000-7000-8000-000000000b02"
_INST_EQ = "00000000-0000-7000-8000-000000000b03"
_INST_PAPER = "00000000-0000-7000-8000-000000000b04"
_WALLET = "00000000-0000-7000-8000-000000000c01"
_FOREIGN_WALLET = "00000000-0000-7000-8000-000000000c02"


def _symbol(public_id: str, native_symbol: str, quote: str | None, asset_type: str) -> Symbol:
    """Build one active symbol row."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base=native_symbol.split("-", maxsplit=1)[0],
        quote=quote,
        asset_type=asset_type,
        created_at=_NOW,
        timestamp=_NOW,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    source_exchange: str | None = None,
) -> Instrument:
    """Build one active instrument row linked to a symbol."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange=source_exchange,
        requires_ai_review=False,
        timestamp=_NOW,
        session_id=_SESSION,
        sequence_id=1,
    )


def _candle(
    instrument_public_id: str,
    open_at: datetime,
    close: float,
    timeframe: str = "1m",
    complete: bool = True,
) -> Candle:
    """Build one active candle row for range-read tests."""
    return Candle(
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timeframe=timeframe,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1.0,
        vwap=close,
        trades=1,
        source="native",
        complete=complete,
        timestamp=_NOW,
        session_id=_SESSION,
        sequence_id=1,
    )


def _venue_event(
    shard_key: str,
    wallet_public_id: str,
    mode: str,
    event_type: str = "fill_observed",
) -> VenueEvent:
    """Build one append-only venue event for scoped shard discovery."""
    return VenueEvent(
        event_type=event_type,
        shard_key=shard_key,
        wallet_public_id=wallet_public_id,
        exchange="kraken",
        instrument="BTC-USD",
        mode=mode,
        received_at=_NOW,
        liquidity_role="taker",
        timestamp=_NOW,
        session_id=_SESSION,
        sequence_id=1,
    )


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with symbols and instruments."""
    db_path = tmp_path / "instrument-refs.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Candle.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                _symbol(_SPID_USD, "BTC-USD", "USD", "crypto"),
                _symbol(_SPID_EUR, "BTC-EUR", "EUR", "crypto"),
                _symbol(_SPID_EQ, "AAPL", None, "equity"),
                _instrument(_INST_USD, _SPID_USD, "kraken"),
                _instrument(_INST_EUR, _SPID_EUR, "kraken"),
                _instrument(_INST_EQ, _SPID_EQ, "kraken_equities"),
                _instrument(_INST_PAPER, _SPID_USD, "paper", "kraken"),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


class TestGetInstrumentSymbolRefs:
    """Cover the symbol-reference resolution read."""

    async def test_empty_ids_short_circuit(self, repository: SQLAlchemyRepository) -> None:
        """An empty id sequence returns an empty list without a query."""
        rows = await repository.get_instrument_symbol_refs([], _NOW)
        assert rows == []

    async def test_resolves_native_symbol_exchange_and_quote(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Each live instrument resolves to its native/candle-venue/quote triple."""
        rows = await repository.get_instrument_symbol_refs([_INST_USD, _INST_EUR], _NOW)
        by_id = {r["instrument_public_id"]: r for r in rows}
        assert by_id[_INST_USD]["native_symbol"] == "BTC-USD"
        assert by_id[_INST_USD]["exchange"] == "kraken"
        assert by_id[_INST_USD]["quote_currency"] == "USD"
        assert by_id[_INST_EUR]["quote_currency"] == "EUR"

    async def test_paper_instrument_projects_source_exchange(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A PAPER identity resolves marks from its canonical source venue."""
        rows = await repository.get_instrument_symbol_refs([_INST_PAPER], _NOW)
        assert rows == [
            {
                "instrument_public_id": _INST_PAPER,
                "native_symbol": "BTC-USD",
                "exchange": "kraken",
                "quote_currency": "USD",
            }
        ]

    async def test_null_quote_for_non_quoted_asset(self, repository: SQLAlchemyRepository) -> None:
        """An equity symbol has a null quote currency (no direct USD mark)."""
        rows = await repository.get_instrument_symbol_refs([_INST_EQ], _NOW)
        assert len(rows) == 1
        assert rows[0]["native_symbol"] == "AAPL"
        assert rows[0]["quote_currency"] is None

    async def test_unknown_instrument_is_omitted(self, repository: SQLAlchemyRepository) -> None:
        """An unknown instrument id yields no row rather than a null-symbol row."""
        rows = await repository.get_instrument_symbol_refs(
            ["00000000-0000-7000-8000-0000000000ff"], _NOW
        )
        assert rows == []


class TestGetPnlTimelineCandles:
    """Cover the single-query finalized candle range projection."""

    async def test_empty_refs_short_circuit(self, repository: SQLAlchemyRepository) -> None:
        """An empty reference sequence returns without opening a query."""
        rows = await repository.get_pnl_timeline_candles([], _NOW, _NOW, _NOW)
        assert rows == []

    async def test_batches_source_series_and_preserves_requested_ids(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """One bounded query maps source candles onto live and PAPER identities."""
        start = _NOW.replace(minute=58) - timedelta(hours=1)
        end = start + timedelta(minutes=1)
        async with repository.session() as session:
            session.add_all(
                [
                    _candle(_INST_USD, start - timedelta(minutes=1), 90.0),
                    _candle(_INST_USD, start, 100.0),
                    _candle(_INST_USD, end, 101.0),
                    _candle(_INST_USD, start + timedelta(seconds=30), 102.0, complete=False),
                    _candle(_INST_USD, start + timedelta(seconds=45), 103.0, timeframe="5m"),
                    _candle(_INST_EUR, start, 200.0),
                ]
            )
            await session.commit()
        refs = await repository.get_instrument_symbol_refs(
            [_INST_USD, _INST_PAPER, _INST_EUR], _NOW
        )
        statements: list[str] = []

        def _capture(
            connection: object,
            cursor: object,
            statement: str,
            parameters: object,
            context: object,
            executemany: bool,
        ) -> None:
            """Capture SQL emitted by the batched range read."""
            statements.append(statement)

        sa_event.listen(repository.engine.sync_engine, "before_cursor_execute", _capture)
        try:
            rows = await repository.get_pnl_timeline_candles(refs, start, end, _NOW)
        finally:
            sa_event.remove(repository.engine.sync_engine, "before_cursor_execute", _capture)
        assert len(statements) == 1
        assert {(row["instrument_public_id"], row["open_at"], row["close"]) for row in rows} == {
            (_INST_USD, start, 100.0),
            (_INST_USD, end, 101.0),
            (_INST_PAPER, start, 100.0),
            (_INST_PAPER, end, 101.0),
            (_INST_EUR, start, 200.0),
        }
        assert [row["open_at"] for row in rows] == sorted(row["open_at"] for row in rows)
        assert all(set(row) == {"instrument_public_id", "open_at", "close"} for row in rows)

    async def test_returns_candle_version_known_at_as_of(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A correction learned after the horizon cannot rewrite an as-of mark."""
        open_at = _NOW - timedelta(minutes=1)
        correction_at = _NOW + timedelta(hours=2)
        horizon = _NOW + timedelta(hours=1)
        candle_public_id = "00000000-0000-7000-8000-000000000d01"
        original = _candle(_INST_USD, open_at, 100.0)
        original.public_id = candle_public_id
        original.known_to = correction_at
        corrected = _candle(_INST_USD, open_at, 125.0)
        corrected.public_id = candle_public_id
        corrected.timestamp = correction_at
        corrected.known_to = KNOWN_TO_MAX
        corrected.sequence_id = 2
        async with repository.session() as session:
            session.add_all([original, corrected])
            await session.commit()
        refs = await repository.get_instrument_symbol_refs([_INST_USD], horizon)
        rows = await repository.get_pnl_timeline_candles(refs, open_at, open_at, horizon)
        assert [(row["open_at"], row["close"]) for row in rows] == [(open_at, 100.0)]


class TestGetFillShardKeysForScope:
    """Cover exact wallet/mode fill-evidence discovery."""

    async def test_filters_exact_scope_event_type_and_duplicates(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Only distinct fill shards for the requested full scope are returned."""
        async with repository.session() as session:
            session.add_all(
                [
                    _venue_event("shard-b", _WALLET, "live"),
                    _venue_event("shard-a", _WALLET, "live"),
                    _venue_event("shard-b", _WALLET, "live"),
                    _venue_event("paper-shard", _WALLET, "paper"),
                    _venue_event("foreign-shard", _FOREIGN_WALLET, "live"),
                    _venue_event("accepted-shard", _WALLET, "live", "order_accepted"),
                ]
            )
            await session.commit()
        rows = await repository.get_fill_shard_keys_for_scope(_WALLET, "live", _NOW)
        assert rows == ["shard-a", "shard-b"]
