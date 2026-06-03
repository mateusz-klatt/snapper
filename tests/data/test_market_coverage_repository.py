"""Tests for :meth:`SQLAlchemyRepository.get_market_data_coverage`.

Pins the per-exchange coverage contract over ACTIVE (current
bitemporal) instruments:

* ``instruments`` = count of active instruments for the exchange.
* ``fresh_ticks`` = count having >=1 ``ticks`` row newer than
  ``now - tick_window``.
* ``fresh_candles`` = count having >=1 ``candles`` row whose ``open_at``
  is newer than ``now - candle_window``.
* ``gated_off`` = count whose current ``can_market_data`` is FALSE.
* ``dark`` = count NOT gated off AND WITHOUT any fresh ticks.

A fixed reference ``now`` is injected into every call so the cutoff
boundary assertions are deterministic regardless of wall-clock drift.
Exercises the strict ``>`` window boundary (a tick exactly AT the cutoff
is stale; one second newer is fresh), closed (non-current) instruments
and capabilities being excluded, and that overlapping active capability
rows do not fan out the per-exchange counts.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.api.schemas.market_coverage import MarketDataCoverageExchange
from snapper.api.schemas.market_coverage import MarketDataCoveragePayload
from snapper.api.schemas.market_coverage import MarketDataCoverageResponse
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import Tick
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
_PAST = _NOW - timedelta(hours=1)
_TICK_WINDOW = 600
_CANDLE_WINDOW = 1800


@pytest.fixture
async def _repo() -> SQLAlchemyRepository:
    """Async fixture yielding a fresh in-memory aiosqlite repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    return repo


async def _coverage(repo: SQLAlchemyRepository) -> list[dict[str, object]]:
    """Run the coverage query at the fixed reference ``_NOW``."""
    rows = await repo.get_market_data_coverage(
        tick_window_seconds=_TICK_WINDOW,
        candle_window_seconds=_CANDLE_WINDOW,
        now=_NOW,
    )
    return [dict(row) for row in rows]


async def _add_instrument(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    known_to: datetime = KNOWN_TO_MAX,
) -> None:
    """Insert one instrument row (active unless ``known_to`` is closed)."""
    async with repo.session() as s:
        s.add(
            Instrument(
                public_id=public_id,
                symbol_public_id=symbol_public_id,
                exchange=exchange,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
                known_to=known_to,
            )
        )
        await s.commit()


async def _add_capability(
    repo: SQLAlchemyRepository,
    *,
    symbol_public_id: str,
    exchange: str,
    can_market_data: bool,
    known_to: datetime = KNOWN_TO_MAX,
    public_id: str | None = None,
) -> None:
    """Insert one symbol_exchange_capabilities row for the symbol+exchange."""
    pid = public_id if public_id is not None else f"cap-{symbol_public_id}-{exchange}"
    async with repo.session() as s:
        s.add(
            SymbolExchangeCapability(
                public_id=pid,
                symbol_public_id=symbol_public_id,
                exchange=exchange,
                can_market_data=can_market_data,
                can_trade=False,
                source="test",
                reason=None,
                created_at=_PAST,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
                known_to=known_to,
            )
        )
        await s.commit()


async def _add_tick(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    instrument_public_id: str,
    at: datetime,
) -> None:
    """Insert one tick row at bus-time ``at`` for the instrument."""
    async with repo.session() as s:
        s.add(
            Tick(
                public_id=public_id,
                instrument_public_id=instrument_public_id,
                bid=1.0,
                ask=1.1,
                last=1.05,
                volume=10.0,
                session_id="s-1",
                sequence_id=1,
                timestamp=at,
            )
        )
        await s.commit()


async def _add_candle(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    instrument_public_id: str,
    open_at: datetime,
) -> None:
    """Insert one 1m candle row opening at ``open_at`` for the instrument."""
    async with repo.session() as s:
        s.add(
            Candle(
                public_id=public_id,
                instrument_public_id=instrument_public_id,
                open_at=open_at,
                timeframe="1m",
                open=1.0,
                high=1.2,
                low=0.9,
                close=1.1,
                volume=5.0,
                vwap=None,
                trades=None,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
            )
        )
        await s.commit()


class TestSchemaInstantiation:
    """The coverage schemas accept the documented shapes."""

    def test_exchange_row_roundtrips(self) -> None:
        """A coverage row builds with all six integer/string fields."""
        row = MarketDataCoverageExchange(
            exchange="kraken",
            instruments=3,
            fresh_ticks=2,
            fresh_candles=1,
            gated_off=1,
            dark=0,
        )
        assert row.exchange == "kraken"
        assert row.dark == 0

    def test_response_envelope_has_literal_type(self) -> None:
        """The response wrapper defaults ``type`` to the literal discriminator."""
        payload = MarketDataCoveragePayload(
            exchanges=[],
            tick_window_seconds=600,
            candle_window_seconds=1800,
        )
        response = MarketDataCoverageResponse(
            session_id="sess",
            sequence_id=1,
            public_id="pid",
            timestamp=_NOW,
            payload=payload,
        )
        assert response.type == "market_data_coverage"
        assert response.payload.tick_window_seconds == 600


class TestGetMarketDataCoverage:
    """``get_market_data_coverage`` aggregates per exchange."""

    @pytest.mark.asyncio
    async def test_empty_db_returns_no_rows(self, _repo: SQLAlchemyRepository) -> None:
        """A DB with no instruments yields an empty coverage list."""
        assert await _coverage(_repo) == []

    @pytest.mark.asyncio
    async def test_default_now_used_when_omitted(self, _repo: SQLAlchemyRepository) -> None:
        """Omitting ``now`` falls back to wall-clock and still runs cleanly."""
        rows = await _repo.get_market_data_coverage(
            tick_window_seconds=_TICK_WINDOW, candle_window_seconds=_CANDLE_WINDOW
        )
        assert rows == []

    @pytest.mark.asyncio
    async def test_counts_and_dark_and_gated(self, _repo: SQLAlchemyRepository) -> None:
        """Full coverage across fresh / dark / gated / no-capability cases."""
        await _add_instrument(
            _repo, public_id="i-live", symbol_public_id="sym-1", exchange="kraken"
        )
        await _add_instrument(
            _repo, public_id="i-dark", symbol_public_id="sym-2", exchange="kraken"
        )
        await _add_instrument(
            _repo, public_id="i-gated", symbol_public_id="sym-3", exchange="kraken"
        )
        await _add_instrument(
            _repo, public_id="i-nocap", symbol_public_id="sym-4", exchange="kraken"
        )
        await _add_capability(
            _repo, symbol_public_id="sym-1", exchange="kraken", can_market_data=True
        )
        await _add_capability(
            _repo, symbol_public_id="sym-2", exchange="kraken", can_market_data=True
        )
        await _add_capability(
            _repo, symbol_public_id="sym-3", exchange="kraken", can_market_data=False
        )
        await _add_tick(
            _repo,
            public_id="t-live",
            instrument_public_id="i-live",
            at=_NOW - timedelta(seconds=10),
        )
        await _add_candle(
            _repo,
            public_id="c-live",
            instrument_public_id="i-live",
            open_at=_NOW - timedelta(seconds=10),
        )
        rows = await _coverage(_repo)
        assert len(rows) == 1
        row = rows[0]
        assert row["exchange"] == "kraken"
        assert row["instruments"] == 4
        assert row["fresh_ticks"] == 1
        assert row["fresh_candles"] == 1
        assert row["gated_off"] == 1
        assert row["dark"] == 2

    @pytest.mark.asyncio
    async def test_multiple_exchanges_ordered(self, _repo: SQLAlchemyRepository) -> None:
        """Rows are grouped per exchange and ordered by exchange."""
        await _add_instrument(_repo, public_id="i-b", symbol_public_id="sym-b", exchange="kraken")
        await _add_instrument(_repo, public_id="i-a", symbol_public_id="sym-a", exchange="binance")
        rows = await _coverage(_repo)
        assert [row["exchange"] for row in rows] == ["binance", "kraken"]
        assert all(row["instruments"] == 1 for row in rows)

    @pytest.mark.asyncio
    async def test_tick_window_boundary_is_strict(self, _repo: SQLAlchemyRepository) -> None:
        """A tick exactly AT the cutoff is stale; one second newer is fresh."""
        await _add_instrument(_repo, public_id="i-at", symbol_public_id="sym-at", exchange="kraken")
        await _add_instrument(
            _repo, public_id="i-after", symbol_public_id="sym-after", exchange="kraken"
        )
        cutoff = _NOW - timedelta(seconds=_TICK_WINDOW)
        await _add_tick(_repo, public_id="t-at", instrument_public_id="i-at", at=cutoff)
        await _add_tick(
            _repo,
            public_id="t-after",
            instrument_public_id="i-after",
            at=cutoff + timedelta(seconds=1),
        )
        rows = await _coverage(_repo)
        assert rows[0]["instruments"] == 2
        assert rows[0]["fresh_ticks"] == 1
        assert rows[0]["dark"] == 1

    @pytest.mark.asyncio
    async def test_candle_window_boundary_is_strict(self, _repo: SQLAlchemyRepository) -> None:
        """A candle exactly AT the cutoff is stale; one second newer is fresh."""
        await _add_instrument(
            _repo, public_id="c-at", symbol_public_id="sym-cat", exchange="kraken"
        )
        await _add_instrument(
            _repo, public_id="c-after", symbol_public_id="sym-cafter", exchange="kraken"
        )
        cutoff = _NOW - timedelta(seconds=_CANDLE_WINDOW)
        await _add_candle(_repo, public_id="cd-at", instrument_public_id="c-at", open_at=cutoff)
        await _add_candle(
            _repo,
            public_id="cd-after",
            instrument_public_id="c-after",
            open_at=cutoff + timedelta(seconds=1),
        )
        rows = await _coverage(_repo)
        assert rows[0]["fresh_candles"] == 1

    @pytest.mark.asyncio
    async def test_overlapping_active_capabilities_do_not_fan_out(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Two simultaneously-active capability rows must not inflate counts.

        The sentinel-only partial unique index does not prevent a second
        active (future ``known_to``) capability row coexisting with the
        sentinel row; an EXISTS-based query keeps every tally at one per
        instrument rather than fanning out via a join.
        """
        await _add_instrument(
            _repo, public_id="i-fan", symbol_public_id="sym-fan", exchange="kraken"
        )
        await _add_capability(
            _repo, symbol_public_id="sym-fan", exchange="kraken", can_market_data=False
        )
        await _add_capability(
            _repo,
            symbol_public_id="sym-fan",
            exchange="kraken",
            can_market_data=False,
            known_to=_NOW + timedelta(days=1),
            public_id="cap-sym-fan-kraken-future",
        )
        rows = await _coverage(_repo)
        assert len(rows) == 1
        assert rows[0]["instruments"] == 1
        assert rows[0]["gated_off"] == 1
        assert rows[0]["dark"] == 0

    @pytest.mark.asyncio
    async def test_conflicting_active_capabilities_gate_off(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """A FALSE capability dominates a co-active TRUE one (EXISTS-FALSE wins)."""
        await _add_instrument(
            _repo, public_id="i-conf", symbol_public_id="sym-conf", exchange="kraken"
        )
        await _add_capability(
            _repo, symbol_public_id="sym-conf", exchange="kraken", can_market_data=True
        )
        await _add_capability(
            _repo,
            symbol_public_id="sym-conf",
            exchange="kraken",
            can_market_data=False,
            known_to=_NOW + timedelta(days=1),
            public_id="cap-sym-conf-kraken-false",
        )
        rows = await _coverage(_repo)
        assert rows[0]["instruments"] == 1
        assert rows[0]["gated_off"] == 1
        assert rows[0]["dark"] == 0

    @pytest.mark.asyncio
    async def test_closed_instrument_excluded(self, _repo: SQLAlchemyRepository) -> None:
        """A non-current (closed) instrument version is not counted."""
        await _add_instrument(
            _repo,
            public_id="i-closed",
            symbol_public_id="sym-closed",
            exchange="kraken",
            known_to=_NOW - timedelta(minutes=5),
        )
        assert await _coverage(_repo) == []

    @pytest.mark.asyncio
    async def test_closed_capability_ignored(self, _repo: SQLAlchemyRepository) -> None:
        """A closed capability row does not gate an otherwise-dark instrument."""
        await _add_instrument(
            _repo, public_id="i-stalecap", symbol_public_id="sym-sc", exchange="kraken"
        )
        await _add_capability(
            _repo,
            symbol_public_id="sym-sc",
            exchange="kraken",
            can_market_data=False,
            known_to=_NOW - timedelta(minutes=5),
        )
        rows = await _coverage(_repo)
        assert rows[0]["gated_off"] == 0
        assert rows[0]["dark"] == 1
