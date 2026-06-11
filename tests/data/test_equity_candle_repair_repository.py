"""Tests for Kraken Equities candle repair reads rebuilt from raw trades."""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Executable

from snapper.application.maintenance.equity_candle_repair import EquityCandleRepairService
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TradeUpsertRow

_START = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
_FRAGMENT_OPEN = _START
_SINGLE_OPEN = _START + timedelta(minutes=1)
_ZERO_SIZE_OPEN = _START + timedelta(minutes=2)
_BACKFILL_OPEN = _START + timedelta(minutes=3)
_T1 = _START + timedelta(minutes=10)
_T2 = _START + timedelta(minutes=11)
_REPAIR_BUS_TIME = _START + timedelta(hours=2)
_SEED_SESSION_ID = "00000000-0000-0000-0000-000000000111"
_EQUITY_INSTRUMENT_ID = "00000000-0000-0000-0000-000000000101"
_FUTURES_INSTRUMENT_ID = "00000000-0000-0000-0000-000000000202"
_EQUITY_SYMBOL_ID = "00000000-0000-0000-0000-000000000301"
_FUTURES_SYMBOL_ID = "00000000-0000-0000-0000-000000000302"


class _AsyncSyncSession:
    """Async-shaped wrapper around a synchronous SQLAlchemy SQLite session."""

    def __init__(self, session: Session) -> None:
        """Store the synchronous session used by repository methods.

        Args:
            session: Synchronous SQLAlchemy session bound to real SQLite.
        """
        self._session = session

    async def execute(self, statement: object) -> object:
        """Execute a SQLAlchemy statement against the real SQLite session.

        Args:
            statement: SQLAlchemy Core or ORM statement.

        Returns:
            Synchronous SQLAlchemy result object.
        """
        return self._session.execute(cast(Executable, statement))

    def add(self, instance: object) -> None:
        """Add an ORM instance to the synchronous session.

        Args:
            instance: ORM object to stage for commit.
        """
        self._session.add(instance)

    async def commit(self) -> None:
        """Commit the synchronous transaction."""
        self._session.commit()


class _SyncSQLiteRepository(SQLAlchemyRepository):
    """SQLAlchemyRepository using real synchronous SQLite for test execution."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """Store a synchronous session factory.

        Args:
            session_factory: Factory bound to the test SQLite engine.
        """
        self._sync_session_factory = session_factory

    @property
    def dialect_name(self) -> str:
        """Return the SQLite dialect name."""
        return "sqlite"

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield an async-shaped session wrapper around a real SQLite session.

        Yields:
            Object cast to ``AsyncSession`` for repository method compatibility.
        """
        sync_session = self._sync_session_factory()
        try:
            yield cast(AsyncSession, _AsyncSyncSession(sync_session))
        except Exception:
            sync_session.rollback()
            raise
        finally:
            sync_session.close()


@pytest.fixture()
def repo(tmp_path: Path) -> Iterator[SQLAlchemyRepository]:
    """Yield a real file-backed SQLite repository for repair read tests.

    Args:
        tmp_path: Pytest temporary directory for the SQLite database.

    Yields:
        Repository with schema created on a real SQLite database file.
    """
    db_path = tmp_path / "equity-repair.db"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    Base.metadata.create_all(engine)
    sync_session_factory = sessionmaker(engine, expire_on_commit=False)
    repository = _SyncSQLiteRepository(sync_session_factory)
    try:
        yield repository
    finally:
        engine.dispose()


async def _seed_instrument(
    repository: SQLAlchemyRepository,
    *,
    public_id: str,
    symbol_public_id: str,
    exchange: str,
) -> None:
    """Insert one instrument row used by the portable exchange EXISTS filter.

    Args:
        repository: Repository owning the SQLite session.
        public_id: Instrument public identifier.
        symbol_public_id: Symbol public identifier carried by the instrument.
        exchange: Exchange name to store.
    """
    async with repository.session() as session:
        session.add(
            Instrument(
                public_id=public_id,
                symbol_public_id=symbol_public_id,
                exchange=exchange,
                session_id=_SEED_SESSION_ID,
                sequence_id=1,
                timestamp=_START,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


async def _upsert_candle(
    repository: SQLAlchemyRepository,
    *,
    instrument_public_id: str,
    open_at: datetime,
    timestamp: datetime,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    vwap: float | None,
    trades: int | None,
    sequence_id: int,
) -> None:
    """Upsert one candle version through the production SCD2 path.

    Args:
        repository: Repository that writes the candle.
        instrument_public_id: Instrument natural-key component.
        open_at: Candle event-time bucket.
        timestamp: Candle bus-time version timestamp.
        open_price: Open price for this version.
        high: High price for this version.
        low: Low price for this version.
        close: Close price for this version.
        volume: Volume for this version.
        vwap: Optional VWAP for this version.
        trades: Optional trade count for this version.
        sequence_id: Provenance sequence identifier.
    """
    row: CandleUpsertRow = {
        "instrument_public_id": instrument_public_id,
        "open_at": open_at,
        "timestamp": timestamp,
        "timeframe": "1m",
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": vwap,
        "trades": trades,
        "session_id": _SEED_SESSION_ID,
        "sequence_id": sequence_id,
    }
    await repository.upsert_candles([row])


async def _upsert_trades(
    repository: SQLAlchemyRepository,
    rows: list[TradeUpsertRow],
) -> None:
    """Insert raw trades through the repository trade path.

    Args:
        repository: Repository that writes the trades.
        rows: Trade rows to insert.
    """
    await repository.upsert_trades(rows)


async def _count_candles(repository: SQLAlchemyRepository) -> int:
    """Count all SCD2 candle versions in the repository.

    Args:
        repository: Repository owning the SQLite session.

    Returns:
        Number of candle rows stored across current and superseded versions.
    """
    async with repository.session() as session:
        return int((await session.execute(select(func.count()).select_from(Candle))).scalar_one())


def _trade(
    *,
    instrument_public_id: str = _EQUITY_INSTRUMENT_ID,
    trade_id: str,
    executed_at: datetime,
    timestamp: datetime,
    price: float,
    size: float,
    sequence_id: int,
) -> TradeUpsertRow:
    """Build one raw trade row for repair tests.

    Args:
        instrument_public_id: Instrument that owns the trade.
        trade_id: Exchange trade identifier.
        executed_at: Exchange event time.
        timestamp: Bus-time arrival timestamp.
        price: Trade price.
        size: Trade size.
        sequence_id: Provenance sequence identifier.

    Returns:
        Trade row accepted by ``upsert_trades``.
    """
    return {
        "instrument_public_id": instrument_public_id,
        "timestamp": timestamp,
        "price": price,
        "size": size,
        "side": "buy",
        "trade_id": trade_id,
        "executed_at": executed_at,
        "session_id": _SEED_SESSION_ID,
        "sequence_id": sequence_id,
    }


async def _seed_fragmented_candle(
    repository: SQLAlchemyRepository,
    *,
    instrument_public_id: str,
    open_at: datetime,
    trades: int | None = 1,
) -> None:
    """Seed two SCD2 candle versions for one natural key.

    Args:
        repository: Repository that writes the candle versions.
        instrument_public_id: Instrument natural-key component.
        open_at: Candle event-time bucket.
        trades: Trade count value for the versions, or ``None`` for backfill.
    """
    await _upsert_candle(
        repository,
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timestamp=_T1,
        open_price=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=1.0,
        vwap=1.0,
        trades=trades,
        sequence_id=1,
    )
    await _upsert_candle(
        repository,
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timestamp=_T2,
        open_price=2.0,
        high=3.0,
        low=1.5,
        close=2.5,
        volume=2.0,
        vwap=2.0,
        trades=trades,
        sequence_id=2,
    )


async def _seed_single_candle(repository: SQLAlchemyRepository) -> None:
    """Seed one current candle version for the non-fragmented minute.

    Args:
        repository: Repository that writes the candle version.
    """
    await _upsert_candle(
        repository,
        instrument_public_id=_EQUITY_INSTRUMENT_ID,
        open_at=_SINGLE_OPEN,
        timestamp=_T1,
        open_price=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=1.0,
        vwap=10.0,
        trades=1,
        sequence_id=3,
    )


async def _seed_repository(repository: SQLAlchemyRepository) -> None:
    """Seed instruments, fragmented candles, and raw trades for repair reads.

    Args:
        repository: Repository to populate.
    """
    await _seed_instrument(
        repository,
        public_id=_EQUITY_INSTRUMENT_ID,
        symbol_public_id=_EQUITY_SYMBOL_ID,
        exchange="kraken_equities",
    )
    await _seed_instrument(
        repository,
        public_id=_FUTURES_INSTRUMENT_ID,
        symbol_public_id=_FUTURES_SYMBOL_ID,
        exchange="kraken_futures",
    )
    await _seed_fragmented_candle(
        repository,
        instrument_public_id=_EQUITY_INSTRUMENT_ID,
        open_at=_FRAGMENT_OPEN,
    )
    await _seed_single_candle(repository)
    await _seed_fragmented_candle(
        repository,
        instrument_public_id=_EQUITY_INSTRUMENT_ID,
        open_at=_ZERO_SIZE_OPEN,
    )
    await _seed_fragmented_candle(
        repository,
        instrument_public_id=_EQUITY_INSTRUMENT_ID,
        open_at=_BACKFILL_OPEN,
        trades=None,
    )
    await _seed_fragmented_candle(
        repository,
        instrument_public_id=_FUTURES_INSTRUMENT_ID,
        open_at=_FRAGMENT_OPEN,
    )
    await _upsert_trades(
        repository,
        [
            _trade(
                trade_id="late-arrival-first",
                executed_at=_FRAGMENT_OPEN + timedelta(seconds=20),
                timestamp=_START + timedelta(minutes=15),
                price=105.0,
                size=2.0,
                sequence_id=10,
            ),
            _trade(
                trade_id="event-open",
                executed_at=_FRAGMENT_OPEN + timedelta(seconds=5),
                timestamp=_START + timedelta(minutes=16),
                price=100.0,
                size=1.0,
                sequence_id=11,
            ),
            _trade(
                trade_id="event-close",
                executed_at=_FRAGMENT_OPEN + timedelta(seconds=50),
                timestamp=_START + timedelta(minutes=17),
                price=103.0,
                size=3.0,
                sequence_id=12,
            ),
            _trade(
                trade_id="single-minute",
                executed_at=_SINGLE_OPEN + timedelta(seconds=10),
                timestamp=_START + timedelta(minutes=18),
                price=999.0,
                size=4.0,
                sequence_id=13,
            ),
            _trade(
                trade_id="zero-open",
                executed_at=_ZERO_SIZE_OPEN + timedelta(seconds=1),
                timestamp=_START + timedelta(minutes=19),
                price=200.0,
                size=0.0,
                sequence_id=14,
            ),
            _trade(
                trade_id="zero-close",
                executed_at=_ZERO_SIZE_OPEN + timedelta(seconds=59),
                timestamp=_START + timedelta(minutes=20),
                price=201.0,
                size=0.0,
                sequence_id=15,
            ),
            _trade(
                trade_id="backfill-minute",
                executed_at=_BACKFILL_OPEN + timedelta(seconds=15),
                timestamp=_START + timedelta(minutes=21),
                price=300.0,
                size=1.0,
                sequence_id=16,
            ),
            _trade(
                instrument_public_id=_FUTURES_INSTRUMENT_ID,
                trade_id="futures-minute",
                executed_at=_FRAGMENT_OPEN + timedelta(seconds=10),
                timestamp=_START + timedelta(minutes=22),
                price=400.0,
                size=1.0,
                sequence_id=17,
            ),
        ],
    )


class TestEquityCandleRepairRepository:
    """Repository repair reads rebuild eligible minutes from raw trades."""

    @pytest.mark.asyncio
    async def test_rebuilds_only_fragmented_equity_minutes_from_raw_trades(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Test real SQLite repair read derives corrected bars from trades.

        Given: Kraken Equities and Futures instruments with raw trades, a
            fragmented Equities minute, a single-version Equities minute, a
            zero-size fragmented Equities minute, backfill candle versions, and
            a fragmented Futures minute,
        When: Repair rows are read for the half-open range,
        Then: Only fragmented live-synthesized Equities minutes are rebuilt,
            with open and close coming from earliest and latest event time.
        """
        await _seed_repository(repo)
        batch = await repo.get_equity_candle_repairs_from_trades(
            start=_START,
            end=_START + timedelta(hours=1),
        )
        rows = batch.repairs
        assert batch.unreconstructable_minutes == 0
        assert [row.open_at for row in rows] == [_FRAGMENT_OPEN, _ZERO_SIZE_OPEN]
        first = rows[0]
        assert first.instrument_public_id == _EQUITY_INSTRUMENT_ID
        assert first.timeframe == "1m"
        assert first.open == pytest.approx(100.0)
        assert first.close == pytest.approx(103.0)
        assert first.high == pytest.approx(105.0)
        assert first.low == pytest.approx(100.0)
        assert first.volume == pytest.approx(6.0)
        assert first.trades == 3
        assert first.vwap == pytest.approx(619.0 / 6.0)
        zero = rows[1]
        assert zero.open == pytest.approx(200.0)
        assert zero.close == pytest.approx(201.0)
        assert zero.high == pytest.approx(201.0)
        assert zero.low == pytest.approx(200.0)
        assert zero.volume == pytest.approx(0.0)
        assert zero.trades == 2
        assert zero.vwap is None

    @pytest.mark.asyncio
    async def test_empty_range_returns_no_repairs(self, repo: SQLAlchemyRepository) -> None:
        """Test empty ranges do not scan trades or return repair rows.

        Given: A real SQLite repository with no candle minutes in range,
        When: The repair read is called with ``start == end``,
        Then: It returns an empty list.
        """
        batch = await repo.get_equity_candle_repairs_from_trades(
            start=_START,
            end=_START,
        )
        assert batch.repairs == []
        assert batch.unreconstructable_minutes == 0

    @pytest.mark.asyncio
    async def test_same_executed_at_ties_resolve_by_insertion_id(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Test equal event-time trades order deterministically by row id.

        Given: A fragmented Equities minute whose two raw trades share the
            exact same ``executed_at``, with the later-inserted trade carrying
            an EARLIER bus-time ``timestamp``,
        When: Repair rows are read for the range,
        Then: Open comes from the first-inserted trade and close from the
            last-inserted trade, proving the ``(instrument, executed_at, id)``
            ordering breaks ties by insertion id rather than bus time.
        """
        await _seed_instrument(
            repo,
            public_id=_EQUITY_INSTRUMENT_ID,
            symbol_public_id=_EQUITY_SYMBOL_ID,
            exchange="kraken_equities",
        )
        await _seed_fragmented_candle(
            repo,
            instrument_public_id=_EQUITY_INSTRUMENT_ID,
            open_at=_FRAGMENT_OPEN,
        )
        tie_at = _FRAGMENT_OPEN + timedelta(seconds=30)
        await _upsert_trades(
            repo,
            [
                _trade(
                    trade_id="tie-first-inserted",
                    executed_at=tie_at,
                    timestamp=_START + timedelta(minutes=15),
                    price=100.0,
                    size=1.0,
                    sequence_id=30,
                ),
                _trade(
                    trade_id="tie-second-inserted",
                    executed_at=tie_at,
                    timestamp=_START + timedelta(minutes=14),
                    price=101.0,
                    size=2.0,
                    sequence_id=31,
                ),
            ],
        )
        batch = await repo.get_equity_candle_repairs_from_trades(
            start=_START,
            end=_START + timedelta(hours=1),
        )
        assert len(batch.repairs) == 1
        row = batch.repairs[0]
        assert row.open == pytest.approx(100.0)
        assert row.close == pytest.approx(101.0)
        assert row.high == pytest.approx(101.0)
        assert row.low == pytest.approx(100.0)
        assert row.volume == pytest.approx(3.0)
        assert row.trades == 2

    @pytest.mark.asyncio
    async def test_fragmented_minute_without_trades_is_counted_not_dropped(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Test trade-less fragmented minutes surface as unreconstructable.

        Given: A fragmented Equities minute with no persisted raw trades at
            all (for example a minute older than trade-persistence rollout),
        When: Repair rows are read for the range,
        Then: No repair rows are produced and the batch reports one
            unreconstructable minute instead of silently dropping the
            detected fragmentation.
        """
        await _seed_instrument(
            repo,
            public_id=_EQUITY_INSTRUMENT_ID,
            symbol_public_id=_EQUITY_SYMBOL_ID,
            exchange="kraken_equities",
        )
        await _seed_fragmented_candle(
            repo,
            instrument_public_id=_EQUITY_INSTRUMENT_ID,
            open_at=_FRAGMENT_OPEN,
        )
        batch = await repo.get_equity_candle_repairs_from_trades(
            start=_START,
            end=_START + timedelta(hours=1),
        )
        assert batch.repairs == []
        assert batch.unreconstructable_minutes == 1

    @pytest.mark.asyncio
    async def test_service_dry_run_and_idempotent_rerun_use_real_value_guard(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Test service dry-run and idempotency against real SQLite writes.

        Given: A seeded repository with two repairable Equities minutes,
        When: The service dry-runs, writes once, and then writes again,
        Then: Dry-run does not add candles and the second write returns zero
            because the active corrected rows already match.
        """
        await _seed_repository(repo)
        service = EquityCandleRepairService(repo)
        before = await _count_candles(repo)
        dry_run = await service.repair(
            start=_START,
            end=_START + timedelta(hours=1),
            repair_bus_time=_REPAIR_BUS_TIME,
            dry_run=True,
            batch_size=1,
            chunk_size=timedelta(minutes=30),
        )
        after_dry_run = await _count_candles(repo)
        first_write = await service.repair(
            start=_START,
            end=_START + timedelta(hours=1),
            repair_bus_time=_REPAIR_BUS_TIME,
            dry_run=False,
            batch_size=1,
            chunk_size=timedelta(minutes=30),
        )
        after_first_write = await _count_candles(repo)
        second_write = await service.repair(
            start=_START,
            end=_START + timedelta(hours=1),
            repair_bus_time=_REPAIR_BUS_TIME + timedelta(minutes=1),
            dry_run=False,
            batch_size=1,
            chunk_size=timedelta(minutes=30),
        )
        after_second_write = await _count_candles(repo)
        assert dry_run.rows_rewritten == 2
        assert after_dry_run == before
        assert first_write.rows_rewritten == 2
        assert after_first_write == before + 2
        assert second_write.fragmented_minutes_found == 2
        assert second_write.rows_rewritten == 0
        assert after_second_write == after_first_write
