"""Tests for InstrumentSpec model, async repository methods, and sync helpers."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture()
def db_session() -> Session:
    """Create an in-memory SQLite database with all tables."""
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture()
async def repo() -> SQLAlchemyRepository:
    """Create an in-memory async SQLite repository with tables."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


def _ts(offset_hours: int = 0) -> datetime:
    """Return a deterministic UTC timestamp with optional hour offset."""
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=offset_hours)


async def _seed_instrument(
    repo: SQLAlchemyRepository,
    native_symbol: str,
    exchange: str,
    ts: datetime,
) -> str:
    """Insert a Symbol + Instrument and return the instrument public_id."""
    async with repo.session() as s:
        sym = Symbol(
            native_symbol=native_symbol,
            base=native_symbol.split("-", maxsplit=1)[0],
            quote=native_symbol.split("-")[1] if "-" in native_symbol else None,
            asset_type="crypto",
            created_at=ts,
            session_id="seed",
            sequence_id=1,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add(sym)
        await s.flush()
        inst = Instrument(
            symbol_public_id=sym.public_id,
            exchange=exchange,
            session_id="seed",
            sequence_id=1,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add(inst)
        await s.commit()
        return inst.public_id


class TestInstrumentSpecModel:
    """Tests for InstrumentSpec ORM model with expiry_at and instrument_kind."""

    def test_create_with_expiry_and_kind(self, db_session: Session) -> None:
        """Given expiry_at and instrument_kind, When inserted, Then fields persist."""
        now = datetime.now(UTC)
        expiry = now + timedelta(days=30)
        spec = InstrumentSpec(
            public_id="spec-001",
            instrument_public_id="inst-001",
            tick_size=0.01,
            lot_size=1.0,
            min_order_size=0.001,
            max_order_size=1000.0,
            cost_decimals=2,
            qty_decimals=8,
            margin_initial=0.05,
            position_limit_long=100,
            position_limit_short=50,
            status="online",
            expiry_at=expiry,
            instrument_kind="future",
            session_id="sess-1",
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(spec)
        db_session.commit()
        rows = db_session.execute(select(InstrumentSpec)).scalars().all()
        assert len(rows) == 1
        assert rows[0].expiry_at is not None
        assert rows[0].instrument_kind == "future"
        assert rows[0].tick_size == 0.01
        assert rows[0].status == "online"

    def test_nullable_expiry_and_kind(self, db_session: Session) -> None:
        """Given expiry_at=None and instrument_kind=None, When inserted, Then both None."""
        now = datetime.now(UTC)
        spec = InstrumentSpec(
            public_id="spec-002",
            instrument_public_id="inst-002",
            tick_size=0.5,
            lot_size=1.0,
            expiry_at=None,
            instrument_kind=None,
            session_id="sess-1",
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(spec)
        db_session.commit()
        row = db_session.execute(select(InstrumentSpec)).scalars().first()
        assert row is not None
        assert row.expiry_at is None
        assert row.instrument_kind is None

    def test_expiry_preserves_timezone(self, db_session: Session) -> None:
        """Given timezone-aware expiry_at, When read back, Then tzinfo preserved."""
        now = datetime.now(UTC)
        expiry = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)
        spec = InstrumentSpec(
            public_id="spec-003",
            instrument_public_id="inst-003",
            expiry_at=expiry,
            instrument_kind="future",
            session_id="sess-1",
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(spec)
        db_session.commit()
        row = db_session.execute(select(InstrumentSpec)).scalars().first()
        assert row is not None
        assert row.expiry_at is not None
        assert row.expiry_at.tzinfo is not None


class TestGetInstrumentSpec:
    """Tests for async get_instrument_spec repository method."""

    @pytest.mark.asyncio
    async def test_returns_spec_with_new_fields(self, repo: SQLAlchemyRepository) -> None:
        """Given spec with expiry_at and instrument_kind, When queried, Then fields match."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts)
        expiry = ts + timedelta(days=90)
        spec = InstrumentSpecInput(
            tick_size=0.01,
            lot_size=1.0,
            expiry_at=expiry,
            instrument_kind="future",
        )
        await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            spec=spec,
        )
        row = await repo.get_instrument_spec(ipid, ts + timedelta(seconds=1))
        assert row is not None
        assert row["instrument_public_id"] == ipid
        assert row["tick_size"] == 0.01
        assert row["lot_size"] == 1.0
        assert row["expiry_at"] == expiry
        assert row["instrument_kind"] == "future"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_spec(self, repo: SQLAlchemyRepository) -> None:
        """Given instrument with no spec, When queried, Then returns None."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "ETH-USD", "kraken", ts)
        row = await repo.get_instrument_spec(ipid, ts + timedelta(seconds=1))
        assert row is None

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_instrument(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Given nonexistent instrument_public_id, When queried, Then returns None."""
        row = await repo.get_instrument_spec("nonexistent-id", _ts())
        assert row is None


class TestReviseInstrumentSpecWithExpiry:
    """Tests for async revise_instrument_spec with expiry_at and instrument_kind."""

    @pytest.mark.asyncio
    async def test_create_with_expiry(self, repo: SQLAlchemyRepository) -> None:
        """Given no existing spec, When revising with expiry_at, Then new row id > 0."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts)
        spec = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        row_id = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            spec=spec,
        )
        assert row_id > 0

    @pytest.mark.asyncio
    async def test_unchanged_when_same(self, repo: SQLAlchemyRepository) -> None:
        """Given existing spec, When revising with identical payload, Then same id returned."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts)
        spec = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        id1 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            spec=spec,
        )
        id2 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
            spec=spec,
        )
        assert id2 == id1

    @pytest.mark.asyncio
    async def test_update_when_expiry_changes(self, repo: SQLAlchemyRepository) -> None:
        """Given existing spec, When revising with different expiry_at, Then new id created."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts)
        spec1 = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        id1 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            spec=spec1,
        )
        spec2 = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=60),
            instrument_kind="future",
        )
        id2 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
            spec=spec2,
        )
        assert id2 != id1

    @pytest.mark.asyncio
    async def test_update_when_kind_changes(self, repo: SQLAlchemyRepository) -> None:
        """Given spec with kind='future', When revising with kind='perpetual', Then new id."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts)
        spec1 = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        id1 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            spec=spec1,
        )
        spec2 = InstrumentSpecInput(
            tick_size=0.01,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="perpetual",
        )
        id2 = await repo.revise_instrument_spec(
            instrument_public_id=ipid,
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
            spec=spec2,
        )
        assert id2 != id1


@pytest.fixture()
def sync_repo() -> tuple[DatabaseRepository, str]:
    """Create a sync DatabaseRepository with tables and a seeded instrument.

    Returns:
        Tuple of (DatabaseRepository, instrument_public_id).
    """
    repo = DatabaseRepository("sqlite:///:memory:")
    repo.create_all()
    ts = _ts()
    with repo.get_session() as session:
        sym = Symbol(
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            created_at=ts,
            session_id="seed",
            sequence_id=1,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(sym)
        session.flush()
        inst = Instrument(
            symbol_public_id=sym.public_id,
            exchange="kraken",
            session_id="seed",
            sequence_id=1,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(inst)
        session.commit()
        ipid = inst.public_id
    return repo, ipid


class TestSyncHelpers:
    """Tests for DatabaseRepository sync helpers: get/revise_instrument_spec_sync."""

    def test_get_instrument_spec_sync_returns_none(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given no spec for instrument, When get_instrument_spec_sync, Then None."""
        repo, ipid = sync_repo
        with repo.get_session() as session:
            result = DatabaseRepository.get_instrument_spec_sync(session, ipid, _ts(1))
        assert result is None

    def test_revise_and_get_sync(self, sync_repo: tuple[DatabaseRepository, str]) -> None:
        """Given spec created via sync revise, When get_sync called, Then ORM row returned."""
        repo, ipid = sync_repo
        ts = _ts()
        expiry = ts + timedelta(days=30)
        spec = InstrumentSpecInput(
            tick_size=0.5,
            lot_size=1.0,
            expiry_at=expiry,
            instrument_kind="future",
        )
        with repo.get_session() as session:
            DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=1,
                timestamp=ts,
                spec=spec,
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts + timedelta(seconds=1)
            )
        assert row is not None
        assert row.instrument_public_id == ipid
        assert row.tick_size == 0.5
        assert row.lot_size == 1.0
        assert row.expiry_at is not None
        assert row.instrument_kind == "future"

    def test_revise_sync_unchanged(self, sync_repo: tuple[DatabaseRepository, str]) -> None:
        """Given existing spec, When revise_sync with same payload, Then 'unchanged'."""
        repo, ipid = sync_repo
        ts = _ts()
        spec = InstrumentSpecInput(
            tick_size=0.5,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        with repo.get_session() as session:
            DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=1,
                timestamp=ts,
                spec=spec,
            )
            session.commit()
        with repo.get_session() as session:
            status = DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=2,
                timestamp=ts + timedelta(seconds=1),
                spec=spec,
            )
            session.commit()
        assert status == "unchanged"

    def test_revise_sync_updated(self, sync_repo: tuple[DatabaseRepository, str]) -> None:
        """Given existing spec, When revise_sync with changed payload, Then 'updated'."""
        repo, ipid = sync_repo
        ts = _ts()
        spec1 = InstrumentSpecInput(
            tick_size=0.5,
            expiry_at=ts + timedelta(days=30),
            instrument_kind="future",
        )
        with repo.get_session() as session:
            DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=1,
                timestamp=ts,
                spec=spec1,
            )
            session.commit()
        spec2 = InstrumentSpecInput(
            tick_size=0.5,
            expiry_at=ts + timedelta(days=60),
            instrument_kind="perpetual",
        )
        with repo.get_session() as session:
            status = DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=2,
                timestamp=ts + timedelta(seconds=1),
                spec=spec2,
            )
            session.commit()
        assert status == "updated"

    def test_revise_sync_created(self, sync_repo: tuple[DatabaseRepository, str]) -> None:
        """Given no existing spec, When revise_sync called, Then 'created'."""
        repo, ipid = sync_repo
        ts = _ts()
        spec = InstrumentSpecInput(
            tick_size=0.25,
            expiry_at=ts + timedelta(days=90),
            instrument_kind="future",
        )
        with repo.get_session() as session:
            status = DatabaseRepository.revise_instrument_spec_sync(
                session=session,
                instrument_public_id=ipid,
                session_id="s1",
                sequence_id=1,
                timestamp=ts,
                spec=spec,
            )
            session.commit()
        assert status == "created"
