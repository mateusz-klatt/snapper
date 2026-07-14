"""Tests for InstrumentSpec model, async repository methods, and sync helpers."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.base import PRESERVE_EXISTING
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import ExactDecimalNumeric
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import is_effective_unit_certified
from snapper.data.repository_types import InstrumentSpecRow


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


def _metadata(
    observed_at: datetime,
    unit_certified: bool = True,
    spec_source: str | None = "kraken_futures:rest.get_instruments",
    spec_version: str | None = "s2a-v1:test",
) -> InstrumentMetadataInput:
    """Build complete futures metadata for repository and updater tests."""
    return InstrumentMetadataInput(
        tick_size=0.25,
        lot_size=3.0,
        min_order_size=3.0,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=0,
        margin_initial=0.02,
        position_limit_long=100,
        position_limit_short=100,
        status="active",
        contract_size=Decimal("3.000000000000000001"),
        quantity_unit="contract_count",
        spec_source=spec_source,
        spec_version=spec_version,
        spec_observed_at=observed_at,
        unit_certified=unit_certified,
    )


def _certified_row(observed_at: datetime | None, stored: bool = True) -> InstrumentSpecRow:
    """Build a complete typed repository row for freshness tests."""
    return InstrumentSpecRow(
        instrument_public_id="inst-1",
        tick_size=0.25,
        lot_size=3.0,
        min_order_size=3.0,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=0,
        margin_initial=0.02,
        position_limit_long=100,
        position_limit_short=100,
        status="active",
        contract_size=Decimal("3"),
        quantity_unit="contract_count",
        spec_source="kraken_futures:rest.get_instruments",
        spec_version="s2a-v1:test",
        spec_observed_at=observed_at,
        unit_certified=stored,
        expiry_at=None,
        instrument_kind="perpetual",
        funding_type="perpetual_funding",
        funding_frequency_hours=1,
        rollover_rate_long=None,
        rollover_rate_short=None,
        max_funding_rate=0.0025,
    )


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

    def test_exact_sqlite_decimal_codec_still_enforces_positive_check(
        self, db_session: Session
    ) -> None:
        """Exact SQLite text storage cannot bypass the positive contract CHECK."""
        now = datetime.now(UTC)
        db_session.add(
            InstrumentSpec(
                public_id="spec-negative",
                instrument_public_id="inst-negative",
                contract_size=Decimal("-0.000000000000000001"),
                session_id="session-negative",
                sequence_id=1,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
        )
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestExactDecimalNumeric:
    """Tests for dialect-specific exact Decimal processors."""

    def test_postgresql_processors_preserve_and_normalize_decimals(self) -> None:
        """PostgreSQL processors preserve native Decimals and normalize legacy values."""
        numeric = ExactDecimalNumeric(38, 18)
        dialect = postgresql_dialect()
        native = Decimal("1.500000000000000001")

        bind_processor = numeric.bind_processor(dialect)
        assert bind_processor is numeric._native_bind
        assert bind_processor(native) is native

        result_processor = numeric.result_processor(dialect, None)
        assert result_processor is numeric._native_result
        assert result_processor(None) is None
        assert result_processor(native) is native
        assert result_processor("1.5") == Decimal("1.5")

    def test_sqlite_result_processor_decodes_legacy_numeric_value(self) -> None:
        """SQLite result processing accepts legacy values without the exact-text suffix."""
        assert ExactDecimalNumeric._sqlite_result(1.5) == Decimal("1.5")


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
            contract_size=Decimal("7.125000000000000001"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:test",
            spec_observed_at=ts,
            unit_certified=True,
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
        assert row["contract_size"] == Decimal("7.125000000000000001")
        assert row["quantity_unit"] == "contract_count"
        assert row["spec_source"] == "kraken_futures:rest.get_instruments"
        assert row["spec_version"] == "s2a-v1:test"
        assert row["spec_observed_at"] == ts
        assert row["unit_certified"] is True
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
            contract_size=Decimal("2.5"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:unchanged",
            spec_observed_at=ts,
            unit_certified=True,
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
            lot_size=2.5,
            min_order_size=2.5,
            contract_size=Decimal("2.5"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:no-op",
            spec_observed_at=ts,
            unit_certified=True,
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
            contract_size=Decimal("2.5"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:sync",
            spec_observed_at=ts,
            unit_certified=True,
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
        assert row.contract_size == Decimal("2.5")
        assert row.quantity_unit == "contract_count"
        assert row.spec_source == "kraken_futures:rest.get_instruments"
        assert row.spec_version == "s2a-v1:sync"
        assert row.spec_observed_at == ts
        assert row.unit_certified is True
        assert row.expiry_at is not None
        assert row.instrument_kind == "future"

    def test_revise_sync_unchanged(self, sync_repo: tuple[DatabaseRepository, str]) -> None:
        """Given existing spec, When revise_sync with same payload, Then 'unchanged'."""
        repo, ipid = sync_repo
        ts = _ts()
        spec = InstrumentSpecInput(
            tick_size=0.5,
            lot_size=2.5,
            min_order_size=2.5,
            contract_size=Decimal("2.5"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:no-op-sync",
            spec_observed_at=ts,
            unit_certified=True,
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


class TestUpdaterReviseHelper:
    """Tests for SymbolUpdaterService._revise_instrument_spec carry-forward.

    The helper exists at
    ``snapper.application.updaters.symbols.base.SymbolUpdaterService``
    and merges three patterns onto the InstrumentSpec row: pure
    carry-forward (tick_size, lot_size, ...), direct assignment
    (expiry_at, instrument_kind), and conditional carry-forward (the
    funding fields). The conditional pattern is exercised by the funding
    fee model: one updater seeds rollover rates and a later refresh
    must NOT clear them.
    """

    def test_seeds_funding_fields_when_absent(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given no spec, When called with funding fields, Then they are persisted."""
        repo, ipid = sync_repo
        ts = _ts()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts,
                session_id="s1",
                sequence_id=1,
                funding_type="spot_margin_rollover",
                funding_frequency_hours=4,
                rollover_rate_long=0.00025,
                rollover_rate_short=0.00010,
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts + timedelta(seconds=1)
            )
        assert row is not None
        assert row.funding_type == "spot_margin_rollover"
        assert row.funding_frequency_hours == 4
        assert row.rollover_rate_long == pytest.approx(0.00025)
        assert row.rollover_rate_short == pytest.approx(0.00010)

    def test_conditional_carry_forward_preserves_funding_fields(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given seeded funding fields, When called WITHOUT them, Then they survive.

        This is the regression coverage for the conditional carry-forward
        pattern: a later updater (e.g. an equities refresh) that does not
        know about funding fields must not erase them on the next SCD2
        revision. The helper carries forward whichever value the existing
        row had whenever the parameter is None.
        """
        repo, ipid = sync_repo
        ts1 = _ts()
        ts2 = ts1 + timedelta(hours=1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts1,
                session_id="s1",
                sequence_id=1,
                funding_type="perpetual_funding",
                funding_frequency_hours=1,
                max_funding_rate=0.0025,
            )
            session.commit()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts2,
                session_id="s1",
                sequence_id=2,
                instrument_kind="perpetual",
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts2 + timedelta(seconds=1)
            )
        assert row is not None
        assert row.funding_type == "perpetual_funding"
        assert row.funding_frequency_hours == 1
        assert row.max_funding_rate == pytest.approx(0.0025)
        assert row.instrument_kind == "perpetual"

    def test_explicit_value_overrides_existing_funding_field(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given a seeded rollover rate, When refreshed with a new rate, Then updated.

        Verifies the conditional carry-forward correctly prefers the
        provided value over the existing one when both are present.
        """
        repo, ipid = sync_repo
        ts1 = _ts()
        ts2 = ts1 + timedelta(hours=1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts1,
                session_id="s1",
                sequence_id=1,
                funding_type="spot_margin_rollover",
                rollover_rate_long=0.00025,
            )
            session.commit()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts2,
                session_id="s1",
                sequence_id=2,
                funding_type="spot_margin_rollover",
                rollover_rate_long=0.00050,
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts2 + timedelta(seconds=1)
            )
        assert row is not None
        assert row.rollover_rate_long == pytest.approx(0.00050)

    def test_no_existing_no_funding_passed_creates_null_row(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given no existing spec, When called with no funding fields, Then NULLs.

        Backward-compatibility check: the helper still works for
        updaters that have no concept of funding (e.g. Polygon equities)
        and produces NULL columns instead of crashing.
        """
        repo, ipid = sync_repo
        ts = _ts()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts,
                session_id="s1",
                sequence_id=1,
                instrument_kind="spot",
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts + timedelta(seconds=1)
            )
        assert row is not None
        assert row.instrument_kind == "spot"
        assert row.funding_type is None
        assert row.funding_frequency_hours is None
        assert row.rollover_rate_long is None
        assert row.rollover_rate_short is None
        assert row.max_funding_rate is None

    def test_explicit_none_clears_existing_funding_fields(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """Given seeded funding fields, When called with explicit None, Then cleared.

        Verifies the sentinel protocol: ``PRESERVE_EXISTING`` (the
        default) carries forward, but passing ``None`` explicitly
        writes NULL, allowing the owning updater to revoke funding
        metadata when the instrument loses its margin eligibility.
        """
        repo, ipid = sync_repo
        ts1 = _ts()
        ts2 = ts1 + timedelta(hours=1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts1,
                session_id="s1",
                sequence_id=1,
                funding_type="spot_margin_rollover",
                funding_frequency_hours=4,
                rollover_rate_long=0.00025,
                rollover_rate_short=0.00010,
            )
            session.commit()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session=session,
                instrument_public_id=ipid,
                now=ts2,
                session_id="s1",
                sequence_id=2,
                instrument_kind="spot",
                funding_type=None,
                funding_frequency_hours=None,
                rollover_rate_long=None,
                rollover_rate_short=None,
            )
            session.commit()
        with repo.get_session() as session:
            row = DatabaseRepository.get_instrument_spec_sync(
                session, ipid, ts2 + timedelta(seconds=1)
            )
        assert row is not None
        assert row.funding_type is None
        assert row.funding_frequency_hours is None
        assert row.rollover_rate_long is None
        assert row.rollover_rate_short is None
        assert row.instrument_kind == "spot"


class TestPreserveExistingSentinel:
    """Tests for the PRESERVE_EXISTING sentinel object."""

    def test_repr(self) -> None:
        """Verify repr returns a readable string for debugging."""
        assert repr(PRESERVE_EXISTING) == "PRESERVE_EXISTING"

    def test_is_falsy_guard(self) -> None:
        """Verify sentinel is truthy so it is distinguishable from None."""
        assert PRESERVE_EXISTING


class TestInstrumentMetadataRevision:
    """Atomic metadata replacement and preservation contracts."""

    def test_omitted_metadata_preserves_observation_without_refresh(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """A funding-only revision carries the complete metadata block unchanged."""
        repo, instrument_public_id = sync_repo
        first = _ts()
        second = _ts(1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                first,
                "session-1",
                1,
                instrument_kind="perpetual",
                metadata=_metadata(first),
            )
            session.commit()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                second,
                "session-1",
                2,
                instrument_kind="perpetual",
                funding_type="perpetual_funding",
            )
            session.commit()
            row = DatabaseRepository.get_instrument_spec_sync(
                session, instrument_public_id, second + timedelta(seconds=1)
            )
        assert row is not None
        assert row.contract_size == Decimal("3.000000000000000001")
        assert row.spec_observed_at == first
        assert row.unit_certified is True

    def test_supplied_metadata_replaces_nulls_and_forces_incomplete_false(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """A received incomplete definition clears old values and cannot certify."""
        repo, instrument_public_id = sync_repo
        first = _ts()
        second = _ts(1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                first,
                "session-1",
                1,
                metadata=_metadata(first),
            )
            session.commit()
        incomplete = InstrumentMetadataInput(
            tick_size=None,
            lot_size=None,
            min_order_size=None,
            max_order_size=None,
            cost_decimals=None,
            qty_decimals=None,
            margin_initial=None,
            position_limit_long=None,
            position_limit_short=None,
            status="active",
            contract_size=Decimal("9"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:incomplete",
            spec_observed_at=second,
            unit_certified=True,
        )
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                second,
                "session-1",
                2,
                metadata=incomplete,
            )
            session.commit()
            row = DatabaseRepository.get_instrument_spec_sync(
                session, instrument_public_id, second + timedelta(seconds=1)
            )
        assert row is not None
        assert row.tick_size is None
        assert row.lot_size is None
        assert row.contract_size == Decimal("9")
        assert row.unit_certified is False

    def test_partial_provenance_is_cleared_atomically(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """A partial provenance triple is discarded instead of bypassing the CHECK."""
        repo, instrument_public_id = sync_repo
        observed_at = _ts()
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                observed_at,
                "session-1",
                1,
                metadata=_metadata(observed_at, spec_version=None),
            )
            session.commit()
            row = DatabaseRepository.get_instrument_spec_sync(
                session, instrument_public_id, observed_at + timedelta(seconds=1)
            )
        assert row is not None
        assert row.spec_source is None
        assert row.spec_version is None
        assert row.spec_observed_at is None
        assert row.unit_certified is False

    def test_explicit_empty_metadata_clears_the_whole_block(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """A supplied all-NULL observation clears metadata without inventing provenance."""
        repo, instrument_public_id = sync_repo
        first = _ts()
        second = _ts(1)
        empty = InstrumentMetadataInput(
            tick_size=None,
            lot_size=None,
            min_order_size=None,
            max_order_size=None,
            cost_decimals=None,
            qty_decimals=None,
            margin_initial=None,
            position_limit_long=None,
            position_limit_short=None,
            status=None,
            contract_size=None,
            quantity_unit=None,
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
            unit_certified=False,
        )
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                first,
                "session-1",
                1,
                metadata=_metadata(first),
            )
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                second,
                "session-1",
                2,
                metadata=empty,
            )
            session.commit()
            row = DatabaseRepository.get_instrument_spec_sync(
                session, instrument_public_id, second + timedelta(seconds=1)
            )
        assert row is not None
        assert row.tick_size is None
        assert row.contract_size is None
        assert row.quantity_unit is None
        assert row.spec_source is None
        assert row.spec_version is None
        assert row.spec_observed_at is None
        assert row.unit_certified is False

    def test_identical_reobservation_creates_new_scd2_version(
        self, sync_repo: tuple[DatabaseRepository, str]
    ) -> None:
        """A fresh observation time closes and inserts even when content is unchanged."""
        repo, instrument_public_id = sync_repo
        first = _ts()
        second = _ts(1)
        with repo.get_session() as session:
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                first,
                "session-1",
                1,
                metadata=_metadata(first),
            )
            SymbolUpdaterService._revise_instrument_spec(
                session,
                instrument_public_id,
                second,
                "session-1",
                2,
                metadata=_metadata(second),
            )
            session.commit()
            rows = (
                session.execute(
                    select(InstrumentSpec)
                    .where(InstrumentSpec.instrument_public_id == instrument_public_id)
                    .order_by(InstrumentSpec.timestamp)
                )
                .scalars()
                .all()
            )
        assert len(rows) == 2
        assert rows[0].known_to == second
        assert rows[1].spec_observed_at == second


class TestEffectiveUnitCertification:
    """Twelve-hour read-time unit evidence freshness contract."""

    def test_fresh_certification_is_effective(self) -> None:
        """A stored certification just below twelve hours remains effective."""
        observed_at = _ts()
        assert is_effective_unit_certified(
            _certified_row(observed_at), observed_at + timedelta(hours=12, microseconds=-1)
        )

    @pytest.mark.parametrize(
        ("row", "evaluated_at"),
        (
            (_certified_row(_ts()), _ts() + timedelta(hours=12)),
            (_certified_row(_ts(), stored=False), _ts() + timedelta(hours=1)),
            (_certified_row(None), _ts() + timedelta(hours=1)),
            (_certified_row(_ts() + timedelta(seconds=1)), _ts()),
            (_certified_row(datetime(2026, 1, 1)), _ts()),
            (_certified_row(_ts()), datetime(2026, 1, 1)),
        ),
    )
    def test_stale_missing_naive_future_or_stored_false_fails_closed(
        self,
        row: InstrumentSpecRow,
        evaluated_at: datetime,
    ) -> None:
        """Every invalid freshness input produces an ineffective certification."""
        assert is_effective_unit_certified(row, evaluated_at) is False
