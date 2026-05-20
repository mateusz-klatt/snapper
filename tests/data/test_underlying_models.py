"""Tests for UnderlyingAsset and InstrumentUnderlyingMapping ORM models."""

from datetime import UTC
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import UnderlyingAsset


@pytest.fixture()
def db_session() -> Session:
    """Create an in-memory SQLite database with all tables."""
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _now() -> datetime:
    return datetime.now(UTC)


class TestUnderlyingAsset:
    """Tests for UnderlyingAsset model instantiation and constraints."""

    def test_create_underlying_asset(self, db_session: Session) -> None:
        """Given valid fields, When inserted, Then row persists with all columns."""
        asset = UnderlyingAsset(
            public_id="ua-001",
            name={"en": "S&P 500"},
            ticker="SPX",
            asset_class="index",
            sector="US Large Cap",
            description={"en": "Standard & Poor 500 Index"},
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(asset)
        db_session.commit()
        rows = db_session.execute(select(UnderlyingAsset)).scalars().all()
        assert len(rows) == 1
        assert rows[0].ticker == "SPX"
        assert rows[0].name == {"en": "S&P 500"}
        assert rows[0].sector == "US Large Cap"
        assert rows[0].description == {"en": "Standard & Poor 500 Index"}

    def test_nullable_fields(self, db_session: Session) -> None:
        """Given sector=None and description=None, When inserted, Then succeeds."""
        asset = UnderlyingAsset(
            public_id="ua-002",
            name={"en": "Bitcoin"},
            ticker="BTC",
            asset_class="crypto",
            sector=None,
            description=None,
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(asset)
        db_session.commit()
        row = db_session.execute(select(UnderlyingAsset)).scalars().first()
        assert row is not None
        assert row.sector is None
        assert row.description is None

    def test_invalid_asset_class_rejected(self, db_session: Session) -> None:
        """Given invalid asset_class, When inserted, Then CHECK constraint fails."""
        asset = UnderlyingAsset(
            public_id="ua-003",
            name={"en": "Bad"},
            ticker="BAD",
            asset_class="invalid_class",
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(asset)
        with pytest.raises(IntegrityError, match="ck_underlying_asset_class"):
            db_session.commit()


class TestInstrumentUnderlyingMapping:
    """Tests for InstrumentUnderlyingMapping model instantiation and constraints."""

    def test_create_mapping(self, db_session: Session) -> None:
        """Given valid fields, When inserted, Then row persists."""
        mapping = InstrumentUnderlyingMapping(
            public_id="ium-001",
            instrument_public_id="inst-001",
            underlying_public_id="ua-001",
            relationship_type="derivative",
            contract_family="ES",
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(mapping)
        db_session.commit()
        rows = db_session.execute(select(InstrumentUnderlyingMapping)).scalars().all()
        assert len(rows) == 1
        assert rows[0].relationship_type == "derivative"
        assert rows[0].contract_family == "ES"

    def test_nullable_contract_family(self, db_session: Session) -> None:
        """Given contract_family=None, When inserted, Then succeeds."""
        mapping = InstrumentUnderlyingMapping(
            public_id="ium-002",
            instrument_public_id="inst-002",
            underlying_public_id="ua-001",
            relationship_type="exact",
            contract_family=None,
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(mapping)
        db_session.commit()
        row = db_session.execute(select(InstrumentUnderlyingMapping)).scalars().first()
        assert row is not None
        assert row.contract_family is None

    def test_invalid_relationship_type_rejected(self, db_session: Session) -> None:
        """Given invalid relationship_type, When inserted, Then CHECK constraint fails."""
        mapping = InstrumentUnderlyingMapping(
            public_id="ium-003",
            instrument_public_id="inst-003",
            underlying_public_id="ua-001",
            relationship_type="bogus",
            session_id="sess-1",
            sequence_id=1,
            timestamp=_now(),
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(mapping)
        with pytest.raises(IntegrityError, match="ck_ium_relationship_type"):
            db_session.commit()

    def test_duplicate_active_instrument_rejected(self, db_session: Session) -> None:
        """Given two active mappings for same instrument, When committed, Then unique index fails."""
        now = _now()
        m1 = InstrumentUnderlyingMapping(
            public_id="ium-004a",
            instrument_public_id="inst-004",
            underlying_public_id="ua-001",
            relationship_type="exact",
            session_id="sess-1",
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        m2 = InstrumentUnderlyingMapping(
            public_id="ium-004b",
            instrument_public_id="inst-004",
            underlying_public_id="ua-002",
            relationship_type="derivative",
            session_id="sess-1",
            sequence_id=2,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        db_session.add(m1)
        db_session.commit()
        db_session.add(m2)
        with pytest.raises(IntegrityError):
            db_session.commit()
