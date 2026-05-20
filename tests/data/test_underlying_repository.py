"""Tests for underlying asset repository methods (SQLAlchemyRepository)."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import UnderlyingAssetRow


@pytest.fixture()
async def repo() -> SQLAlchemyRepository:
    """Create an in-memory SQLite repository with tables."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


def _ts(offset_hours: int = 0) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=offset_hours)


async def _seed_instrument(
    repo: SQLAlchemyRepository,
    native_symbol: str,
    exchange: str,
    asset_type: str = "crypto",
    ts: datetime | None = None,
) -> str:
    """Insert a Symbol + Instrument and return the instrument public_id."""
    bus = ts or _ts()
    async with repo.session() as s:
        sym = Symbol(
            native_symbol=native_symbol,
            base=native_symbol.split("-", maxsplit=1)[0],
            quote=native_symbol.split("-")[1] if "-" in native_symbol else None,
            asset_type=asset_type,
            created_at=bus,
            session_id="seed",
            sequence_id=1,
            timestamp=bus,
            known_to=KNOWN_TO_MAX,
        )
        s.add(sym)
        await s.flush()
        inst = Instrument(
            symbol_public_id=sym.public_id,
            exchange=exchange,
            session_id="seed",
            sequence_id=1,
            timestamp=bus,
            known_to=KNOWN_TO_MAX,
        )
        s.add(inst)
        await s.commit()
        return inst.public_id


class TestUpsertUnderlyingAsset:
    """Tests for upsert_underlying_asset SCD2 behaviour."""

    @pytest.mark.asyncio
    async def test_create_fresh(self, repo: SQLAlchemyRepository) -> None:
        """Given no existing row, When upserting, Then returns 'created'."""
        pid, status = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
        )
        assert status == "created"
        assert pid

    @pytest.mark.asyncio
    async def test_unchanged(self, repo: SQLAlchemyRepository) -> None:
        """Given identical fields, When upserting again, Then returns 'unchanged'."""
        ts = _ts()
        await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        _, status = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert status == "unchanged"

    @pytest.mark.asyncio
    async def test_update_name(self, repo: SQLAlchemyRepository) -> None:
        """Given changed name, When upserting, Then returns 'updated' with same public_id."""
        ts = _ts()
        pid1, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        pid2, status = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500 Index",
            asset_class="index",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert status == "updated"
        assert pid2 == pid1

    @pytest.mark.asyncio
    async def test_update_sector(self, repo: SQLAlchemyRepository) -> None:
        """Given changed sector, When upserting, Then returns 'updated'."""
        ts = _ts()
        await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        _, status = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
            sector="US Large Cap",
        )
        assert status == "updated"

    @pytest.mark.asyncio
    async def test_description_round_trips_as_locale_map(self, repo: SQLAlchemyRepository) -> None:
        """Given locale descriptions, When querying, Then JSON map round-trips."""
        ts = _ts()
        await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            description={
                "en": "English description.",
                "pl": "Polish description.",
            },
        )
        row = await repo.get_underlying_by_ticker("SPX", ts + timedelta(seconds=1))
        assert row is not None
        assert row["description"] == {
            "en": "English description.",
            "pl": "Polish description.",
        }

    @pytest.mark.asyncio
    async def test_resolve_description_prefers_locale(self, repo: SQLAlchemyRepository) -> None:
        """Given locale description exists, When resolving, Then returns it."""
        row = UnderlyingAssetRow(
            public_id="ua-1",
            ticker="SPX",
            name={"en": "S&P 500"},
            asset_class="index",
            sector=None,
            description={
                "en": "English description.",
                "pl": "Polish description.",
            },
            timestamp=_ts(),
            session_id="s1",
            sequence_id=1,
            instrument_count=0,
        )
        assert repo.resolve_underlying_description(row, "pl") == "Polish description."

    @pytest.mark.asyncio
    async def test_resolve_description_falls_back_to_en(self, repo: SQLAlchemyRepository) -> None:
        """Given locale description missing, When resolving, Then English returns."""
        row = UnderlyingAssetRow(
            public_id="ua-1",
            ticker="SPX",
            name={"en": "S&P 500"},
            asset_class="index",
            sector=None,
            description={"en": "English description."},
            timestamp=_ts(),
            session_id="s1",
            sequence_id=1,
            instrument_count=0,
        )
        assert repo.resolve_underlying_description(row, "pl") == "English description."

    @pytest.mark.asyncio
    async def test_resolve_description_returns_none_without_description(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Given no description map, When resolving, Then returns none."""
        row = UnderlyingAssetRow(
            public_id="ua-1",
            ticker="SPX",
            name={"en": "S&P 500"},
            asset_class="index",
            sector=None,
            description=None,
            timestamp=_ts(),
            session_id="s1",
            sequence_id=1,
            instrument_count=0,
        )
        assert repo.resolve_underlying_description(row, "pl") is None

    @pytest.mark.asyncio
    async def test_resolve_name_prefers_locale(self, repo: SQLAlchemyRepository) -> None:
        """Given locale name exists, When resolving, Then returns it."""
        row = UnderlyingAssetRow(
            public_id="ua-1",
            ticker="GOLD",
            name={"en": "Gold", "pl": "Złoto"},
            asset_class="commodity",
            sector=None,
            description=None,
            timestamp=_ts(),
            session_id="s1",
            sequence_id=1,
            instrument_count=0,
        )
        assert repo.resolve_underlying_name(row, "pl") == "Złoto"

    @pytest.mark.asyncio
    async def test_resolve_name_falls_back_to_en(self, repo: SQLAlchemyRepository) -> None:
        """Given locale name missing, When resolving, Then English returns."""
        row = UnderlyingAssetRow(
            public_id="ua-1",
            ticker="GOLD",
            name={"en": "Gold"},
            asset_class="commodity",
            sector=None,
            description=None,
            timestamp=_ts(),
            session_id="s1",
            sequence_id=1,
            instrument_count=0,
        )
        assert repo.resolve_underlying_name(row, "pl") == "Gold"


class TestUpsertInstrumentUnderlyingMapping:
    """Tests for upsert_instrument_underlying_mapping SCD2 behaviour."""

    @pytest.mark.asyncio
    async def test_create(self, repo: SQLAlchemyRepository) -> None:
        """Given no existing mapping, When upserting, Then returns 'created'."""
        status = await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
        )
        assert status == "created"

    @pytest.mark.asyncio
    async def test_unchanged(self, repo: SQLAlchemyRepository) -> None:
        """Given identical mapping, When upserting again, Then 'unchanged'."""
        ts = _ts()
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        status = await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert status == "unchanged"

    @pytest.mark.asyncio
    async def test_update_underlying_changed(self, repo: SQLAlchemyRepository) -> None:
        """Given different underlying, When upserting, Then 'updated'."""
        ts = _ts()
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        status = await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-2",
            relationship_type="exact",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert status == "updated"

    @pytest.mark.asyncio
    async def test_update_relationship_changed(self, repo: SQLAlchemyRepository) -> None:
        """Given different relationship_type, When upserting, Then 'updated'."""
        ts = _ts()
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        status = await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="derivative",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert status == "updated"

    @pytest.mark.asyncio
    async def test_update_contract_family_changed(self, repo: SQLAlchemyRepository) -> None:
        """Given different contract_family, When upserting, Then 'updated'."""
        ts = _ts()
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="derivative",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
            contract_family="ES",
        )
        status = await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="derivative",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
            contract_family="MES",
        )
        assert status == "updated"


class TestCloseInstrumentUnderlyingMapping:
    """Tests for close_instrument_underlying_mapping."""

    @pytest.mark.asyncio
    async def test_close_existing(self, repo: SQLAlchemyRepository) -> None:
        """Given active mapping, When closing, Then returns True."""
        ts = _ts()
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id="ua-1",
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        result = await repo.close_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_close_nonexistent(self, repo: SQLAlchemyRepository) -> None:
        """Given no mapping, When closing, Then returns False."""
        result = await repo.close_instrument_underlying_mapping(
            instrument_public_id="inst-999",
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
        )
        assert result is False


class TestGetUnderlyingAssets:
    """Tests for get_underlying_assets query."""

    @pytest.mark.asyncio
    async def test_returns_active(self, repo: SQLAlchemyRepository) -> None:
        """Given two underlyings, When querying, Then returns both sorted."""
        ts = _ts()
        await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await repo.upsert_underlying_asset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="s1",
            sequence_id=2,
            timestamp=ts,
        )
        rows = await repo.get_underlying_assets(ts + timedelta(seconds=1))
        assert len(rows) == 2
        assert rows[0]["ticker"] == "BTC"
        assert rows[1]["ticker"] == "SPX"

    @pytest.mark.asyncio
    async def test_empty(self, repo: SQLAlchemyRepository) -> None:
        """Given no underlyings, When querying, Then returns empty list."""
        rows = await repo.get_underlying_assets(_ts())
        assert rows == []


class TestGetUnderlyingByTicker:
    """Tests for get_underlying_by_ticker lookup."""

    @pytest.mark.asyncio
    async def test_found(self, repo: SQLAlchemyRepository) -> None:
        """Given existing ticker, When looking up, Then returns row."""
        ts = _ts()
        await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        row = await repo.get_underlying_by_ticker("SPX", ts + timedelta(seconds=1))
        assert row is not None
        assert row["ticker"] == "SPX"
        assert row["session_id"] == "s1"

    @pytest.mark.asyncio
    async def test_not_found(self, repo: SQLAlchemyRepository) -> None:
        """Given missing ticker, When looking up, Then returns None."""
        row = await repo.get_underlying_by_ticker("NOPE", _ts())
        assert row is None


class TestGetInstrumentsByUnderlying:
    """Tests for get_instruments_by_underlying join query."""

    @pytest.mark.asyncio
    async def test_returns_mapped_instruments(self, repo: SQLAlchemyRepository) -> None:
        """Given mapped instruments, When querying, Then returns with symbol info."""
        ts = _ts()
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts=ts)
        pid, _ = await repo.upsert_underlying_asset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id=ipid,
            underlying_public_id=pid,
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        rows = await repo.get_instruments_by_underlying(pid, ts + timedelta(seconds=1))
        assert len(rows) == 1
        assert rows[0]["native_symbol"] == "BTC-USD"
        assert rows[0]["exchange"] == "kraken"
        assert rows[0]["relationship_type"] == "exact"

    @pytest.mark.asyncio
    async def test_filter_by_relationship_type(self, repo: SQLAlchemyRepository) -> None:
        """Given mixed relationship types, When filtering, Then returns only matching."""
        ts = _ts()
        ipid1 = await _seed_instrument(repo, "BTC-USD", "kraken", ts=ts)
        ipid2 = await _seed_instrument(repo, "BTC-USD-PERP", "kraken_futures", ts=ts)
        pid, _ = await repo.upsert_underlying_asset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id=ipid1,
            underlying_public_id=pid,
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id=ipid2,
            underlying_public_id=pid,
            relationship_type="derivative",
            session_id="s1",
            sequence_id=2,
            timestamp=ts,
        )
        exact_only = await repo.get_instruments_by_underlying(
            pid,
            ts + timedelta(seconds=1),
            relationship_types=["exact"],
        )
        assert len(exact_only) == 1
        assert exact_only[0]["native_symbol"] == "BTC-USD"

    @pytest.mark.asyncio
    async def test_empty_when_no_mappings(self, repo: SQLAlchemyRepository) -> None:
        """Given no mappings, When querying, Then returns empty list."""
        rows = await repo.get_instruments_by_underlying("ua-nonexistent", _ts())
        assert rows == []


class TestGetUnderlyingForInstrument:
    """Tests for get_underlying_for_instrument reverse lookup."""

    @pytest.mark.asyncio
    async def test_found(self, repo: SQLAlchemyRepository) -> None:
        """Given mapped instrument, When looking up, Then returns underlying."""
        ts = _ts()
        pid, _ = await repo.upsert_underlying_asset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id="inst-1",
            underlying_public_id=pid,
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        row = await repo.get_underlying_for_instrument("inst-1", ts + timedelta(seconds=1))
        assert row is not None
        assert row["ticker"] == "BTC"

    @pytest.mark.asyncio
    async def test_not_found(self, repo: SQLAlchemyRepository) -> None:
        """Given unmapped instrument, When looking up, Then returns None."""
        row = await repo.get_underlying_for_instrument("unmapped", _ts())
        assert row is None


class TestCloseUnderlyingAsset:
    """Tests for close_underlying_asset."""

    @pytest.mark.asyncio
    async def test_close_existing(self, repo: SQLAlchemyRepository) -> None:
        """Given active underlying, When closing, Then returns True."""
        ts = _ts()
        pid, _ = await repo.upsert_underlying_asset(
            ticker="OLD",
            name="Old Asset",
            asset_class="crypto",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        result = await repo.close_underlying_asset(
            public_id=pid,
            session_id="s1",
            sequence_id=2,
            timestamp=ts + timedelta(seconds=1),
        )
        assert result is True
        row = await repo.get_underlying_by_ticker("OLD", ts + timedelta(seconds=2))
        assert row is None

    @pytest.mark.asyncio
    async def test_close_nonexistent(self, repo: SQLAlchemyRepository) -> None:
        """Given no active underlying, When closing, Then returns False."""
        result = await repo.close_underlying_asset(
            public_id="nonexistent",
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
        )
        assert result is False
