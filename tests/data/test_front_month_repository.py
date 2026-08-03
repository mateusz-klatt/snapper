"""Tests for front-month and contract-listing repository methods."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture
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
        base = native_symbol.split("-", maxsplit=1)[0]
        quote = native_symbol.split("-")[1] if "-" in native_symbol else "USD"
        sym = Symbol(
            native_symbol=native_symbol,
            base=base,
            quote=quote,
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


async def _seed_future(
    repo: SQLAlchemyRepository,
    native_symbol: str,
    exchange: str,
    underlying_pid: str,
    contract_family: str,
    expiry_at: datetime,
    ts: datetime | None = None,
) -> str:
    """Seed a complete futures instrument with spec and underlying mapping. Creates the symbol, instrument, instrument spec (kind=future + expiry), and the derivative mapping to the given underlying. Returns instrument public_id."""
    bus = ts or _ts()
    ipid = await _seed_instrument(repo, native_symbol, exchange, ts=bus)
    await repo.revise_instrument_spec(
        instrument_public_id=ipid,
        session_id="seed",
        sequence_id=1,
        timestamp=bus,
        spec=InstrumentSpecInput(instrument_kind="future", expiry_at=expiry_at),
    )
    await repo.upsert_instrument_underlying_mapping(
        instrument_public_id=ipid,
        underlying_public_id=underlying_pid,
        relationship_type="derivative",
        session_id="seed",
        sequence_id=1,
        timestamp=bus,
        contract_family=contract_family,
    )
    return ipid


class TestGetFrontMonthInstrument:
    """Tests for get_front_month_instrument query logic."""

    @pytest.mark.asyncio
    async def test_returns_nearest_non_expired(self, repo: SQLAlchemyRepository) -> None:
        """Given three futures with different expiries, When querying at Jan 1, Then returns the nearest non-expired contract (ESM6 June)."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        esm6 = await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESU6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 9, 18, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESZ6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 12, 18, tzinfo=UTC),
            ts=ts,
        )

        row = await repo.get_front_month_instrument(ua_pid, _ts(0))
        assert row is not None
        assert row["instrument_public_id"] == esm6
        assert row["native_symbol"] == "ESM6"

    @pytest.mark.asyncio
    async def test_all_expired_returns_none(self, repo: SQLAlchemyRepository) -> None:
        """Given three futures, When querying far in the future (all expired), Then returns None."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESU6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 9, 18, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESZ6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 12, 18, tzinfo=UTC),
            ts=ts,
        )

        row = await repo.get_front_month_instrument(ua_pid, _ts(9000))
        assert row is None

    @pytest.mark.asyncio
    async def test_single_contract(self, repo: SQLAlchemyRepository) -> None:
        """Given only one future, When querying before expiry, Then returns that single contract as front-month."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        esm6 = await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        row = await repo.get_front_month_instrument(ua_pid, _ts(0))
        assert row is not None
        assert row["instrument_public_id"] == esm6

    @pytest.mark.asyncio
    async def test_exchange_filter(self, repo: SQLAlchemyRepository) -> None:
        """Given futures on two different exchanges, When filtering by exchange, Then returns only the contract from that exchange."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESM6-EQ",
            "kraken_equities",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        kf_ipid = await _seed_future(
            repo,
            "ESM6-FUT",
            "kraken_futures",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        row = await repo.get_front_month_instrument(
            ua_pid,
            _ts(0),
            exchange="kraken_futures",
        )
        assert row is not None
        assert row["instrument_public_id"] == kf_ipid
        assert row["exchange"] == "kraken_futures"

    @pytest.mark.asyncio
    async def test_contract_family_filter(self, repo: SQLAlchemyRepository) -> None:
        """Given two families ES and MES mapping to same underlying, When filtering by contract_family='ES', Then returns only the ES family contract."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        es_ipid = await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "MESM6",
            "cme",
            ua_pid,
            "MES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        row = await repo.get_front_month_instrument(
            ua_pid,
            _ts(0),
            contract_family="ES",
        )
        assert row is not None
        assert row["instrument_public_id"] == es_ipid
        assert row["contract_family"] == "ES"

    @pytest.mark.asyncio
    async def test_rollover_scenario(self, repo: SQLAlchemyRepository) -> None:
        """Given ESM6 (June) and ESU6 (September), When querying before ESM6 expiry, Then returns ESM6. When querying after ESM6 expiry but before ESU6, Then returns ESU6."""
        ts = _ts()
        esm6_expiry = datetime(2026, 6, 20, tzinfo=UTC)
        esu6_expiry = datetime(2026, 9, 18, tzinfo=UTC)
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        esm6 = await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=esm6_expiry,
            ts=ts,
        )
        esu6 = await _seed_future(
            repo,
            "ESU6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=esu6_expiry,
            ts=ts,
        )

        before_esm6 = datetime(2026, 5, 1, tzinfo=UTC)
        row_before = await repo.get_front_month_instrument(ua_pid, before_esm6)
        assert row_before is not None
        assert row_before["instrument_public_id"] == esm6

        after_esm6 = datetime(2026, 7, 1, tzinfo=UTC)
        row_after = await repo.get_front_month_instrument(ua_pid, after_esm6)
        assert row_after is not None
        assert row_after["instrument_public_id"] == esu6

    @pytest.mark.asyncio
    async def test_datetime_precision(self, repo: SQLAlchemyRepository) -> None:
        """Given expiry at 16:30 UTC, When querying at 14:00 UTC same day, Then the contract is still active (not yet expired)."""
        ts = _ts()
        expiry = datetime(2026, 6, 20, 16, 30, tzinfo=UTC)
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        ipid = await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=expiry,
            ts=ts,
        )

        as_of = datetime(2026, 6, 20, 14, 0, tzinfo=UTC)
        row = await repo.get_front_month_instrument(ua_pid, as_of)
        assert row is not None
        assert row["instrument_public_id"] == ipid


class TestGetContractsForUnderlying:
    """Tests for get_contracts_for_underlying query logic."""

    @pytest.mark.asyncio
    async def test_returns_sorted_by_family_then_expiry(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given 3 contracts across ES and MES families, When querying, Then returns sorted by (contract_family, expiry_at)."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESU6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 9, 18, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "MESM6",
            "cme",
            ua_pid,
            "MES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(ua_pid, _ts(0))
        assert len(rows) == 3
        assert rows[0]["contract_family"] == "ES"
        assert rows[0]["native_symbol"] == "ESM6"
        assert rows[1]["contract_family"] == "ES"
        assert rows[1]["native_symbol"] == "ESU6"
        assert rows[2]["contract_family"] == "MES"
        assert rows[2]["native_symbol"] == "MESM6"

    @pytest.mark.asyncio
    async def test_is_front_month_flag(self, repo: SQLAlchemyRepository) -> None:
        """Given two ES contracts (June + September), When querying before June expiry, Then nearest (June) has is_front_month=True, September has False."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESU6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 9, 18, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(ua_pid, _ts(0))
        assert len(rows) == 2
        assert rows[0]["native_symbol"] == "ESM6"
        assert rows[0]["is_front_month"] is True
        assert rows[1]["native_symbol"] == "ESU6"
        assert rows[1]["is_front_month"] is False

    @pytest.mark.asyncio
    async def test_exclude_expired_by_default(self, repo: SQLAlchemyRepository) -> None:
        """Given one expired and one active contract, When querying without include_expired, Then only the active contract is returned."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESH6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2025, 3, 21, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(ua_pid, _ts(0))
        assert len(rows) == 1
        assert rows[0]["native_symbol"] == "ESM6"

    @pytest.mark.asyncio
    async def test_include_expired(self, repo: SQLAlchemyRepository) -> None:
        """Given one expired and one active contract, When querying with include_expired=True, Then both contracts are returned."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESH6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2025, 3, 21, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(
            ua_pid,
            _ts(0),
            include_expired=True,
        )
        assert len(rows) == 2
        symbols = [r["native_symbol"] for r in rows]
        assert "ESH6" in symbols
        assert "ESM6" in symbols

    @pytest.mark.asyncio
    async def test_empty_when_no_derivatives(self, repo: SQLAlchemyRepository) -> None:
        """Given an underlying with only exact-mapped instruments, When querying contracts, Then returns empty list (only derivative relationships qualify)."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        ipid = await _seed_instrument(repo, "BTC-USD", "kraken", ts=ts)
        await repo.upsert_instrument_underlying_mapping(
            instrument_public_id=ipid,
            underlying_public_id=ua_pid,
            relationship_type="exact",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )

        rows = await repo.get_contracts_for_underlying(ua_pid, _ts(0))
        assert rows == []

    @pytest.mark.asyncio
    async def test_contract_family_filter(self, repo: SQLAlchemyRepository) -> None:
        """Given contracts in ES and MES families, When filtering by contract_family='ES', Then only ES contracts are returned."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESM6",
            "cme",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "MESM6",
            "cme",
            ua_pid,
            "MES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(
            ua_pid,
            _ts(0),
            contract_family="ES",
        )
        assert len(rows) == 1
        assert rows[0]["native_symbol"] == "ESM6"
        assert rows[0]["contract_family"] == "ES"

    @pytest.mark.asyncio
    async def test_exchange_filter(self, repo: SQLAlchemyRepository) -> None:
        """Given contracts on two exchanges, When filtering by exchange, Then only that exchange returned."""
        ts = _ts()
        ua_pid, _ = await repo.upsert_underlying_asset(
            ticker="SPX",
            name="S&P 500",
            asset_class="index",
            session_id="s1",
            sequence_id=1,
            timestamp=ts,
        )
        await _seed_future(
            repo,
            "ESM6-EQ",
            "kraken_equities",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )
        await _seed_future(
            repo,
            "ESM6-FUT",
            "kraken_futures",
            ua_pid,
            "ES",
            expiry_at=datetime(2026, 6, 20, tzinfo=UTC),
            ts=ts,
        )

        rows = await repo.get_contracts_for_underlying(
            ua_pid,
            _ts(0),
            exchange="kraken_equities",
        )
        assert len(rows) == 1
        assert rows[0]["exchange"] == "kraken_equities"
