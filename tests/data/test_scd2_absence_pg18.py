"""Opt-in live PostgreSQL 18 deterministic two-session SCD2 race suite for PR-1."""

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import MarketSnapshot
from snapper.data.models import Operator
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.models import Wallet
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import Scd2UpsertSpec
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import TemporalVersionConflictError
from snapper.data.repository import begin_scd2_write
from snapper.data.repository import begin_scd2_write_sync
from snapper.data.repository import close_and_insert_existing
from snapper.data.repository import upsert_scd2
from snapper.data.repository import upsert_scd2_sync
from snapper.data.repository_types import CheckpointUpsertRow
from snapper.data.repository_types import DeskMembershipDetach
from snapper.data.repository_types import MarketSnapshotUpsertRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceUpsertRow

_T0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(seconds=10)
_T2 = _T0 + timedelta(seconds=20)
_WAIT_TIMEOUT_S = 10.0


def _pg_urls() -> tuple[str, str]:
    """Return ``(async_url, sync_url)`` from ``PNL_TEST_POSTGRES_URL`` or skip."""
    raw = os.environ.get("PNL_TEST_POSTGRES_URL", "").strip()
    if not raw:
        pytest.skip("PNL_TEST_POSTGRES_URL is not set; skipping PG18 race tests")
    parsed = make_url(raw)
    assert parsed.get_backend_name() == "postgresql"
    async_url = parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    sync_url = parsed.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)
    return async_url, sync_url


@pytest.fixture
async def pg_repos() -> AsyncIterator[tuple[SQLAlchemyRepository, DatabaseRepository]]:
    """Provision a disposable isolated PG18 database for two-session race tests."""
    async_base, _ = _pg_urls()
    parsed = make_url(async_base)
    database_name = f"scd2_pr1_race_{uuid4().hex}"
    maintenance = SQLAlchemyRepository(async_base)
    created = False
    async_repo: SQLAlchemyRepository | None = None
    sync_repo: DatabaseRepository | None = None
    try:
        async with maintenance.engine.connect() as conn:
            admin = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created = True
        target_async = parsed.set(database=database_name).render_as_string(hide_password=False)
        target_sync = parsed.set(
            drivername="postgresql+psycopg2", database=database_name
        ).render_as_string(hide_password=False)
        async_repo = SQLAlchemyRepository(target_async)
        sync_repo = DatabaseRepository(target_sync)
        await async_repo.create_all()
        yield async_repo, sync_repo
    finally:
        if async_repo is not None:
            await async_repo.engine.dispose()
        if sync_repo is not None:
            sync_repo.engine.dispose()
        if created:
            async with maintenance.engine.connect() as conn:
                admin = await conn.execution_options(isolation_level="AUTOCOMMIT")
                await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        await maintenance.engine.dispose()


async def _wait_for_blocked_lock(repo: SQLAlchemyRepository, relation: str) -> None:
    """Wait until PostgreSQL reports a waiting lock on ``relation``."""
    deadline = asyncio.get_running_loop().time() + _WAIT_TIMEOUT_S
    query = text(
        "SELECT count(*) FROM pg_locks l "
        "JOIN pg_class c ON c.oid = l.relation "
        "WHERE c.relname = :rel AND NOT l.granted"
    )
    while asyncio.get_running_loop().time() < deadline:
        async with repo.session() as session:
            blocked = int((await session.execute(query, {"rel": relation})).scalar_one())
            if blocked > 0:
                return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"Timed out waiting for FOR UPDATE lock contention on {relation}")


async def _seed_symbol_instrument_pg(
    repo: SQLAlchemyRepository,
    native_symbol: str,
) -> tuple[str, str]:
    """Insert an active Symbol and Instrument pair on PostgreSQL."""
    symbol_pid = str(uuid4())
    instrument_pid = str(uuid4())
    async with repo.session() as session:
        session.add(
            Symbol(
                public_id=symbol_pid,
                native_symbol=native_symbol,
                base=native_symbol.split("-", maxsplit=1)[0],
                quote="USD",
                asset_type="crypto",
                created_at=_T0,
                timestamp=_T0,
                session_id="pg-seed",
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id=instrument_pid,
                symbol_public_id=symbol_pid,
                exchange="kraken",
                created_at=_T0,
                timestamp=_T0,
                session_id="pg-seed",
                sequence_id=2,
            )
        )
        await session.commit()
    return symbol_pid, instrument_pid


@pytest.mark.asyncio
async def test_pg18_cold_start_absence_race(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two writers hitting cold-start absence resolve cleanly to one active row."""
    async_repo, _ = pg_repos
    key = f"pg-cold-{uuid4().hex}"
    select_barrier = asyncio.Barrier(2)
    original_flush = AsyncSession.flush

    async def _coordinated_flush(session: AsyncSession) -> None:
        if any(isinstance(obj, Setting) and obj.key == key for obj in session.new):
            await select_barrier.wait()
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _coordinated_flush)

    async def _worker(tag: str) -> str:
        async with async_repo.session() as session:
            await begin_scd2_write(session)
            outcome = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == key],
                    bus_time=_T0,
                    decide=lambda existing: {
                        "key": key,
                        "value": tag if existing is None else f"{existing.value}+{tag}",
                        "category": "race",
                        "description": None,
                        "is_encrypted": False,
                        "session_id": tag,
                        "sequence_id": 1,
                    },
                ),
            )
            await session.commit()
            return outcome.outcome

    outcomes = await asyncio.wait_for(
        asyncio.gather(_worker("w1"), _worker("w2")), timeout=_WAIT_TIMEOUT_S
    )
    assert sorted(outcomes) == ["created", "updated"]
    async with async_repo.session() as session:
        active = (
            await session.scalars(
                select(Setting).where(Setting.key == key, Setting.known_to == KNOWN_TO_MAX)
            )
        ).all()
        assert len(active) == 1


@pytest.mark.asyncio
async def test_pg18_evalplanqual_lock_wake_and_clock_skew_race(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Waking from FOR UPDATE after predecessor close retries cleanly and clamps clock skew."""
    async_repo, _ = pg_repos
    key = f"pg-epq-{uuid4().hex}"
    initial_pid = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            Setting(
                public_id=initial_pid,
                key=key,
                value="v1",
                category="race",
                description=None,
                is_encrypted=False,
                session_id="seed",
                sequence_id=1,
                timestamp=_T2,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()

    a_locked = asyncio.Event()
    b_finished = False

    async def _session_a() -> None:
        async with async_repo.session() as session:
            await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == key],
                    bus_time=_T2,
                    decide=lambda _r: {
                        "key": key,
                        "value": "v2",
                        "category": "race",
                        "description": None,
                        "is_encrypted": False,
                        "session_id": "a",
                        "sequence_id": 2,
                    },
                ),
            )
            a_locked.set()
            await _wait_for_blocked_lock(async_repo, "settings")
            await session.commit()

    async def _session_b() -> str:
        nonlocal b_finished
        await asyncio.wait_for(a_locked.wait(), timeout=_WAIT_TIMEOUT_S)
        async with async_repo.session() as session:
            outcome = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == key],
                    bus_time=_T1,
                    decide=lambda _r: {
                        "key": key,
                        "value": "v3",
                        "category": "race",
                        "description": None,
                        "is_encrypted": False,
                        "session_id": "b",
                        "sequence_id": 3,
                    },
                ),
            )
            await session.commit()
            assert outcome.row is not None
            assert outcome.row.public_id == initial_pid
            assert outcome.row.timestamp == _T2
            b_finished = True
            return outcome.outcome

    _, b_outcome = await asyncio.wait_for(
        asyncio.gather(_session_a(), _session_b()), timeout=_WAIT_TIMEOUT_S
    )
    assert b_finished is True
    assert b_outcome == "updated"


@pytest.mark.asyncio
async def test_pg18_race_instrument_spec(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """InstrumentSpec absence race across concurrent writers converges to 1 active row."""
    async_repo, sync_repo = pg_repos
    _, instrument_pid = await _seed_symbol_instrument_pg(async_repo, f"SPEC-{uuid4().hex[:6]}-USD")
    barrier = asyncio.Barrier(2)

    async def _async_write() -> int:
        await barrier.wait()
        return await async_repo.revise_instrument_spec(
            instrument_public_id=instrument_pid,
            session_id="async-spec",
            sequence_id=1,
            timestamp=_T1,
            spec=InstrumentSpecInput(instrument_kind="spot"),
        )

    def _sync_write() -> str:
        with sync_repo.get_session() as session:
            begin_scd2_write_sync(session)
            outcome = sync_repo.revise_instrument_spec_sync(
                session,
                instrument_pid,
                "sync-spec",
                1,
                _T1,
                InstrumentSpecInput(instrument_kind="spot"),
            )
            session.commit()
            return outcome

    async def _coordinated_sync() -> str:
        await barrier.wait()
        return await asyncio.to_thread(_sync_write)

    res_async, res_sync = await asyncio.wait_for(
        asyncio.gather(_async_write(), _coordinated_sync()), timeout=_WAIT_TIMEOUT_S
    )
    assert res_async > 0 and res_sync in {"created", "updated", "unchanged"}


@pytest.mark.asyncio
async def test_pg18_race_symbol(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Two cross-exchange writers upserting the same Symbol resolve without duplicates."""
    _, sync_repo = pg_repos
    native = f"EUR-PLN-{uuid4().hex[:6]}"
    barrier = asyncio.Barrier(2)

    def _run_worker(exchange: str) -> str:
        with sync_repo.get_session() as session:
            begin_scd2_write_sync(session)
            res = upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=Symbol,
                    match_filters=[Symbol.native_symbol == native],
                    bus_time=_T1,
                    decide=lambda existing: (
                        None
                        if existing is not None
                        else {
                            "native_symbol": native,
                            "base": "EUR",
                            "quote": "PLN",
                            "asset_type": "forex",
                            "created_at": _T1,
                            "session_id": exchange,
                            "sequence_id": 1,
                        }
                    ),
                ),
            )
            session.commit()
            return res.outcome

    async def _task(exchange: str) -> str:
        await barrier.wait()
        return await asyncio.to_thread(_run_worker, exchange)

    outcomes = await asyncio.wait_for(
        asyncio.gather(_task("kraken"), _task("walutomat")), timeout=_WAIT_TIMEOUT_S
    )
    assert sorted(outcomes) == ["created", "unchanged"]


@pytest.mark.asyncio
async def test_pg18_race_symbol_alias_and_secondary_unique_index(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """SymbolAlias handles primary absence race and re-raises secondary collision immediately."""
    async_repo, _ = pg_repos
    sym_a, _ = await _seed_symbol_instrument_pg(async_repo, f"AL1-{uuid4().hex[:6]}-USD")
    sym_b, _ = await _seed_symbol_instrument_pg(async_repo, f"AL2-{uuid4().hex[:6]}-USD")
    barrier = asyncio.Barrier(2)

    async def _alias_worker(tag: str) -> str:
        await barrier.wait()
        async with async_repo.session() as session:
            res = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=SymbolAlias,
                    match_filters=[
                        SymbolAlias.symbol_public_id == sym_a,
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == "ws",
                    ],
                    bus_time=_T1,
                    decide=lambda existing: (
                        None
                        if existing is not None
                        else {
                            "symbol_public_id": sym_a,
                            "exchange": "kraken",
                            "channel": "ws",
                            "exchange_symbol": "RACE/USD",
                            "created_at": _T1,
                            "session_id": tag,
                            "sequence_id": 1,
                        }
                    ),
                ),
            )
            await session.commit()
            return res.outcome

    outcomes = await asyncio.wait_for(
        asyncio.gather(_alias_worker("a"), _alias_worker("b")), timeout=_WAIT_TIMEOUT_S
    )
    assert sorted(outcomes) == ["created", "unchanged"]

    async with async_repo.session() as session:
        with pytest.raises(IntegrityError):
            await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=SymbolAlias,
                    match_filters=[
                        SymbolAlias.symbol_public_id == sym_b,
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == "ws",
                    ],
                    bus_time=_T2,
                    decide=lambda _r: {
                        "symbol_public_id": sym_b,
                        "exchange": "kraken",
                        "channel": "ws",
                        "exchange_symbol": "RACE/USD",
                        "created_at": _T2,
                        "session_id": "sec",
                        "sequence_id": 2,
                    },
                ),
            )


async def _run_concurrent_simple_upsert[ModelT: Base](
    repo: SQLAlchemyRepository,
    build_spec_a: Scd2UpsertSpec[ModelT],
    build_spec_b: Scd2UpsertSpec[ModelT],
) -> tuple[str, str]:
    """Execute two barrier-synchronized async ``upsert_scd2`` calls against ``repo``."""
    barrier = asyncio.Barrier(2)

    async def _one(spec: Scd2UpsertSpec[ModelT]) -> str:
        await barrier.wait()
        async with repo.session() as session:
            outcome = await upsert_scd2(session, spec)
            await session.commit()
            return outcome.outcome

    first, second = await asyncio.wait_for(
        asyncio.gather(_one(build_spec_a), _one(build_spec_b)), timeout=_WAIT_TIMEOUT_S
    )
    return first, second


@pytest.mark.asyncio
async def test_pg18_race_symbol_exchange_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent SymbolExchangeCapability upserts converge to one active row."""
    async_repo, _ = pg_repos
    sym_pid, _ = await _seed_symbol_instrument_pg(async_repo, f"SEC-{uuid4().hex[:6]}-USD")
    spec_a = Scd2UpsertSpec(
        model=SymbolExchangeCapability,
        match_filters=[
            SymbolExchangeCapability.symbol_public_id == sym_pid,
            SymbolExchangeCapability.exchange == "kraken",
        ],
        bus_time=_T1,
        decide=lambda existing: {
            "symbol_public_id": sym_pid,
            "exchange": "kraken",
            "can_market_data": True,
            "can_trade": True,
            "source": "a",
            "reason": None,
            "created_at": existing.created_at if existing is not None else _T1,
            "session_id": "a",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=SymbolExchangeCapability,
        match_filters=spec_a.match_filters,
        bus_time=_T1,
        decide=lambda existing: {
            "symbol_public_id": sym_pid,
            "exchange": "kraken",
            "can_market_data": True,
            "can_trade": False,
            "source": "b",
            "reason": None,
            "created_at": existing.created_at if existing is not None else _T1,
            "session_id": "b",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


@pytest.mark.asyncio
async def test_pg18_race_symbol_market_data_channel_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent SymbolMarketDataChannelCapability writers converge cleanly."""
    async_repo, _ = pg_repos
    sym_pid, _ = await _seed_symbol_instrument_pg(async_repo, f"MDC-{uuid4().hex[:6]}-USD")
    filters = [
        SymbolMarketDataChannelCapability.symbol_public_id == sym_pid,
        SymbolMarketDataChannelCapability.exchange == "kraken_futures",
        SymbolMarketDataChannelCapability.channel == "trade",
    ]
    spec_a = Scd2UpsertSpec(
        model=SymbolMarketDataChannelCapability,
        match_filters=filters,
        bus_time=_T1,
        decide=lambda existing: {
            "symbol_public_id": sym_pid,
            "exchange": "kraken_futures",
            "channel": "trade",
            "can_market_data": False,
            "source": "runtime",
            "reason": "no-trade",
            "created_at": existing.created_at if existing is not None else _T1,
            "session_id": "pub",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=SymbolMarketDataChannelCapability,
        match_filters=filters,
        bus_time=_T1,
        decide=lambda existing: {
            "symbol_public_id": sym_pid,
            "exchange": "kraken_futures",
            "channel": "trade",
            "can_market_data": True,
            "source": "updater",
            "reason": None,
            "created_at": existing.created_at if existing is not None else _T1,
            "session_id": "upd",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


@pytest.mark.asyncio
async def test_pg18_race_instrument_order_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent InstrumentOrderCapability writers resolve cleanly under SAVEPOINT."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"IOC-{uuid4().hex[:6]}-USD")
    filters = [
        InstrumentOrderCapability.instrument_public_id == inst_pid,
        InstrumentOrderCapability.exchange == "kraken",
    ]
    spec_a = Scd2UpsertSpec(
        model=InstrumentOrderCapability,
        match_filters=filters,
        bus_time=_T1,
        decide=lambda _r: {
            "instrument_public_id": inst_pid,
            "exchange": "kraken",
            "can_place_order": False,
            "source": "tokenized",
            "reason": "restricted",
            "session_id": "a",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=InstrumentOrderCapability,
        match_filters=filters,
        bus_time=_T1,
        decide=lambda _r: {
            "instrument_public_id": inst_pid,
            "exchange": "kraken",
            "can_place_order": True,
            "source": "updater",
            "reason": None,
            "session_id": "b",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


@pytest.mark.asyncio
async def test_pg18_race_setting(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent Setting upserts resolve cleanly without duplicate active rows."""
    async_repo, _ = pg_repos
    key = f"pg-set-{uuid4().hex[:8]}"
    spec_a = Scd2UpsertSpec(
        model=Setting,
        match_filters=[Setting.key == key],
        bus_time=_T1,
        decide=lambda _r: {
            "key": key,
            "value": "v1",
            "category": "app",
            "description": None,
            "is_encrypted": False,
            "session_id": "a",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=Setting,
        match_filters=[Setting.key == key],
        bus_time=_T1,
        decide=lambda _r: {
            "key": key,
            "value": "v2",
            "category": "app",
            "description": None,
            "is_encrypted": False,
            "session_id": "b",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


def _snapshot_row(instrument_pid: str, seq: int, bid: float) -> MarketSnapshotUpsertRow:
    """Build a valid ``MarketSnapshotUpsertRow`` for PG18 snapshot race tests."""
    return MarketSnapshotUpsertRow(
        instrument_public_id=instrument_pid,
        timestamp=_T1,
        session_id=f"snap-{seq}",
        sequence_id=seq,
        bid=bid,
        bid_volume=1.0,
        ask=bid + 1.0,
        ask_volume=1.0,
        last_price=bid + 0.5,
        volume_24h=10.0,
        vwap_24h=bid + 0.5,
        low_24h=bid,
        high_24h=bid + 1.0,
        change_24h=0.0,
        spread=1.0,
        spread_pct=0.01,
        public_id=str(uuid4()),
        known_to=KNOWN_TO_MAX,
    )


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_single_row(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent single-row MarketSnapshot writers resolve via SAVEPOINT cleanly."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"SNP1-{uuid4().hex[:6]}-USD")
    barrier = asyncio.Barrier(2)

    async def _single(seq: int, bid: float) -> None:
        await barrier.wait()
        async with async_repo.session() as session:
            await async_repo._upsert_snapshot_row(session, _snapshot_row(inst_pid, seq, bid))
            await session.commit()

    await asyncio.wait_for(
        asyncio.gather(_single(1, 50.0), _single(2, 51.0)), timeout=_WAIT_TIMEOUT_S
    )


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_bulk(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent bulk upsert_market_snapshots collectors fall back per-row cleanly."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"SNPB-{uuid4().hex[:6]}-USD")
    barrier = asyncio.Barrier(2)

    async def _bulk(seq: int, bid: float) -> int:
        await barrier.wait()
        return await async_repo.upsert_market_snapshots([_snapshot_row(inst_pid, seq, bid)])

    counts = await asyncio.wait_for(
        asyncio.gather(_bulk(1, 60.0), _bulk(2, 61.0)), timeout=_WAIT_TIMEOUT_S
    )
    assert counts == [1, 1]
    async with async_repo.session() as session:
        active = (
            await session.scalars(
                select(MarketSnapshot).where(
                    MarketSnapshot.instrument_public_id == inst_pid,
                    MarketSnapshot.known_to == KNOWN_TO_MAX,
                )
            )
        ).all()
        assert len(active) == 1


@pytest.mark.asyncio
async def test_pg18_race_instrument(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Instrument absence race succeeds once and concurrent lock-wake refuses overwrite."""
    async_repo, _ = pg_repos
    sym_pid = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            Symbol(
                public_id=sym_pid,
                native_symbol=f"INST-{uuid4().hex[:6]}-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=_T0,
                timestamp=_T0,
                session_id="seed",
                sequence_id=1,
            )
        )
        await session.commit()

    barrier = asyncio.Barrier(2)

    async def _ensure(seq: int) -> str:
        await barrier.wait()
        async with async_repo.session() as session:
            outcome = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Instrument,
                    match_filters=[
                        Instrument.symbol_public_id == sym_pid,
                        Instrument.exchange == "kraken",
                    ],
                    bus_time=_T1,
                    decide=lambda existing: (
                        None
                        if existing is not None
                        else {
                            "symbol_public_id": sym_pid,
                            "exchange": "kraken",
                            "created_at": _T1,
                            "session_id": f"i-{seq}",
                            "sequence_id": seq,
                        }
                    ),
                ),
            )
            await session.commit()
            assert outcome.row is not None
            return str(outcome.row.public_id)

    pids = await asyncio.wait_for(asyncio.gather(_ensure(1), _ensure(2)), timeout=_WAIT_TIMEOUT_S)
    assert pids[0] == pids[1]


@pytest.mark.asyncio
async def test_pg18_race_user(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent Contract A User close-and-insert enforces stale-version refusal."""
    async_repo, _ = pg_repos
    username = f"u_{uuid4().hex[:8]}"
    async with async_repo.session() as session:
        session.add(
            User(
                username=username,
                password_hash="h1",
                email=f"{username}@example.com",
                role="viewer",
                is_active=True,
                session_id="seed",
                sequence_id=1,
                timestamp=_T0,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()

    a_locked = asyncio.Event()

    async def _user_a() -> None:
        async with async_repo.session() as session:
            await close_and_insert_existing(
                session,
                User,
                [User.username == username],
                {
                    "username": username,
                    "password_hash": "h2",
                    "email": f"{username}@example.com",
                    "role": "operator",
                    "is_active": True,
                    "session_id": "a",
                    "sequence_id": 2,
                },
                _T1,
            )
            a_locked.set()
            await _wait_for_blocked_lock(async_repo, "users")
            await session.commit()

    async def _user_b() -> None:
        await asyncio.wait_for(a_locked.wait(), timeout=_WAIT_TIMEOUT_S)
        async with async_repo.session() as session:
            await close_and_insert_existing(
                session,
                User,
                [User.username == username],
                {
                    "username": username,
                    "password_hash": "h3",
                    "email": f"{username}@example.com",
                    "role": "admin",
                    "is_active": True,
                    "session_id": "b",
                    "sequence_id": 3,
                },
                _T2,
            )
            await session.commit()

    await asyncio.wait_for(asyncio.gather(_user_a(), _user_b()), timeout=_WAIT_TIMEOUT_S)
    async with async_repo.session() as session:
        active = (
            await session.scalars(
                select(User).where(User.username == username, User.known_to == KNOWN_TO_MAX)
            )
        ).all()
        assert len(active) == 1


@pytest.mark.asyncio
async def test_pg18_race_user_trading_caps(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent UserTradingCaps writers resolve cleanly under SAVEPOINT."""
    async_repo, _ = pg_repos
    user_pid = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            User(
                public_id=user_pid,
                username=f"caps_{uuid4().hex[:6]}",
                password_hash="h",
                role="operator",
                is_active=True,
                session_id="seed",
                sequence_id=1,
                timestamp=_T0,
            )
        )
        await session.commit()

    spec_a = Scd2UpsertSpec(
        model=UserTradingCaps,
        match_filters=[UserTradingCaps.user_public_id == user_pid],
        bus_time=_T1,
        decide=lambda _r: {
            "user_public_id": user_pid,
            "max_order_notional_usd": Decimal("100"),
            "session_id": "a",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=UserTradingCaps,
        match_filters=[UserTradingCaps.user_public_id == user_pid],
        bus_time=_T1,
        decide=lambda _r: {
            "user_public_id": user_pid,
            "max_order_notional_usd": Decimal("200"),
            "session_id": "b",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


@pytest.mark.asyncio
async def test_pg18_race_user_operator_membership_detach(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent _commit_desk_detach refuses double-detach cleanly."""
    async_repo, _ = pg_repos
    user_pid = str(uuid4())
    op_pid = str(uuid4())
    username = f"mbr_{uuid4().hex[:6]}"
    async with async_repo.session() as session:
        session.add(
            User(
                public_id=user_pid,
                username=username,
                password_hash="h",
                role="viewer",
                is_active=True,
                session_id="seed",
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_{uuid4().hex[:6]}",
                session_id="seed",
                sequence_id=2,
                timestamp=_T0,
            )
        )
        await session.flush()
        session.add(
            UserOperatorMembership(
                user_public_id=user_pid,
                operator_public_id=op_pid,
                is_primary=False,
                session_id="seed",
                sequence_id=3,
                timestamp=_T0,
            )
        )
        await session.commit()

    barrier = asyncio.Barrier(2)

    async def _noop_revoke(_uid: str) -> None:
        return None

    async def _detach(seq: int) -> str:
        await barrier.wait()
        try:
            res = await async_repo.detach_viewer_from_desk(
                DeskMembershipDetach(
                    username=username,
                    operator_public_id=op_pid,
                    timestamp=_T1,
                    session_id=f"det-{seq}",
                    sequence_id=seq,
                ),
                _noop_revoke,
            )
            return "detached" if res is not None else "missing"
        except TemporalVersionConflictError:
            return "conflict"

    results = await asyncio.wait_for(
        asyncio.gather(_detach(1), _detach(2)), timeout=_WAIT_TIMEOUT_S
    )
    assert sorted(results) in (["detached", "missing"], ["conflict", "detached"])


@pytest.mark.asyncio
async def test_pg18_race_spot_asset_precision_evidence(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Sync and async precision writers resolve concurrent updates on PG18."""
    async_repo, sync_repo = pg_repos
    asset = f"A{uuid4().hex[:5]}"
    barrier = asyncio.Barrier(2)

    async def _async_ev() -> None:
        await barrier.wait()
        await async_repo.upsert_spot_asset_precision_evidence(
            SpotAssetPrecisionEvidenceUpsertRow(
                exchange="walutomat",
                asset=asset,
                balance_decimals=2,
                balance_source="bal",
                balance_version="v1",
                balance_observed_at=_T1,
                fee_decimals=None,
                fee_source=None,
                fee_version=None,
                fee_observed_at=None,
                session_id="a",
                sequence_id=1,
                timestamp=_T1,
            )
        )

    def _sync_ev() -> None:
        with sync_repo.get_session() as session:
            begin_scd2_write_sync(session)
            sync_repo.upsert_spot_asset_precision_evidence_sync(
                session,
                SpotAssetPrecisionEvidenceUpsertRow(
                    exchange="walutomat",
                    asset=asset,
                    balance_decimals=None,
                    balance_source=None,
                    balance_version=None,
                    balance_observed_at=None,
                    fee_decimals=4,
                    fee_source="fee",
                    fee_version="v2",
                    fee_observed_at=_T1,
                    session_id="s",
                    sequence_id=2,
                    timestamp=_T1,
                ),
            )
            session.commit()

    async def _sync_task() -> None:
        await barrier.wait()
        await asyncio.to_thread(_sync_ev)

    await asyncio.wait_for(asyncio.gather(_async_ev(), _sync_task()), timeout=_WAIT_TIMEOUT_S)


@pytest.mark.asyncio
async def test_pg18_race_portfolio_pnl_point(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent PortfolioPnlPoint upsert conflicts resolve deterministically under SAVEPOINT."""
    async_repo, _ = pg_repos
    op_pid = str(uuid4())
    wallet_pid = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_pnl_{uuid4().hex[:6]}",
                session_id="seed",
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Wallet(
                public_id=wallet_pid,
                label=f"w_pnl_{uuid4().hex[:6]}",
                is_paper=True,
                session_id="seed",
                sequence_id=2,
                timestamp=_T0,
            )
        )
        await session.commit()

    epoch = str(uuid4())
    spec_a = Scd2UpsertSpec(
        model=PortfolioPnlPoint,
        match_filters=[
            PortfolioPnlPoint.wallet_public_id == wallet_pid,
            PortfolioPnlPoint.mode == "paper",
            PortfolioPnlPoint.point_time == _T1,
            PortfolioPnlPoint.valuation_ccy == "USD",
        ],
        bus_time=_T1,
        decide=lambda _r: {
            "wallet_public_id": wallet_pid,
            "mode": "paper",
            "point_time": _T1,
            "valuation_ccy": "USD",
            "equity_epoch_id": epoch,
            "total_equity": 100.0,
            "cash_balance": 100.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "source_timestamp": _T1,
            "calc_version": 1,
            "session_id": "a",
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=PortfolioPnlPoint,
        match_filters=spec_a.match_filters,
        bus_time=_T1,
        decide=lambda _r: {
            "wallet_public_id": wallet_pid,
            "mode": "paper",
            "point_time": _T1,
            "valuation_ccy": "USD",
            "equity_epoch_id": epoch,
            "total_equity": 101.0,
            "cash_balance": 101.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "source_timestamp": _T1,
            "calc_version": 1,
            "session_id": "b",
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, spec_a, spec_b)
    assert "created" in outcomes


@pytest.mark.asyncio
async def test_pg18_race_trade_projection_checkpoint(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent upsert_checkpoint calls on PG18 serialize through SAVEPOINT retry."""
    async_repo, _ = pg_repos
    op_pid = str(uuid4())
    wallet_pid = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_cp_{uuid4().hex[:6]}",
                session_id="seed",
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Wallet(
                public_id=wallet_pid,
                label=f"w_cp_{uuid4().hex[:6]}",
                is_paper=True,
                session_id="seed",
                sequence_id=2,
                timestamp=_T0,
            )
        )
        await session.commit()

    shard = f"shard-{uuid4().hex[:8]}"
    barrier = asyncio.Barrier(2)

    async def _cp(seq: int) -> int:
        await barrier.wait()
        return await async_repo.upsert_checkpoint(
            CheckpointUpsertRow(
                shard_key=shard,
                position_qty=float(seq),
                entry_price=10.0,
                position_opened_at=_T0,
                cash=1000.0,
                peak_equity=1000.0,
                realized_pnl=0.0,
                turnover=10.0,
                last_venue_event_id=seq,
                last_venue_event_at=_T1,
                open_command_ids="[]",
                seen_exec_ids="[]",
                checkpoint_at=_T1,
                session_id=f"cp-{seq}",
                sequence_id=seq,
                bus_time=_T1,
                wallet_public_id=wallet_pid,
                operator_public_id=op_pid,
            )
        )

    ids = await asyncio.wait_for(asyncio.gather(_cp(1), _cp(2)), timeout=_WAIT_TIMEOUT_S)
    assert all(row_id > 0 for row_id in ids)


@pytest.mark.asyncio
async def test_pg18_race_underlying_asset(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent upsert_underlying_asset calls resolve to one underlying asset."""
    async_repo, _ = pg_repos
    ticker = f"UND_{uuid4().hex[:6]}"
    barrier = asyncio.Barrier(2)

    async def _asset(seq: int) -> tuple[str, str]:
        await barrier.wait()
        return await async_repo.upsert_underlying_asset(
            ticker=ticker,
            name={"en": "Test Index"},
            asset_class="index",
            session_id=f"ua-{seq}",
            sequence_id=seq,
            timestamp=_T1,
        )

    res_a, res_b = await asyncio.wait_for(
        asyncio.gather(_asset(1), _asset(2)), timeout=_WAIT_TIMEOUT_S
    )
    assert res_a[0] == res_b[0]


@pytest.mark.asyncio
async def test_pg18_race_instrument_underlying_mapping(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent upsert_instrument_underlying_mapping calls resolve via SAVEPOINT."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"MAP-{uuid4().hex[:6]}-USD")
    underlying_pid, _ = await async_repo.upsert_underlying_asset(
        ticker=f"UMAP_{uuid4().hex[:6]}",
        name={"en": "Mapped Index"},
        asset_class="index",
        session_id="seed-ua",
        sequence_id=1,
        timestamp=_T0,
    )
    barrier = asyncio.Barrier(2)

    async def _map(seq: int) -> str:
        await barrier.wait()
        return await async_repo.upsert_instrument_underlying_mapping(
            instrument_public_id=inst_pid,
            underlying_public_id=underlying_pid,
            relationship_type="spot",
            session_id=f"um-{seq}",
            sequence_id=seq,
            timestamp=_T1,
        )

    outcomes = await asyncio.wait_for(asyncio.gather(_map(1), _map(2)), timeout=_WAIT_TIMEOUT_S)
    assert "created" in outcomes
