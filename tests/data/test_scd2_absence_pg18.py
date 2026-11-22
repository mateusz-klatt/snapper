"""Opt-in live PostgreSQL 18 deterministic two-session SCD2 race suite for PR-1."""

import asyncio
import os
import threading
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any
from typing import cast
from uuid import NAMESPACE_URL
from uuid import uuid4
from uuid import uuid5

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as SyncSession

from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import MarketSnapshot
from snapper.data.models import Operator
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Setting
from snapper.data.models import SpotAssetPrecisionEvidence
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import UnderlyingAsset
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.models import Wallet
from snapper.data.repository import SYMBOL_UPDATER_LOCK_IDENTITY
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


def _session_uuid(label: str) -> str:
    """Return a deterministic valid UUID string for ``UUIDColumn`` provenance."""
    return str(uuid5(NAMESPACE_URL, f"snapper-pg18:{label}"))


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
                await admin.exec_driver_sql(
                    f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
                )
        await maintenance.engine.dispose()


async def _wait_for_blocked_lock(repo: SQLAlchemyRepository, relation: str) -> None:
    """Wait until PostgreSQL reports a waiting lock (including transactionid waits)."""
    deadline = asyncio.get_running_loop().time() + _WAIT_TIMEOUT_S
    query = text(
        "SELECT count(*) "
        "FROM pg_stat_activity a "
        "JOIN pg_locks l ON l.pid = a.pid "
        "LEFT JOIN pg_class c ON c.oid = l.relation "
        "WHERE a.datname = current_database() "
        "AND a.pid <> pg_backend_pid() "
        "AND a.wait_event_type = 'Lock' "
        "AND NOT l.granted "
        "AND (c.relname = :rel OR l.locktype IN ('transactionid', 'tuple'))"
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
    seed_session_id = _session_uuid("pg-seed")
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
                session_id=seed_session_id,
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id=instrument_pid,
                symbol_public_id=symbol_pid,
                exchange="kraken",
                timestamp=_T0,
                session_id=seed_session_id,
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
    passed_sessions: set[int] = set()
    original_flush = AsyncSession.flush

    async def _coordinated_flush(session: AsyncSession) -> None:
        session_key = id(session)
        if session_key not in passed_sessions and any(
            isinstance(obj, Setting) and obj.key == key for obj in session.new
        ):
            passed_sessions.add(session_key)
            await select_barrier.wait()
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _coordinated_flush)

    async def _worker(tag: str) -> str:
        worker_sid = _session_uuid(f"cold-{tag}")
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
                        "session_id": worker_sid,
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
    monkeypatch: pytest.MonkeyPatch,
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
                session_id=_session_uuid("seed"),
                sequence_id=1,
                timestamp=_T0,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()

    a_locked = asyncio.Event()
    b_finished = False

    async def _session_a() -> None:
        async with async_repo.session() as session:
            await begin_scd2_write(session)
            await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == key],
                    bus_time=_T1,
                    decide=lambda _r: {
                        "key": key,
                        "value": "v2",
                        "category": "race",
                        "description": None,
                        "is_encrypted": False,
                        "session_id": _session_uuid("epq-a"),
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
            await begin_scd2_write(session)
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
                        "session_id": _session_uuid("epq-b"),
                        "sequence_id": 3,
                    },
                ),
            )
            await session.commit()
            assert outcome.row is not None
            assert outcome.row.public_id == initial_pid
            assert outcome.row.timestamp == _T1
            b_finished = True
            return outcome.outcome

    _, b_outcome = await asyncio.wait_for(
        asyncio.gather(_session_a(), _session_b()), timeout=_WAIT_TIMEOUT_S
    )
    assert b_finished is True
    assert b_outcome == "updated"
    await _run_pg18_clock_skew_subphase(async_repo, monkeypatch)


async def _run_pg18_clock_skew_subphase(
    async_repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise cold-start clock skew clamping deterministically on PG18."""
    skew_key = f"pg-skew-{uuid4().hex}"
    new_flushed = asyncio.Event()
    old_blocked = False
    original_flush = AsyncSession.flush

    async def _skew_flush(session: AsyncSession) -> None:
        nonlocal old_blocked
        pending = [obj for obj in session.new if isinstance(obj, Setting) and obj.key == skew_key]
        if pending and pending[0].value == "old" and not old_blocked:
            old_blocked = True
            await asyncio.wait_for(new_flushed.wait(), timeout=_WAIT_TIMEOUT_S)
        await original_flush(session)
        if pending and pending[0].value == "new":
            new_flushed.set()

    monkeypatch.setattr(AsyncSession, "flush", _skew_flush)

    async def _skew_writer(tag: str, bus_time: datetime, seq: int) -> tuple[str, datetime, str]:
        async with async_repo.session() as session:
            await begin_scd2_write(session)
            outcome = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == skew_key],
                    bus_time=bus_time,
                    decide=lambda _existing: {
                        "key": skew_key,
                        "value": tag,
                        "category": "race",
                        "description": None,
                        "is_encrypted": False,
                        "session_id": _session_uuid(f"skew-{tag}"),
                        "sequence_id": seq,
                    },
                ),
            )
            if tag == "new":
                await _wait_for_blocked_lock(async_repo, "settings")
            await session.commit()
            assert outcome.row is not None
            return outcome.outcome, outcome.row.timestamp, outcome.row.public_id

    res_new, res_old = await asyncio.wait_for(
        asyncio.gather(_skew_writer("new", _T2, 1), _skew_writer("old", _T1, 2)),
        timeout=_WAIT_TIMEOUT_S,
    )
    monkeypatch.undo()
    assert res_new[:2] == ("created", _T2)
    assert res_old[:2] == ("updated", _T2)
    assert res_new[2] == res_old[2]


def _install_async_flush_barrier(
    monkeypatch: pytest.MonkeyPatch,
    model_cls: type[Base],
) -> None:
    """Coordinate two async writers inside ``AsyncSession.flush`` before INSERT."""
    barrier = asyncio.Barrier(2)
    passed_sessions: set[int] = set()
    original_flush = AsyncSession.flush

    async def _coordinated(session: AsyncSession) -> None:
        session_key = id(session)
        if session_key not in passed_sessions and any(
            isinstance(obj, model_cls) for obj in session.new
        ):
            passed_sessions.add(session_key)
            await barrier.wait()
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _coordinated)


def _install_sync_flush_barrier(
    monkeypatch: pytest.MonkeyPatch,
    model_cls: type[Base],
) -> None:
    """Coordinate two threaded sync writers inside ``SyncSession.flush`` before INSERT."""
    barrier = threading.Barrier(2, timeout=_WAIT_TIMEOUT_S)
    passed_sessions: set[int] = set()
    original_flush = SyncSession.flush

    def _coordinated(session: SyncSession, objects: Any = None) -> None:
        session_key = id(session)
        if session_key not in passed_sessions and any(
            isinstance(obj, model_cls) for obj in session.new
        ):
            passed_sessions.add(session_key)
            barrier.wait()
        original_flush(session, objects)

    monkeypatch.setattr(SyncSession, "flush", _coordinated)


def _install_mixed_flush_barrier(
    monkeypatch: pytest.MonkeyPatch,
    model_cls: type[Base],
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Coordinate one async session and one threaded sync session at first ``flush``."""
    async_ready = asyncio.Event()
    sync_ready = asyncio.Event()
    sync_go = threading.Event()
    passed_async: set[int] = set()
    passed_sync: set[int] = set()
    original_async_flush = AsyncSession.flush
    original_sync_flush = SyncSession.flush

    async def _coordinated_async(session: AsyncSession) -> None:
        session_key = id(session)
        if session_key not in passed_async and any(
            isinstance(obj, model_cls) for obj in session.new
        ):
            passed_async.add(session_key)
            async_ready.set()
            await asyncio.wait_for(sync_ready.wait(), timeout=_WAIT_TIMEOUT_S)
            sync_go.set()
        await original_async_flush(session)

    def _coordinated_sync(session: SyncSession, objects: Any = None) -> None:
        session_key = id(session)
        if session_key not in passed_sync and any(
            isinstance(obj, model_cls) for obj in session.new
        ):
            passed_sync.add(session_key)
            loop.call_soon_threadsafe(sync_ready.set)
            if not sync_go.wait(timeout=_WAIT_TIMEOUT_S):
                raise TimeoutError("Timed out waiting for async writer at flush barrier")
        original_sync_flush(session, objects)

    monkeypatch.setattr(AsyncSession, "flush", _coordinated_async)
    monkeypatch.setattr(SyncSession, "flush", _coordinated_sync)


async def _assert_single_active[ModelT: Base](
    repo: SQLAlchemyRepository,
    model_cls: type[ModelT],
    *filters: Any,
) -> list[ModelT]:
    """Assert exactly one open ``KNOWN_TO_MAX`` version exists and return it."""
    known_to_col = cast(Any, model_cls).known_to
    async with repo.session() as session:
        rows = (
            await session.scalars(select(model_cls).where(*filters, known_to_col == KNOWN_TO_MAX))
        ).all()
    assert len(rows) == 1
    return list(rows)


@pytest.mark.asyncio
async def test_pg18_race_instrument_spec(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """InstrumentSpec absence race across concurrent writers converges to 1 active row."""
    async_repo, sync_repo = pg_repos
    _, instrument_pid = await _seed_symbol_instrument_pg(async_repo, f"SPEC-{uuid4().hex[:6]}-USD")
    _install_mixed_flush_barrier(monkeypatch, InstrumentSpec, asyncio.get_running_loop())

    async def _async_write() -> int:
        return await async_repo.revise_instrument_spec(
            instrument_public_id=instrument_pid,
            session_id=_session_uuid("async-spec"),
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
                _session_uuid("sync-spec"),
                1,
                _T1,
                InstrumentSpecInput(instrument_kind="spot"),
            )
            session.commit()
            return outcome

    res_async, res_sync = await asyncio.wait_for(
        asyncio.gather(_async_write(), asyncio.to_thread(_sync_write)), timeout=_WAIT_TIMEOUT_S
    )
    assert res_async > 0 and res_sync in {"created", "updated", "unchanged"}
    await _assert_single_active(
        async_repo, InstrumentSpec, InstrumentSpec.instrument_public_id == instrument_pid
    )


@pytest.mark.asyncio
async def test_pg18_race_symbol(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two cross-exchange writers upserting the same Symbol resolve without duplicates."""
    async_repo, sync_repo = pg_repos
    native = f"EUR-PLN-{uuid4().hex[:6]}"
    _install_sync_flush_barrier(monkeypatch, Symbol)

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
                            "session_id": _session_uuid(f"sym-{exchange}"),
                            "sequence_id": 1,
                        }
                    ),
                ),
            )
            session.commit()
            return res.outcome

    outcomes = await asyncio.wait_for(
        asyncio.gather(
            asyncio.to_thread(_run_worker, "kraken"),
            asyncio.to_thread(_run_worker, "walutomat"),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert sorted(outcomes) == ["created", "unchanged"]
    await _assert_single_active(async_repo, Symbol, Symbol.native_symbol == native)


@pytest.mark.asyncio
async def test_pg18_race_symbol_alias_and_secondary_unique_index(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SymbolAlias handles primary absence race and re-raises cold and update secondary collisions."""
    async_repo, _ = pg_repos
    sym_a, _ = await _seed_symbol_instrument_pg(async_repo, f"AL1-{uuid4().hex[:6]}-USD")
    sym_b, _ = await _seed_symbol_instrument_pg(async_repo, f"AL2-{uuid4().hex[:6]}-USD")
    _install_async_flush_barrier(monkeypatch, SymbolAlias)

    async def _alias_worker(tag: str) -> str:
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
                            "session_id": _session_uuid(f"alias-{tag}"),
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
    monkeypatch.undo()
    assert sorted(outcomes) == ["created", "unchanged"]
    await _assert_single_active(
        async_repo,
        SymbolAlias,
        SymbolAlias.symbol_public_id == sym_a,
        SymbolAlias.exchange == "kraken",
        SymbolAlias.channel == "ws",
    )

    with pytest.raises(IntegrityError):
        async with async_repo.session() as session:
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
                        "session_id": _session_uuid("alias-sec"),
                        "sequence_id": 2,
                    },
                ),
            )

    async with async_repo.session() as session:
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=SymbolAlias,
                match_filters=[
                    SymbolAlias.symbol_public_id == sym_b,
                    SymbolAlias.exchange == "kraken",
                    SymbolAlias.channel == "ws",
                ],
                bus_time=_T1,
                decide=lambda _r: {
                    "symbol_public_id": sym_b,
                    "exchange": "kraken",
                    "channel": "ws",
                    "exchange_symbol": "ALT/USD",
                    "created_at": _T1,
                    "session_id": _session_uuid("alias-b-init"),
                    "sequence_id": 1,
                },
            ),
        )
        await session.commit()

    with pytest.raises(IntegrityError):
        async with async_repo.session() as session:
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
                    decide=lambda existing: {
                        "symbol_public_id": sym_b,
                        "exchange": "kraken",
                        "channel": "ws",
                        "exchange_symbol": "RACE/USD",
                        "created_at": existing.created_at if existing is not None else _T2,
                        "session_id": _session_uuid("alias-b-upd-sec"),
                        "sequence_id": 2,
                    },
                ),
            )
    rows_b = await _assert_single_active(
        async_repo,
        SymbolAlias,
        SymbolAlias.symbol_public_id == sym_b,
        SymbolAlias.exchange == "kraken",
        SymbolAlias.channel == "ws",
    )
    assert rows_b[0].exchange_symbol == "ALT/USD"


async def _run_concurrent_simple_upsert[ModelT: Base](
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    build_spec_a: Scd2UpsertSpec[ModelT],
    build_spec_b: Scd2UpsertSpec[ModelT],
) -> tuple[str, str]:
    """Execute two flush-barrier-synchronized ``upsert_scd2`` calls and verify single active."""
    _install_async_flush_barrier(monkeypatch, build_spec_a.model)

    async def _one(spec: Scd2UpsertSpec[ModelT]) -> str:
        async with repo.session() as session:
            outcome = await upsert_scd2(session, spec)
            await session.commit()
            return outcome.outcome

    first, second = await asyncio.wait_for(
        asyncio.gather(_one(build_spec_a), _one(build_spec_b)), timeout=_WAIT_TIMEOUT_S
    )
    await _assert_single_active(repo, build_spec_a.model, *build_spec_a.match_filters)
    return first, second


@pytest.mark.asyncio
async def test_pg18_race_symbol_exchange_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
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
            "session_id": _session_uuid("sec-a"),
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
            "session_id": _session_uuid("sec-b"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


@pytest.mark.asyncio
async def test_pg18_race_symbol_market_data_channel_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
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
            "session_id": _session_uuid("mdc-pub"),
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
            "session_id": _session_uuid("mdc-upd"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


@pytest.mark.asyncio
async def test_pg18_race_instrument_order_capability(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
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
            "supported_order_types": ["market", "limit"],
            "supports_post_only": True,
            "supports_reduce_only": False,
            "session_id": _session_uuid("ioc-a"),
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
            "supported_order_types": ["market", "limit"],
            "supports_post_only": True,
            "supports_reduce_only": True,
            "session_id": _session_uuid("ioc-b"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


@pytest.mark.asyncio
async def test_pg18_race_setting(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
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
            "session_id": _session_uuid("set-a"),
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
            "session_id": _session_uuid("set-b"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


def _snapshot_row(instrument_pid: str, seq: int, bid: float) -> MarketSnapshotUpsertRow:
    """Build a valid ``MarketSnapshotUpsertRow`` for PG18 snapshot race tests."""
    return MarketSnapshotUpsertRow(
        instrument_public_id=instrument_pid,
        timestamp=_T1,
        session_id=_session_uuid(f"snap-{seq}"),
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent single-row MarketSnapshot writers resolve via SAVEPOINT cleanly."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"SNP1-{uuid4().hex[:6]}-USD")
    _install_async_flush_barrier(monkeypatch, MarketSnapshot)

    async def _single(seq: int, bid: float) -> None:
        async with async_repo.session() as session:
            await async_repo._upsert_snapshot_row(session, _snapshot_row(inst_pid, seq, bid))
            await session.commit()

    await asyncio.wait_for(
        asyncio.gather(_single(1, 50.0), _single(2, 51.0)), timeout=_WAIT_TIMEOUT_S
    )
    await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == inst_pid
    )


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_bulk(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent bulk upsert_market_snapshots collectors fall back per-row cleanly."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"SNPB-{uuid4().hex[:6]}-USD")
    _install_async_flush_barrier(monkeypatch, MarketSnapshot)

    async def _bulk(seq: int, bid: float) -> int:
        return await async_repo.upsert_market_snapshots([_snapshot_row(inst_pid, seq, bid)])

    counts = await asyncio.wait_for(
        asyncio.gather(_bulk(1, 60.0), _bulk(2, 61.0)), timeout=_WAIT_TIMEOUT_S
    )
    assert counts == [1, 1]
    await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == inst_pid
    )


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_mixed_shape_deadlock_free(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent mixed-shape ``[A,B,B]`` vs ``[B,A,A]`` batches never 40P01 deadlock."""
    async_repo, _ = pg_repos
    _, warm_1 = await _seed_symbol_instrument_pg(async_repo, f"SNPW1-{uuid4().hex[:6]}-USD")
    _, warm_2 = await _seed_symbol_instrument_pg(async_repo, f"SNPW2-{uuid4().hex[:6]}-USD")
    warm_a, warm_b = sorted((warm_1, warm_2))
    assert (
        await async_repo.upsert_market_snapshots(
            [
                {**_snapshot_row(warm_a, 1, 10.0), "timestamp": _T0},
                {**_snapshot_row(warm_b, 2, 20.0), "timestamp": _T0},
            ]
        )
        == 2
    )

    _, cold_1 = await _seed_symbol_instrument_pg(async_repo, f"SNPC1-{uuid4().hex[:6]}-USD")
    _, cold_2 = await _seed_symbol_instrument_pg(async_repo, f"SNPC2-{uuid4().hex[:6]}-USD")
    cold_a, cold_b = sorted((cold_1, cold_2))

    _install_async_flush_barrier(monkeypatch, MarketSnapshot)
    cold_counts = await asyncio.wait_for(
        asyncio.gather(
            async_repo.upsert_market_snapshots(
                [
                    {**_snapshot_row(cold_a, 10, 30.0), "timestamp": _T1},
                    {**_snapshot_row(cold_b, 11, 40.0), "timestamp": _T1},
                    {**_snapshot_row(cold_b, 12, 41.0), "timestamp": _T1},
                ]
            ),
            async_repo.upsert_market_snapshots(
                [
                    {**_snapshot_row(cold_b, 20, 42.0), "timestamp": _T1},
                    {**_snapshot_row(cold_a, 21, 32.0), "timestamp": _T1},
                    {**_snapshot_row(cold_a, 22, 33.0), "timestamp": _T1},
                ]
            ),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    monkeypatch.undo()
    assert cold_counts == [3, 3]

    warm_counts = await asyncio.wait_for(
        asyncio.gather(
            async_repo.upsert_market_snapshots(
                [
                    {**_snapshot_row(warm_a, 30, 11.0), "timestamp": _T1},
                    {**_snapshot_row(warm_b, 31, 21.0), "timestamp": _T1},
                    {**_snapshot_row(warm_b, 32, 22.0), "timestamp": _T2},
                ]
            ),
            async_repo.upsert_market_snapshots(
                [
                    {**_snapshot_row(warm_b, 40, 23.0), "timestamp": _T1 + timedelta(seconds=1)},
                    {**_snapshot_row(warm_a, 41, 13.0), "timestamp": _T1 + timedelta(seconds=1)},
                    {**_snapshot_row(warm_a, 42, 14.0), "timestamp": _T2 + timedelta(seconds=1)},
                ]
            ),
            return_exceptions=True,
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert any(res == 3 for res in warm_counts)
    for res in warm_counts:
        assert res == 3 or isinstance(res, TemporalVersionConflictError)
    for ipid in (cold_a, cold_b, warm_a, warm_b):
        await _assert_single_active(
            async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == ipid
        )


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_mixed_shape_skewed_active_timestamps(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Mixed batches remain deadlock-free with skewed active timestamps."""
    async_repo, _ = pg_repos
    _, inst_1 = await _seed_symbol_instrument_pg(async_repo, f"SNPSK1-{uuid4().hex[:6]}-USD")
    _, inst_2 = await _seed_symbol_instrument_pg(async_repo, f"SNPSK2-{uuid4().hex[:6]}-USD")
    skew_a, skew_b = sorted((inst_1, inst_2))
    assert (
        await async_repo.upsert_market_snapshots(
            [
                {**_snapshot_row(skew_a, 1, 10.0), "timestamp": _T2},
                {**_snapshot_row(skew_b, 2, 20.0), "timestamp": _T0},
            ]
        )
        == 2
    )

    start_barrier = asyncio.Barrier(2)

    async def _run_batch(batch: list[MarketSnapshotUpsertRow]) -> int:
        await start_barrier.wait()
        return await async_repo.upsert_market_snapshots(batch)

    counts = await asyncio.wait_for(
        asyncio.gather(
            _run_batch(
                [
                    {**_snapshot_row(skew_a, 3, 11.0), "timestamp": _T1},
                    {**_snapshot_row(skew_b, 4, 21.0), "timestamp": _T1},
                    {**_snapshot_row(skew_b, 5, 22.0), "timestamp": _T1},
                ]
            ),
            _run_batch(
                [
                    {**_snapshot_row(skew_b, 6, 23.0), "timestamp": _T1},
                    {**_snapshot_row(skew_a, 7, 12.0), "timestamp": _T1},
                    {**_snapshot_row(skew_a, 8, 13.0), "timestamp": _T1},
                ]
            ),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert counts == [3, 3]
    active_a = await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == skew_a
    )
    active_b = await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == skew_b
    )
    assert active_a[0].timestamp == _T2
    assert active_b[0].timestamp == _T1


@pytest.mark.asyncio
async def test_pg18_race_market_snapshot_all_unique_reverse_order_skewed(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """All-unique ``[A,B]`` and ``[B,A]`` writes cannot form a 40P01 lock cycle."""
    async_repo, _ = pg_repos
    _, inst_1 = await _seed_symbol_instrument_pg(async_repo, f"SNPU1-{uuid4().hex[:6]}-USD")
    _, inst_2 = await _seed_symbol_instrument_pg(async_repo, f"SNPU2-{uuid4().hex[:6]}-USD")
    inst_a, inst_b = sorted((inst_1, inst_2))
    assert (
        await async_repo.upsert_market_snapshots(
            [
                {**_snapshot_row(inst_a, 1, 10.0), "timestamp": _T2},
                {**_snapshot_row(inst_b, 2, 20.0), "timestamp": _T0},
            ]
        )
        == 2
    )

    older_than_b = _T0 - timedelta(seconds=10)
    newer_than_a = _T2 + timedelta(seconds=10)
    start_barrier = asyncio.Barrier(2)

    async def _run_batch(batch: list[MarketSnapshotUpsertRow]) -> int:
        await start_barrier.wait()
        return await async_repo.upsert_market_snapshots(batch)

    counts = await asyncio.wait_for(
        asyncio.gather(
            _run_batch(
                [
                    {**_snapshot_row(inst_a, 3, 11.0), "timestamp": _T1},
                    {**_snapshot_row(inst_b, 4, 21.0), "timestamp": _T1},
                ]
            ),
            _run_batch(
                [
                    {**_snapshot_row(inst_b, 5, 22.0), "timestamp": older_than_b},
                    {**_snapshot_row(inst_a, 6, 12.0), "timestamp": newer_than_a},
                ]
            ),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert counts == [2, 2]
    active_a = await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == inst_a
    )
    active_b = await _assert_single_active(
        async_repo, MarketSnapshot, MarketSnapshot.instrument_public_id == inst_b
    )
    assert active_a[0].timestamp == newer_than_a
    assert active_b[0].timestamp == _T1


@pytest.mark.asyncio
async def test_pg18_race_instrument(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrument absence race succeeds once and concurrent lock-wake converges cleanly."""
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
                session_id=_session_uuid("inst-seed"),
                sequence_id=1,
            )
        )
        await session.commit()

    _install_async_flush_barrier(monkeypatch, Instrument)

    async def _ensure(seq: int) -> str:
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
                            "session_id": _session_uuid(f"inst-{seq}"),
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
    await _assert_single_active(
        async_repo,
        Instrument,
        Instrument.symbol_public_id == sym_pid,
        Instrument.exchange == "kraken",
    )


@pytest.mark.asyncio
async def test_pg18_revise_instrument_lock_wake_and_reciprocal_rekey(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """``revise_instrument`` refuses EPQ lock-wake races and serializes reciprocal rekeys."""
    async_repo, _ = pg_repos
    sym_1, inst_1 = await _seed_symbol_instrument_pg(async_repo, f"REV1-{uuid4().hex[:6]}-USD")
    sym_2, _ = await _seed_symbol_instrument_pg(async_repo, f"REV2-{uuid4().hex[:6]}-USD")

    a_locked = asyncio.Event()

    async def _reviser_a() -> int:
        async with async_repo.session() as session:
            await begin_scd2_write(session)
            inst, _conflict = await async_repo._load_revise_instrument_locked_rows(
                session, inst_1, sym_1, "binance", _T1
            )
            new_row = await close_and_insert_existing(
                session,
                Instrument,
                [Instrument.public_id == inst_1],
                {
                    "symbol_public_id": sym_1,
                    "exchange": "binance",
                    "requires_ai_review": inst.requires_ai_review,
                    "source_exchange": None,
                    "session_id": _session_uuid("rev-a"),
                    "sequence_id": 1,
                },
                _T1,
            )
            a_locked.set()
            await _wait_for_blocked_lock(async_repo, "instruments")
            await session.commit()
            return int(new_row.id)

    async def _reviser_b() -> str:
        await asyncio.wait_for(a_locked.wait(), timeout=_WAIT_TIMEOUT_S)
        try:
            await async_repo.revise_instrument(
                instrument_public_id=inst_1,
                symbol_public_id=sym_1,
                exchange="coinbase",
                session_id=_session_uuid("rev-b"),
                sequence_id=2,
                timestamp=_T2,
            )
            return "updated"
        except TemporalVersionConflictError:
            return "conflict"

    _, status = await asyncio.wait_for(
        asyncio.gather(_reviser_a(), _reviser_b()), timeout=_WAIT_TIMEOUT_S
    )
    assert status == "conflict"
    active = await _assert_single_active(async_repo, Instrument, Instrument.public_id == inst_1)
    assert active[0].exchange == "binance"

    _, inst_2 = await _seed_symbol_instrument_pg(async_repo, f"REV3-{uuid4().hex[:6]}-USD")
    results = await asyncio.wait_for(
        asyncio.gather(
            async_repo.revise_instrument(inst_1, sym_2, "kraken", _session_uuid("swap-1"), 3, _T2),
            async_repo.revise_instrument(inst_2, sym_1, "binance", _session_uuid("swap-2"), 3, _T2),
            return_exceptions=True,
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert any(isinstance(res, ValueError) for res in results)


def _wait_for_advisory_wait_sync(sync_repo: DatabaseRepository) -> None:
    """Poll ``pg_locks`` synchronously until a backend waits on an advisory lock."""
    deadline = threading.Event()
    elapsed = 0.0
    query = text(
        "SELECT count(*) "
        "FROM pg_stat_activity a "
        "JOIN pg_locks l ON l.pid = a.pid "
        "WHERE a.datname = current_database() "
        "AND a.pid <> pg_backend_pid() "
        "AND l.locktype = 'advisory' "
        "AND NOT l.granted"
    )
    while elapsed < _WAIT_TIMEOUT_S:
        with sync_repo.get_session() as probe:
            blocked = int(probe.execute(query).scalar_one())
            if blocked > 0:
                return
        deadline.wait(0.01)
        elapsed += 0.01
    raise TimeoutError(
        "Timed out waiting for advisory lock contention on SYMBOL_UPDATER_LOCK_IDENTITY"
    )


@pytest.mark.asyncio
async def test_pg18_revise_instrument_serializes_with_symbol_updater_advisory_lock(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """``revise_instrument`` serializes with symbol updaters via the shared catalog advisory lock."""
    async_repo, sync_repo = pg_repos
    sym_1, inst_1 = await _seed_symbol_instrument_pg(async_repo, f"UPD1-{uuid4().hex[:6]}-USD")
    sym_2, inst_2 = await _seed_symbol_instrument_pg(async_repo, f"UPD2-{uuid4().hex[:6]}-USD")
    sym_3 = str(uuid4())
    async with async_repo.session() as session:
        session.add(
            Symbol(
                public_id=sym_3,
                native_symbol=f"UPD3-{uuid4().hex[:6]}-USD",
                base="UPD3",
                quote="USD",
                asset_type="crypto",
                created_at=_T0,
                timestamp=_T0,
                session_id=_session_uuid("sym3"),
                sequence_id=1,
            )
        )
        await session.commit()

    assert SYMBOL_UPDATER_LOCK_IDENTITY
    updater_locked = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _run_catalog_updater() -> str:
        with sync_repo.get_session() as session:
            DatabaseRepository.acquire_symbol_updater_lock_sync(session)
            session.execute(
                select(Instrument)
                .where(Instrument.public_id == inst_2, Instrument.known_to == KNOWN_TO_MAX)
                .with_for_update()
            ).scalar_one()
            loop.call_soon_threadsafe(updater_locked.set)
            _wait_for_advisory_wait_sync(sync_repo)
            ensured_pid = SymbolUpdaterService._ensure_instrument_identity(
                session,
                sym_1,
                "kraken",
                _T1,
                _session_uuid("updater-sync"),
                1,
            )
            session.commit()
            return ensured_pid

    async def _run_rekey() -> int:
        await asyncio.wait_for(updater_locked.wait(), timeout=_WAIT_TIMEOUT_S)
        return await async_repo.revise_instrument(
            inst_1,
            sym_3,
            "kraken",
            _session_uuid("rekey-async"),
            2,
            _T2,
        )

    ensured_pid, revised_id = await asyncio.wait_for(
        asyncio.gather(asyncio.to_thread(_run_catalog_updater), _run_rekey()),
        timeout=_WAIT_TIMEOUT_S,
    )
    assert ensured_pid == inst_1
    assert revised_id > 0
    active = await _assert_single_active(async_repo, Instrument, Instrument.public_id == inst_1)
    assert active[0].symbol_public_id == sym_3


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
                created_at=_T0,
                session_id=_session_uuid("user-seed"),
                sequence_id=1,
                timestamp=_T0,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()

    a_locked = asyncio.Event()

    async def _user_a() -> None:
        async with async_repo.session() as session:
            await begin_scd2_write(session)
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
                    "created_at": _T0,
                    "session_id": _session_uuid("user-a"),
                    "sequence_id": 2,
                },
                _T1,
            )
            a_locked.set()
            await _wait_for_blocked_lock(async_repo, "users")
            await session.commit()

    async def _user_b() -> str:
        await asyncio.wait_for(a_locked.wait(), timeout=_WAIT_TIMEOUT_S)
        async with async_repo.session() as session:
            await begin_scd2_write(session)
            try:
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
                        "created_at": _T0,
                        "session_id": _session_uuid("user-b"),
                        "sequence_id": 3,
                    },
                    _T2,
                )
                await session.commit()
                return "updated"
            except TemporalVersionConflictError:
                return "conflict"

    _, b_status = await asyncio.wait_for(
        asyncio.gather(_user_a(), _user_b()), timeout=_WAIT_TIMEOUT_S
    )
    assert b_status == "conflict"
    active = await _assert_single_active(async_repo, User, User.username == username)
    assert active[0].role == "operator"


@pytest.mark.asyncio
async def test_pg18_race_user_trading_caps(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
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
                created_at=_T0,
                session_id=_session_uuid("caps-seed"),
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
            "max_daily_notional_usd": Decimal("100"),
            "session_id": _session_uuid("caps-a"),
            "sequence_id": 1,
        },
    )
    spec_b = Scd2UpsertSpec(
        model=UserTradingCaps,
        match_filters=[UserTradingCaps.user_public_id == user_pid],
        bus_time=_T1,
        decide=lambda _r: {
            "user_public_id": user_pid,
            "max_daily_notional_usd": Decimal("200"),
            "session_id": _session_uuid("caps-b"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


@pytest.mark.asyncio
async def test_pg18_race_user_operator_membership_detach(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
) -> None:
    """Concurrent _commit_desk_detach refuses double-detach cleanly."""
    async_repo, _ = pg_repos
    user_pid = str(uuid4())
    op_pid = str(uuid4())
    username = f"mbr_{uuid4().hex[:6]}"
    seed_sid = _session_uuid("mbr-seed")
    async with async_repo.session() as session:
        session.add(
            User(
                public_id=user_pid,
                username=username,
                password_hash="h",
                role="viewer",
                is_active=True,
                created_at=_T0,
                session_id=seed_sid,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_{uuid4().hex[:6]}",
                session_id=seed_sid,
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
                session_id=seed_sid,
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
                    session_id=_session_uuid(f"det-{seq}"),
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
    async with async_repo.session() as session:
        active = (
            await session.scalars(
                select(UserOperatorMembership).where(
                    UserOperatorMembership.user_public_id == user_pid,
                    UserOperatorMembership.known_to == KNOWN_TO_MAX,
                )
            )
        ).all()
        assert active == []


@pytest.mark.asyncio
async def test_pg18_race_spot_asset_precision_evidence(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync and async precision writers merge both planes under advisory lock and SAVEPOINT retry."""
    async_repo, sync_repo = pg_repos
    asset = f"A{uuid4().hex[:5]}"

    def _balance_row(key_asset: str) -> SpotAssetPrecisionEvidenceUpsertRow:
        return SpotAssetPrecisionEvidenceUpsertRow(
            exchange="walutomat",
            asset=key_asset,
            balance_decimals=2,
            balance_source="bal",
            balance_version="v1",
            balance_observed_at=_T1,
            fee_decimals=None,
            fee_source=None,
            fee_version=None,
            fee_observed_at=None,
            session_id=_session_uuid("prec-a"),
            sequence_id=1,
            timestamp=_T1,
        )

    def _fee_row(key_asset: str) -> SpotAssetPrecisionEvidenceUpsertRow:
        return SpotAssetPrecisionEvidenceUpsertRow(
            exchange="walutomat",
            asset=key_asset,
            balance_decimals=None,
            balance_source=None,
            balance_version=None,
            balance_observed_at=None,
            fee_decimals=4,
            fee_source="fee",
            fee_version="v2",
            fee_observed_at=_T1,
            session_id=_session_uuid("prec-s"),
            sequence_id=2,
            timestamp=_T1,
        )

    def _sync_ev(key_asset: str) -> None:
        with sync_repo.get_session() as session:
            begin_scd2_write_sync(session)
            sync_repo.upsert_spot_asset_precision_evidence_sync(session, _fee_row(key_asset))
            session.commit()

    await asyncio.wait_for(
        asyncio.gather(
            async_repo.upsert_spot_asset_precision_evidence(_balance_row(asset)),
            asyncio.to_thread(_sync_ev, asset),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    active = await _assert_single_active(
        async_repo,
        SpotAssetPrecisionEvidence,
        SpotAssetPrecisionEvidence.exchange == "walutomat",
        SpotAssetPrecisionEvidence.asset == asset,
    )
    assert active[0].balance_decimals == 2
    assert active[0].fee_decimals == 4

    retry_asset = f"R{uuid4().hex[:5]}"

    async def _noop_async_lock(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        async_repo,
        "_acquire_spot_asset_precision_evidence_advisory_lock",
        _noop_async_lock,
    )
    monkeypatch.setattr(
        DatabaseRepository,
        "_acquire_spot_asset_precision_evidence_advisory_lock_sync",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    _install_mixed_flush_barrier(
        monkeypatch, SpotAssetPrecisionEvidence, asyncio.get_running_loop()
    )
    await asyncio.wait_for(
        asyncio.gather(
            async_repo.upsert_spot_asset_precision_evidence(_balance_row(retry_asset)),
            asyncio.to_thread(_sync_ev, retry_asset),
        ),
        timeout=_WAIT_TIMEOUT_S,
    )
    monkeypatch.undo()
    retry_active = await _assert_single_active(
        async_repo,
        SpotAssetPrecisionEvidence,
        SpotAssetPrecisionEvidence.exchange == "walutomat",
        SpotAssetPrecisionEvidence.asset == retry_asset,
    )
    assert retry_active[0].balance_decimals == 2
    assert retry_active[0].fee_decimals == 4


@pytest.mark.asyncio
async def test_pg18_race_portfolio_pnl_point(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent PortfolioPnlPoint upsert conflicts resolve deterministically under SAVEPOINT."""
    async_repo, _ = pg_repos
    op_pid = str(uuid4())
    wallet_pid = str(uuid4())
    seed_sid = _session_uuid("pnl-seed")
    async with async_repo.session() as session:
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_pnl_{uuid4().hex[:6]}",
                session_id=seed_sid,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Wallet(
                public_id=wallet_pid,
                label=f"w_pnl_{uuid4().hex[:6]}",
                is_paper=True,
                session_id=seed_sid,
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
            "point_kind": "sample",
            "epoch_public_id": epoch,
            "calc_version": "v1",
            "valuation_status": "complete",
            "realized_pnl": 0.0,
            "fee_pnl": 0.0,
            "accrual_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "external_flow_adjustment": 0.0,
            "cash_usd": 100.0,
            "position_value_usd": 0.0,
            "mark_source": "test",
            "mark_time": _T1,
            "session_id": _session_uuid("pnl-a"),
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
            "point_kind": "sample",
            "epoch_public_id": epoch,
            "calc_version": "v1",
            "valuation_status": "complete",
            "realized_pnl": 1.0,
            "fee_pnl": 0.0,
            "accrual_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "external_flow_adjustment": 0.0,
            "cash_usd": 101.0,
            "position_value_usd": 0.0,
            "mark_source": "test",
            "mark_time": _T1,
            "session_id": _session_uuid("pnl-b"),
            "sequence_id": 2,
        },
    )
    outcomes = await _run_concurrent_simple_upsert(async_repo, monkeypatch, spec_a, spec_b)
    assert sorted(outcomes) == ["created", "updated"]


@pytest.mark.asyncio
async def test_pg18_race_trade_projection_checkpoint(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent upsert_checkpoint calls on PG18 serialize through SAVEPOINT retry."""
    async_repo, _ = pg_repos
    op_pid = str(uuid4())
    wallet_pid = str(uuid4())
    seed_sid = _session_uuid("cp-seed")
    async with async_repo.session() as session:
        session.add(
            Operator(
                public_id=op_pid,
                label=f"op_cp_{uuid4().hex[:6]}",
                session_id=seed_sid,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        session.add(
            Wallet(
                public_id=wallet_pid,
                label=f"w_cp_{uuid4().hex[:6]}",
                is_paper=True,
                session_id=seed_sid,
                sequence_id=2,
                timestamp=_T0,
            )
        )
        await session.commit()

    shard = f"shard-{uuid4().hex[:8]}"
    _install_async_flush_barrier(monkeypatch, TradeProjectionCheckpoint)

    async def _cp(seq: int) -> int:
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
                session_id=_session_uuid(f"cp-{seq}"),
                sequence_id=seq,
                bus_time=_T1,
                wallet_public_id=wallet_pid,
                operator_public_id=op_pid,
            )
        )

    ids = await asyncio.wait_for(asyncio.gather(_cp(1), _cp(2)), timeout=_WAIT_TIMEOUT_S)
    assert all(row_id > 0 for row_id in ids)
    await _assert_single_active(
        async_repo, TradeProjectionCheckpoint, TradeProjectionCheckpoint.shard_key == shard
    )


@pytest.mark.asyncio
async def test_pg18_race_underlying_asset(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent upsert_underlying_asset calls resolve to one underlying asset."""
    async_repo, _ = pg_repos
    ticker = f"UND_{uuid4().hex[:6]}"
    _install_async_flush_barrier(monkeypatch, UnderlyingAsset)

    async def _asset(seq: int) -> tuple[str, str]:
        return await async_repo.upsert_underlying_asset(
            ticker=ticker,
            name={"en": "Test Index"},
            asset_class="index",
            session_id=_session_uuid(f"ua-{seq}"),
            sequence_id=seq,
            timestamp=_T1,
        )

    res_a, res_b = await asyncio.wait_for(
        asyncio.gather(_asset(1), _asset(2)), timeout=_WAIT_TIMEOUT_S
    )
    assert res_a[0] == res_b[0]
    await _assert_single_active(async_repo, UnderlyingAsset, UnderlyingAsset.ticker == ticker)


@pytest.mark.asyncio
async def test_pg18_race_instrument_underlying_mapping(
    pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent upsert_instrument_underlying_mapping calls resolve via SAVEPOINT."""
    async_repo, _ = pg_repos
    _, inst_pid = await _seed_symbol_instrument_pg(async_repo, f"MAP-{uuid4().hex[:6]}-USD")
    underlying_pid, _ = await async_repo.upsert_underlying_asset(
        ticker=f"UMAP_{uuid4().hex[:6]}",
        name={"en": "Mapped Index"},
        asset_class="index",
        session_id=_session_uuid("um-seed"),
        sequence_id=1,
        timestamp=_T0,
    )
    _install_async_flush_barrier(monkeypatch, InstrumentUnderlyingMapping)

    async def _map(seq: int) -> str:
        return await async_repo.upsert_instrument_underlying_mapping(
            instrument_public_id=inst_pid,
            underlying_public_id=underlying_pid,
            relationship_type="exact",
            session_id=_session_uuid(f"um-{seq}"),
            sequence_id=seq,
            timestamp=_T1,
        )

    outcomes = await asyncio.wait_for(asyncio.gather(_map(1), _map(2)), timeout=_WAIT_TIMEOUT_S)
    assert sorted(outcomes) == ["created", "unchanged"]
    await _assert_single_active(
        async_repo,
        InstrumentUnderlyingMapping,
        InstrumentUnderlyingMapping.instrument_public_id == inst_pid,
    )
