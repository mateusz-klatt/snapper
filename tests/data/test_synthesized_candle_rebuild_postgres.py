"""Witness guarded candle rebuilds against a migrated, disposable PostgreSQL 18 database.

Opt in with TEST_DB_URL. Use a fresh PostgreSQL instance bound to loopback and
a database named snapper_rebuild_witness. The repository locks public.candles by
design, so this module uses the real migrated table in an otherwise empty
scratch database. It only inserts synthetic rows in fixture-owned partitions.
"""

import asyncio
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from secrets import token_hex
from time import monotonic
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from uuid import UUID
from uuid import uuid4
from uuid import uuid7

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.engine import URL
from sqlalchemy.engine import Connection
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import Executable
from sqlalchemy.sql import Select

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical import synthesized_candle_rebuild as rebuild
from snapper.application.updaters.historical.guarded_candle_write import CandleKey
from snapper.application.updaters.historical.guarded_candle_write import classify_failure
from snapper.application.updaters.historical.guarded_candle_write import reconcile_versions
from snapper.cli import synthesized_candle_rebuild as rebuild_cli
from snapper.core.json_types import JsonObject
from snapper.data import repository as repository_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import is_open_ended_version
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import sample_other_client_backends
from snapper.data.repository import synthesis_inputs_sha256
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.data.repository_types import ClientSample
from snapper.data.repository_types import SynthesizedRebuildExpectation
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import write_snapshot

_LOG = logging.getLogger(__name__)
_DB_NAME = "snapper_rebuild_witness"
_DAY = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4)
_AUTHOR = _DAY + timedelta(days=3)
_WRITE = _AUTHOR + timedelta(hours=1)
_OBSERVER_FIELDS = {
    "pid",
    "current_user",
    "session_user",
    "now",
    "clock_timestamp",
    "server_addr",
    "server_port",
    "database",
    "postmaster_start",
}
_BACKEND_FIELDS = {
    "pid",
    "usename",
    "datname",
    "client_addr",
    "client_port",
    "application_name",
    "backend_type",
    "backend_start",
    "xact_start",
    "state",
}


def _opt_in() -> URL | None:
    """Read only the explicit test URL and normalize the PostgreSQL async driver."""
    try:
        url = make_url(os.environ.get("TEST_DB_URL", ""))
    except ArgumentError:
        return None
    return (
        url.set(drivername="postgresql+asyncpg") if url.get_backend_name() == "postgresql" else None
    )


pytestmark = [
    pytest.mark.skipif(
        _opt_in() is None, reason="set TEST_DB_URL to opt into the PostgreSQL witness"
    ),
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.timeout(90),
]


def _leaf(day: datetime) -> str:
    """Return the migrated naming convention for a daily leaf."""
    return f"candles_d{day:%Y%m%d}"


def _fixture_days() -> tuple[datetime, ...]:
    """Include one neighboring populated day on each side of the enumeration."""
    return tuple(_DAY + timedelta(days=offset) for offset in (-1, 0, 1))


@dataclass(frozen=True, slots=True)
class _Database:
    """Dedicated migrated database and a no-pool administrative connection factory."""

    url: URL
    admin: AsyncEngine


async def _partition_fixture(connection: AsyncConnection) -> datetime:
    """Make three closed-day leaves behind the empty migration's legacy boundary."""
    bound = (
        await connection.execute(
            text(
                "SELECT ((regexp_match(pg_get_expr(relpartbound, oid), $p$'([^']+)'$p$))[1])"
                "::timestamptz FROM pg_class WHERE oid = 'public.candles_legacy'::regclass"
            )
        )
    ).scalar_one()
    assert isinstance(bound, datetime) and bound > _fixture_days()[-1]
    await connection.execute(
        text("ALTER TABLE public.candles DETACH PARTITION public.candles_legacy")
    )
    for day in _fixture_days():
        leaf = _leaf(day)
        await connection.execute(
            text(
                f"CREATE TABLE public.{leaf} PARTITION OF public.candles "
                f"FOR VALUES FROM ('{day.isoformat()}') TO ('{(day + timedelta(days=1)).isoformat()}')"
            )
        )
        await connection.execute(text(f"ALTER TABLE public.{leaf} ADD PRIMARY KEY (id)"))
        await connection.execute(
            text(
                f"CREATE UNIQUE INDEX {leaf}_public_id ON public.{leaf} (public_id) "
                f"WHERE known_to = TIMESTAMPTZ '{KNOWN_TO_MAX.isoformat()}'"
            )
        )
    return bound


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def database() -> AsyncIterator[_Database]:
    """Accept only the dedicated empty loopback database and restore its partition topology."""
    url = _opt_in()
    assert url is not None
    local_database = url.host in {"127.0.0.1", "localhost", "::1"} and url.database == _DB_NAME
    assert local_database, "use the fresh loopback snapper_rebuild_witness database"
    assert not url.query, "use a direct loopback test URL without routing options"
    admin = create_async_engine(url, poolclass=NullPool, hide_parameters=True)
    try:
        async with admin.begin() as connection:
            pg = (await connection.execute(text("SHOW server_version_num"))).scalar_one()
            assert int(pg) // 10000 == 18
            assert (
                await connection.execute(text("SELECT current_database()"))
            ).scalar_one() == _DB_NAME
            assert not (
                await connection.execute(text("SELECT EXISTS (SELECT FROM public.candles)"))
            ).scalar_one()
            bound = await _partition_fixture(connection)
        try:
            yield _Database(url, admin)
        finally:
            async with admin.begin() as connection:
                for day in _fixture_days():
                    await connection.execute(text(f"DROP TABLE public.{_leaf(day)}"))
                await connection.execute(
                    text(
                        "ALTER TABLE public.candles ATTACH PARTITION public.candles_legacy "
                        f"FOR VALUES FROM (MINVALUE) TO ('{bound.isoformat()}')"
                    )
                )
    finally:
        await admin.dispose()


@pytest_asyncio.fixture(loop_scope="module")
async def repository(database: _Database) -> AsyncIterator[SQLAlchemyRepository]:
    """Use the actual named, one-connection rebuild repository builder."""
    repo = rebuild_cli._repository(database.url.render_as_string(hide_password=False), str(uuid7()))
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.fixture
def instrument() -> str:
    """Isolate each test's candle keys."""
    return str(uuid4())


def _row(instrument: str, opening: datetime, timeframe: str, **changes: object) -> CandleUpsertRow:
    """Create a closed and completely typed witness candle."""
    values: dict[str, object] = {
        "instrument_public_id": instrument,
        "open_at": opening,
        "timeframe": timeframe,
        "timestamp": _WRITE,
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 2,
        "source": "synthesized",
        "complete": True,
        "session_id": str(uuid7()),
        "sequence_id": 1,
        "price_basis": None,
    }
    return cast(CandleUpsertRow, values | changes)


def _key(row: CandleUpsertRow) -> CandleKey:
    """Resolve the tested natural key."""
    return row["instrument_public_id"], row["timeframe"], row["open_at"]


async def _store(repository: SQLAlchemyRepository, rows: Sequence[CandleUpsertRow]) -> None:
    """Seed explicit fixture rows using the real migrated candle column defaults."""
    async with repository.session() as session:
        await session.execute(insert(Candle), list(rows))
        await session.commit()


async def _versions(repository: SQLAlchemyRepository, key: CandleKey) -> list[CandleVersionRow]:
    """Read the versions of an exact key in physical order."""
    async with repository.session() as session:
        result = await session.execute(
            select(Candle.__table__)
            .where(
                Candle.instrument_public_id == key[0],
                Candle.timeframe == key[1],
                Candle.open_at == key[2],
            )
            .order_by(Candle.id)
        )
        return [cast(CandleVersionRow, dict(row)) for row in result.mappings()]


def _expected(
    current: CandleVersionRow | None = None, inputs: Sequence[CandleVersionRow] = ()
) -> SynthesizedRebuildExpectation:
    """Bind a window to its predecessor and read minute versions."""
    predecessor = (
        None
        if current is None
        else CandlePredecessor(
            id=current["id"], public_id=current["public_id"], timestamp=current["timestamp"]
        )
    )
    return SynthesizedRebuildExpectation(
        predecessor=predecessor, inputs_sha256=synthesis_inputs_sha256(inputs)
    )


async def test_rebuild_insert_and_replacement_route_to_the_opening_day(
    repository: SQLAlchemyRepository, instrument: str
) -> None:
    """Preserve identity and history while both writes land in the real daily leaf."""
    first = _row(instrument, _DAY, "5m", timestamp=_AUTHOR)
    key = _key(first)
    inserted = await repository.rebuild_synthesized_candles([first], expected={key: _expected()})
    assert inserted.outcomes[key].status == "inserted"
    (old,) = await _versions(repository, key)
    replacement = _row(instrument, _DAY, "5m", close=10.5, volume=4.0)
    replaced = await repository.rebuild_synthesized_candles(
        [replacement], expected={key: _expected(old)}
    )
    assert replaced.outcomes[key].status == "replaced"
    closed, current = await _versions(repository, key)
    assert closed["known_to"] == replacement["timestamp"]
    assert is_open_ended_version(current["known_to"])
    assert current["public_id"] == old["public_id"]
    assert current["id"] != old["id"]
    async with repository.session() as session:
        locations = (
            (
                await session.execute(
                    text(
                        "SELECT DISTINCT tableoid::regclass::text FROM public.candles "
                        "WHERE instrument_public_id = CAST(:instrument AS uuid)"
                    ),
                    {"instrument": instrument},
                )
            )
            .scalars()
            .all()
        )
    assert locations == [_leaf(_DAY)]


@pytest.mark.parametrize("change", ["predecessor", "inputs"])
async def test_competing_commits_between_preflight_and_lock_refuse_the_voted_window(
    repository: SQLAlchemyRepository,
    database: _Database,
    instrument: str,
    change: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeat both guards under the actual PostgreSQL fence after a competing commit."""
    current = _row(instrument, _DAY, "5m", timestamp=_AUTHOR, close=10.0)
    await _store(repository, [current])
    key = _key(current)
    (before,) = await _versions(repository, key)
    fence = repository._begin_candle_reconciliation_write

    async def race_then_lock(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Commit with a separate PostgreSQL connection before acquiring the fence."""
        async with database.admin.begin() as other:
            if change == "inputs":
                await other.execute(insert(Candle).values(**_row(instrument, _DAY, "1m")))
            else:
                await other.execute(
                    update(Candle)
                    .where(Candle.open_at == _DAY, Candle.id == before["id"])
                    .values(known_to=_WRITE)
                )
                successor = dict(current) | {"public_id": before["public_id"]}
                await other.execute(insert(Candle).values(**successor))
        await fence(session, sqlite_write_lock=sqlite_write_lock)

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", race_then_lock)
    new = _row(instrument, _DAY, "5m")
    result = await repository.rebuild_synthesized_candles([new], expected={key: _expected(before)})
    assert result.outcomes[key].status == (
        "inputs_changed" if change == "inputs" else "predecessor_changed"
    )
    assert result.sample is None


async def test_row_exclusive_writer_blocks_rebuild_for_approximately_one_second(
    repository: SQLAlchemyRepository, database: _Database, instrument: str
) -> None:
    """Witness the real transaction conflict and the driver's reported lock timeout."""
    row = _row(instrument, _DAY, "5m")
    async with database.admin.begin() as writer:
        await writer.execute(text("LOCK TABLE public.candles IN ROW EXCLUSIVE MODE"))
        dry_run = await repository.rebuild_synthesized_candles(
            [row], expected={_key(row): _expected()}, dry_run=True
        )
        assert dry_run.outcomes[_key(row)].status == "insert"
        started = monotonic()
        with pytest.raises(DBAPIError) as caught:
            await repository.rebuild_synthesized_candles([row], expected={_key(row): _expected()})
        elapsed = monotonic() - started
        assert getattr(caught.value.orig, "sqlstate", None) == "55P03"
        assert classify_failure(caught.value) == "lock_timeout"
        assert 0.85 <= elapsed < 5.0
        _LOG.info("Conflicting writer returned 55P03 in %.3f seconds", elapsed)
    assert await _versions(repository, _key(row)) == []


@dataclass(frozen=True, slots=True)
class _Roles:
    """Distinct PostgreSQL logins with no special statistics-viewing privileges."""

    reader: URL
    peer: URL


@pytest_asyncio.fixture(loop_scope="module")
async def roles(database: _Database) -> AsyncIterator[_Roles]:
    """Create real login roles so PostgreSQL masks another user's backend fields."""
    identity = uuid4().hex[:12]
    names = (f"rebuild_reader_{identity}", f"rebuild_peer_{identity}")
    passwords = (token_hex(24), token_hex(24))
    async with database.admin.begin() as connection:
        for name, password in zip(names, passwords, strict=True):
            await connection.execute(text(f"CREATE ROLE {name} LOGIN PASSWORD '{password}'"))
        await connection.execute(
            text(
                "GRANT SELECT ON public.symbols, public.instruments, public.symbol_aliases, "
                f"public.process_runs, public.candles TO {names[0]}"
            )
        )
    try:
        yield _Roles(
            database.url.set(username=names[0], password=passwords[0]),
            database.url.set(username=names[1], password=passwords[1]),
        )
    finally:
        async with database.admin.begin() as connection:
            for name in names:
                await connection.execute(text(f"DROP OWNED BY {name}"))
                await connection.execute(text(f"DROP ROLE {name}"))


def _peer(roles: _Roles, name: str) -> AsyncEngine:
    """Open distinct real client backends without retaining a pooled connection."""
    return create_async_engine(
        roles.peer,
        poolclass=NullPool,
        connect_args={"server_settings": {"application_name": name, "timezone": "UTC"}},
    )


async def _lock_and_sample_for_daily_window(
    repository: SQLAlchemyRepository,
    instrument: str,
    monkeypatch: pytest.MonkeyPatch,
    other_backends: int,
) -> None:
    """Observe the real lock and staged row at the repository's sampling point."""
    locked_times: list[float] = []
    real_fence = repository._begin_candle_reconciliation_write
    samples: list[ClientSample] = []
    run_id = str(uuid7())

    async def fence(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Measure from acknowledgement of the lock."""
        await real_fence(session, sqlite_write_lock=sqlite_write_lock)
        locked_times.append(monotonic())

    async def observe(session: AsyncSession) -> ClientSample | None:
        """See both the parent fence and the staged row in this uncommitted transaction."""
        modes = (
            (
                await session.execute(
                    text(
                        "SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() AND granted "
                        "AND relation = 'public.candles'::regclass"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "ShareRowExclusiveLock" in modes
        staged = (
            (
                await session.execute(
                    select(Candle.id).where(
                        Candle.instrument_public_id == instrument,
                        Candle.timeframe == "1d",
                        Candle.session_id == run_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(staged) == 1
        sample = await sample_other_client_backends(session)
        assert sample is not None
        samples.append(sample)
        return sample

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", fence)
    monkeypatch.setattr(repository_module, "sample_other_client_backends", observe)
    key = (instrument, "1d", _DAY)
    async with repository.session() as session:
        inputs = await repository._synthesized_rebuild_inputs(session, [key])
    assert len(inputs[key]) == 1440
    versions = await _versions(repository, key)
    at = _WRITE + timedelta(seconds=len(versions))
    plan = rebuild._classify(
        rebuild._Window(key, "XRP-EUR", 1, "no_input"), versions, inputs[key], at
    )
    row = rebuild._write_row(plan, run_id, at, 1)
    result = await repository.rebuild_synthesized_candles(
        [row], expected={key: rebuild._expectation(plan)}
    )
    held = monotonic() - locked_times[0]
    assert result.outcomes[key].status == ("replaced" if versions else "inserted")
    assert result.sample == samples[0] and samples[0].other_client_backends == other_backends
    _LOG.info(
        "Full 1d grid (%s minute versions) fence through commit acknowledgement: %.3f seconds",
        len(inputs[key]),
        held,
    )


async def test_largest_chunk_samples_under_the_real_lock_with_one_other_client(
    repository: SQLAlchemyRepository,
    roles: _Roles,
    instrument: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Measure the maximum grid and observe the other backend come and go."""
    pool = repository.engine.pool
    assert isinstance(pool, AsyncAdaptedQueuePool) and pool.size() == 1
    await _store(
        repository,
        [
            _row(instrument, _DAY + timedelta(minutes=offset), "1m", timestamp=_AUTHOR)
            for offset in range(1440)
        ],
    )
    peer = _peer(roles, "witness-peer")
    try:
        async with peer.connect() as connection:
            await connection.execute(text("SELECT pg_backend_pid()"))
            with monkeypatch.context() as patch:
                await _lock_and_sample_for_daily_window(repository, instrument, patch, 1)
    finally:
        await peer.dispose()
    assert (
        await repository.upsert_candles(
            [_row(instrument, _DAY, "1m", timestamp=_AUTHOR + timedelta(minutes=1), volume=4.0)]
        )
        == 1
    )
    await _lock_and_sample_for_daily_window(repository, instrument, monkeypatch, 0)


def _enumeration_run(instrument: str, run_id: str) -> rebuild._RepairRun:
    """Supply the already-verified approval bounds to the actual enumeration query."""
    scope = candle_repair.RepairWindow(_DAY, _DAY + timedelta(days=1))
    return cast(
        rebuild._RepairRun,
        SimpleNamespace(
            record=SimpleNamespace(run_id=run_id), window=scope, instruments={instrument: "XRP-EUR"}
        ),
    )


async def _populate_index_distractors(database: _Database) -> None:
    """Give the planner realistic selectivity on the populated target and control leaves."""
    async with database.admin.begin() as connection:
        for day in _fixture_days():
            await connection.execute(
                text(
                    "INSERT INTO public.candles (public_id, instrument_public_id, timeframe, open_at, "
                    "open, high, low, close, volume, vwap, trades, session_id, sequence_id, timestamp, known_to) "
                    "SELECT gen_random_uuid(), gen_random_uuid(), '1m', :opening, "
                    "10, 12, 9, 11, 3, 10.75, 2, gen_random_uuid(), n, :author, :known_to "
                    "FROM generate_series(1, 15000) n"
                ),
                {"opening": day, "author": _AUTHOR, "known_to": KNOWN_TO_MAX},
            )
            await connection.execute(text(f"ANALYZE public.{_leaf(day)}"))


def _plan_nodes(plan: JsonObject) -> list[JsonObject]:
    """Walk nested scans and bitmap nodes in the measured JSON plan."""
    return [
        plan,
        *(
            node
            for child in cast(list[JsonObject], plan.get("Plans", []))
            for node in _plan_nodes(child)
        ),
    ]


async def test_actual_enumeration_prunes_neighbors_and_uses_the_leaf_range_index(
    repository: SQLAlchemyRepository,
    database: _Database,
    instrument: str,
) -> None:
    """Explain the exact production enumeration with populated control partitions."""
    run_id = str(uuid7())
    await _store(
        repository,
        [
            _row(instrument, day, "1m", session_id=run_id, timestamp=_AUTHOR)
            for day in _fixture_days()
        ],
    )
    await _populate_index_distractors(database)
    emitted: list[Executable] = []

    def remember(
        connection: Connection,
        clause: Executable,
        multiparams: object,
        params: object,
        execution_options: object,
    ) -> None:
        """Capture the actual select, including inherited SQLAlchemy column types."""
        del connection, multiparams, params, execution_options
        if isinstance(clause, Select):
            emitted.append(clause)

    engine = repository.engine.sync_engine
    event.listen(engine, "before_execute", remember)
    try:
        versions = await rebuild._enumerate_repair(repository, _enumeration_run(instrument, run_id))
    finally:
        event.remove(engine, "before_execute", remember)
    assert [version["open_at"] for version in versions] == [_DAY]
    (statement,) = emitted
    query = statement.compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True})
    async with database.admin.connect() as connection:
        raw = (
            await connection.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
        ).scalar_one()
    doc = cast(list[JsonObject], json.loads(raw) if isinstance(raw, str) else raw)
    nodes = _plan_nodes(cast(JsonObject, doc[0]["Plan"]))
    assert {node["Relation Name"] for node in nodes if "Relation Name" in node} == {_leaf(_DAY)}
    assert all(node["Node Type"] != "Seq Scan" for node in nodes)
    assert {node["Index Name"] for node in nodes if "Index Name" in node} == {
        f"{_leaf(_DAY)}_instrument_public_id_open_at_idx"
    }


async def test_verification_reads_one_repeatable_read_only_snapshot(
    repository: SQLAlchemyRepository,
    database: _Database,
    instrument: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Commit a paired change between the two SELECTs and observe the old pair atomically."""
    base = _row(instrument, _DAY, "1m", timestamp=_AUTHOR)
    output = _row(instrument, _DAY, "5m", timestamp=_AUTHOR)
    await _store(repository, [base, output])
    key = _key(output)
    context = candle_repair.RunContext(repository, {}, {})
    real_read = rebuild._read_output_versions

    async def read_then_commit(
        session: AsyncSession, unit: rebuild._ReadUnit
    ) -> dict[CandleKey, list[CandleVersionRow]]:
        """Check actual transaction flags and commit a live-style batch separately."""
        isolation = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
        assert isolation == "repeatable read"
        assert (await session.execute(text("SHOW transaction_read_only"))).scalar_one() == "on"
        seen = await real_read(session, unit)
        writer = SQLAlchemyRepository(database.url.render_as_string(hide_password=False))
        try:
            assert (
                await writer.upsert_candles(
                    [
                        _row(instrument, _DAY, "1m", volume=9.0),
                        _row(instrument, _DAY, "5m", volume=9.0),
                    ]
                )
                == 2
            )
        finally:
            await writer.engine.dispose()
        return seen

    with monkeypatch.context() as patch:
        patch.setattr(rebuild, "_read_output_versions", read_then_commit)
        observed = await rebuild._snapshot(context, rebuild._units([key])[0])
    assert observed.before <= observed.after
    assert observed.outputs[key][0]["volume"] == observed.inputs[instrument][0]["volume"] == 3.0
    later = await rebuild._snapshot(context, rebuild._units([key])[0])
    (current,) = [row for row in later.outputs[key] if is_open_ended_version(row["known_to"])]
    assert current["volume"] == later.inputs[instrument][0]["volume"] == 9.0


async def test_reconciliation_reads_both_outcomes_while_another_writer_holds_a_lock(
    repository: SQLAlchemyRepository, database: _Database, instrument: str
) -> None:
    """Find exact timestamp and price-basis matches without the rebuild write fence."""
    run_id = str(uuid7())
    rows = [_row(instrument, _DAY, timeframe, session_id=run_id) for timeframe in ("5m", "15m")]
    await _store(repository, rows)
    assert (
        await repository.upsert_candles(
            [_row(instrument, _DAY, "15m", volume=6.0, timestamp=_WRITE + timedelta(seconds=1))]
        )
        == 1
    )
    async with database.admin.begin() as writer:
        await writer.execute(text("LOCK TABLE public.candles IN ROW EXCLUSIVE MODE"))
        async with asyncio.timeout(3):
            result = await reconcile_versions(
                repository,
                {_key(row): row for row in rows},
                run_id,
                extra_fields=("timestamp", "price_basis"),
            )
    assert result == {_key(rows[0]): "applied", _key(rows[1]): "applied_superseded"}


def _capture_value(value: object) -> str | int | None:
    """Encode a PostgreSQL observation as the coordinator's JSON scalar."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int)) or value is None:
        return value
    return str(value)


async def _capture(reader: AsyncEngine, endpoint: JsonObject) -> JsonObject:
    """Run the two documented statements in a fresh unprivileged transaction."""
    async with reader.begin() as connection:
        observer = (
            (
                await connection.execute(
                    text(
                        "SELECT pg_backend_pid() AS pid, current_user AS current_user, "
                        "session_user AS session_user, now() AS now, clock_timestamp() AS clock_timestamp, "
                        "inet_server_addr() AS server_addr, inet_server_port() AS server_port, "
                        "current_database() AS database, pg_postmaster_start_time() AS postmaster_start"
                    )
                )
            )
            .mappings()
            .one()
        )
        rows = (
            (
                await connection.execute(
                    text(
                        "SELECT pid AS pid, usename AS usename, datname AS datname, client_addr AS client_addr, "
                        "client_port AS client_port, application_name AS application_name, "
                        "backend_type AS backend_type, backend_start AS backend_start, "
                        "xact_start AS xact_start, state AS state FROM pg_stat_activity ORDER BY pid"
                    )
                )
            )
            .mappings()
            .all()
        )
    assert set(observer) == _OBSERVER_FIELDS
    assert all(set(row) == _BACKEND_FIELDS for row in rows)
    return {
        "endpoint": endpoint,
        "role": observer["session_user"],
        "observer": {key: _capture_value(value) for key, value in observer.items()},
        "backends": [{key: _capture_value(value) for key, value in row.items()} for row in rows],
    }


def _settlement_record(run_id: str, capture: JsonObject) -> rebuild._RunRecord:
    """Bind the real capture to a synthetic previously exited invocation."""
    observer = cast(JsonObject, capture["observer"])
    when = datetime.fromisoformat(cast(str, observer["now"]))
    return rebuild._RunRecord.model_validate(
        {
            "run_id": run_id,
            "voted_digest": "0" * 64,
            "stop_reason": "completed",
            "approval_sha256": "1" * 64,
            "receipt_sha256": "2" * 64,
            "receipt_lines_sha256": "3" * 64,
            "application_name": f"snapper:repair-candles:{run_id}",
            "observed_addresses": [],
            "container": {
                "name": "fixture",
                "image_id": "sha256:" + "4" * 64,
                "addresses": [],
                "started_at": (when - timedelta(hours=1)).isoformat(),
                "finished_at": (when - timedelta(minutes=1)).isoformat(),
            },
            "settlements": [capture],
        }
    )


async def test_unprivileged_capture_binds_current_database_and_counts_a_hidden_backend(
    roles: _Roles,
) -> None:
    """Check real masking, observer proof and exact-name settlement before and after disconnect."""
    reader = create_async_engine(roles.reader, poolclass=NullPool)
    run_id = str(uuid7())
    peer = _peer(roles, f"snapper:repair-candles:{run_id}")
    endpoint = candle_repair.database_endpoint(roles.reader.render_as_string(hide_password=False))
    try:
        async with peer.connect() as connection:
            pid = (await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            capture = await _capture(reader, endpoint)
            record = _settlement_record(run_id, capture)
            rebuild._check_record(record, endpoint)
            hidden = next(row for row in record.settlements[0].backends if row.pid == pid)
            assert hidden.datname == _DB_NAME and hidden.usename == roles.peer.username
            assert (
                hidden.client_addr,
                hidden.backend_start,
                hidden.state,
                hidden.backend_type,
            ) == (None,) * 4
            assert not rebuild._settled(record)
            async with AsyncSession(reader) as session:
                sample = await sample_other_client_backends(session)
                assert sample is not None and sample.other_client_backends == 1
        clear = _settlement_record(run_id, await _capture(reader, endpoint))
        rebuild._check_record(clear, endpoint)
        assert rebuild._settled(clear)
        clear.settlements[0].observer.database = "other-database"
        with pytest.raises(candle_repair.RepairRefusedError, match="endpoint differs"):
            rebuild._check_record(clear, endpoint)
    finally:
        await peer.dispose()
        await reader.dispose()


@dataclass(frozen=True, slots=True)
class _CommandInputs:
    """Real repair artifacts and reusable CLI arguments for both commands."""

    repair: tuple[str, ...]
    rebuilt: tuple[str, ...]


@pytest_asyncio.fixture(loop_scope="module")
async def catalog(
    repository: SQLAlchemyRepository, instrument: str
) -> AsyncIterator[tuple[str, str]]:
    """Create and remove just this invocation's PostgreSQL catalog identities."""
    base = f"TEST{instrument[:6].upper()}"
    symbol, alias = f"{base}-EUR", f"Z{base}ZEUR"
    symbol_id, owner = str(uuid4()), str(uuid7())
    common = {"timestamp": _AUTHOR, "session_id": owner, "sequence_id": 1}
    async with repository.session() as session:
        session.add_all(
            [
                Symbol(
                    public_id=symbol_id,
                    native_symbol=symbol,
                    base=base,
                    quote="EUR",
                    created_at=_AUTHOR,
                    **common,
                ),
                Instrument(
                    public_id=instrument, symbol_public_id=symbol_id, exchange="kraken", **common
                ),
                SymbolAlias(
                    symbol_public_id=symbol_id,
                    exchange="kraken",
                    channel="rest",
                    exchange_symbol=alias,
                    created_at=_AUTHOR,
                    **common,
                ),
            ]
        )
        await session.commit()
    try:
        yield symbol, alias
    finally:
        async with repository.session() as session:
            for model in (SymbolAlias, Instrument, Symbol):
                await session.execute(delete(model).where(model.session_id == owner))
            await session.commit()


@pytest_asyncio.fixture(loop_scope="module")
async def command_inputs(
    repository: SQLAlchemyRepository,
    database: _Database,
    tmp_path: Path,
    catalog: tuple[str, str],
) -> _CommandInputs:
    """Create a real one-minute write and its exact binding in the disposable database."""
    symbol, alias = catalog
    snapshot = tmp_path / "source"
    write_snapshot(
        snapshot,
        alias,
        [
            kraken_row(_DAY, 2),
            kraken_row(_DAY + timedelta(minutes=1), 1),
        ],
        (_DAY + timedelta(minutes=1, seconds=30)).timestamp(),
    )
    start, end = _DAY, _DAY + timedelta(minutes=5)
    request = candle_repair.RepairRequest(
        source=candle_repair.RepairSource.SNAPSHOT,
        exchange="kraken",
        window=candle_repair.RepairWindow(start, end),
        symbols=(symbol,),
        all_mapped=False,
        snapshot_dir=snapshot,
        output_dir=tmp_path / "vote",
    )
    runtime = candle_repair.RunContext(
        repository,
        candle_repair.database_endpoint(database.url.render_as_string(hide_password=False)),
        {},
    )
    request.output_dir.mkdir()
    vote = await candle_repair.plan_repair(runtime, request)
    written = tmp_path / "repair-write"
    written.mkdir()
    report = await candle_repair.write_repair(
        runtime,
        replace(request, output_dir=written),
        candle_repair.WriteOptions(request.output_dir, vote.digest, None, 0, 3),
    )
    assert report.totals["applied"] == 1
    manifest = _repair_manifest(tmp_path, written)
    return _CommandInputs(
        (
            "--source",
            "snapshot",
            "--snapshot-dir",
            str(snapshot),
            "--symbol",
            symbol,
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
        ),
        ("--repair-run", str(written), "--run-records", str(manifest), "--end", end.isoformat()),
    )


def _repair_manifest(root: Path, written: Path) -> Path:
    """Produce the fixture record from real receipt identity and full file hashes."""
    receipt = cast(JsonObject, json.loads((written / "receipt.json").read_bytes()))
    started = datetime.fromisoformat(cast(str, receipt["started_at"])) - timedelta(seconds=1)
    finished = datetime.fromisoformat(cast(str, receipt["ended_at"])) + timedelta(seconds=1)
    hashes = {
        key: candle_repair.sha256_hex((written / path).read_bytes())
        for key, path in (
            ("approval_sha256", "approval.json"),
            ("receipt_sha256", "receipt.json"),
            ("receipt_lines_sha256", "receipt.jsonl"),
        )
    }
    manifest = root / "run-records.json"
    manifest.write_bytes(
        candle_repair.canonical_json(
            {
                "runs": [
                    {
                        **{key: receipt[key] for key in ("run_id", "voted_digest", "stop_reason")},
                        **hashes,
                        "application_name": None,
                        "observed_addresses": [],
                        "container": {
                            "name": "fixture",
                            "image_id": "sha256:" + "1" * 64,
                            "started_at": started.isoformat(),
                            "finished_at": finished.isoformat(),
                            "addresses": [],
                        },
                        "settlements": [],
                    }
                ]
            }
        )
    )
    return manifest


async def _named_backends(observer: AsyncConnection, login: str, prefix: str) -> bool:
    """Check connection names from the first SQL command, after startup bookkeeping."""
    rows = (
        await observer.execute(
            text(
                "SELECT application_name, wait_event_type FROM pg_stat_activity "
                "WHERE usename = :login AND datname = current_database() AND query_start IS NOT NULL"
            ),
            {"login": login},
        )
    ).all()
    await observer.rollback()
    assert len(rows) <= 1
    for name, _ in rows:
        assert name.startswith(prefix)
        assert UUID(name.removeprefix(prefix)).version == 7
    return any(kind == "Lock" for _, kind in rows)


async def _wait_for_first_read(
    observer: AsyncConnection, login: str, prefix: str, child: asyncio.subprocess.Process
) -> None:
    """Hold the first catalog read so its connection cannot escape observation."""
    async with asyncio.timeout(45):
        while child.returncode is None:
            if await _named_backends(observer, login, prefix):
                return
            await asyncio.sleep(0.02)
    pytest.fail("the command exited before its first held database read")


@pytest.mark.parametrize("command", ["repair-candles", "rebuild-synthesized-candles"])
async def test_command_connections_are_named_from_startup(
    database: _Database,
    roles: _Roles,
    command_inputs: _CommandInputs,
    tmp_path: Path,
    command: str,
) -> None:
    """Inspect the real CLI processes through a held initial read and a distinct login."""
    tag = "repair-candles" if command == "repair-candles" else "rebuild-synth"
    argv = command_inputs.repair if command == "repair-candles" else command_inputs.rebuilt
    environment = os.environ.copy()
    environment.update(
        {
            "DB_URL": roles.reader.render_as_string(hide_password=False),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "SNAPPER_LOG_FILE": str(tmp_path / "command.log"),
        }
    )
    async with database.admin.connect() as observer, database.admin.begin() as fence:
        await fence.execute(text("LOCK TABLE public.symbols IN ACCESS EXCLUSIVE MODE"))
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "snapper",
            command,
            *argv,
            "--output-dir",
            str(tmp_path / "out"),
            cwd=tmp_path,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await _wait_for_first_read(
                observer, cast(str, roles.reader.username), f"snapper:{tag}:", child
            )
        except BaseException:
            if child.returncode is None:
                child.kill()
                await child.wait()
            raise
    try:
        async with asyncio.timeout(30):
            output, errors = await child.communicate()
        assert child.returncode == 0, errors.decode(errors="replace")
        assert b"DRY RUN" in output
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


@pytest.mark.parametrize(
    ("changes", "status", "reason"),
    [
        ({"known_to": _WRITE}, "history_without_current", None),
        ({"source": "native"}, "protected_source", None),
        ({"source": "calculated"}, "protected_source", None),
        ({"timestamp": _WRITE}, "not_newer", None),
        ({}, "unchanged", None),
        ({"volume": 4.0}, "would_lose_observations", "lower_volume"),
        ({"trades": 3}, "would_lose_observations", "lower_trades"),
        ({"volume": 4.0, "trades": 3}, "would_lose_observations", "lower_volume+lower_trades"),
        ({"trades": None}, "would_lose_observations", "unknown_current"),
        (
            {"volume": 4.0, "trades": None},
            "would_lose_observations",
            "lower_volume+unknown_current",
        ),
        ({"volume": float("nan")}, "would_lose_observations", "unknown_current"),
        ({"volume": float("inf")}, "would_lose_observations", "unknown_current"),
        ({"vwap": 10.5}, "replaced", None),
        ({"trades": 1}, "replaced", None),
        ({"complete": False}, "replaced", None),
        ({"price_basis": "mid"}, "replaced", None),
        ({"volume": 2.0}, "replaced", None),
    ],
)
async def test_real_postgres_guard_preserves_ownership_and_observation_counts(
    repository: SQLAlchemyRepository,
    instrument: str,
    changes: dict[str, object],
    status: str,
    reason: str | None,
) -> None:
    """Exercise the same dry-run and transactional rules on the migrated column types."""
    candidate = _row(instrument, _DAY, "5m")
    key = _key(candidate)
    await _store(repository, [_row(instrument, _DAY, "5m", **({"timestamp": _AUTHOR} | changes))])
    (existing,) = await _versions(repository, key)
    expected = {key: _expected(existing)}
    preview = await repository.rebuild_synthesized_candles(
        [candidate], expected=expected, dry_run=True
    )
    assert preview.outcomes[key].status == ("replace" if status == "replaced" else status)
    assert preview.sample is None
    written = await repository.rebuild_synthesized_candles([candidate], expected=expected)
    assert (written.outcomes[key].status, written.outcomes[key].reason) == (status, reason)
    assert len(await _versions(repository, key)) == (2 if status == "replaced" else 1)


@pytest.mark.parametrize(
    "defect",
    [
        "count",
        "grid",
        "minute",
        "alignment",
        "open",
        "vwap",
        "trades",
        "complete",
        "duplicate",
        "missing",
        "extra",
    ],
)
async def test_postgres_rebuild_validation_refuses_before_opening_a_session(
    repository: SQLAlchemyRepository,
    instrument: str,
    defect: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply both limits and validate the entire batch before using any database session."""
    row = _row(instrument, _DAY, "5m")
    changes: dict[str, dict[str, object]] = {
        "minute": {"timeframe": "1m"},
        "alignment": {"open_at": _DAY + timedelta(minutes=1)},
        "open": {"timestamp": _DAY + timedelta(minutes=1)},
        "vwap": {"vwap": None},
        "trades": {"trades": None},
        "complete": {"complete": False},
    }
    row = cast(CandleUpsertRow, dict(row) | changes.get(defect, {}))
    rows = [row]
    if defect == "count":
        rows *= 101
    if defect == "duplicate":
        rows *= 2
    if defect == "grid":
        rows.append(_row(instrument, _DAY, "1d"))
    expected = {_key(candidate): _expected() for candidate in rows}
    if defect == "missing":
        expected.clear()
    if defect == "extra":
        expected[instrument, "5m", _DAY + timedelta(minutes=5)] = _expected()
    session = MagicMock(side_effect=AssertionError("validation reached the database"))
    monkeypatch.setattr(repository, "session", session)
    with pytest.raises(ValueError):
        await repository.rebuild_synthesized_candles(rows, expected=expected)
    session.assert_not_called()
