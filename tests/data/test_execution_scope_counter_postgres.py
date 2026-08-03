"""Opt-in live-PostgreSQL proof of the sealed-prefix contiguity invariant.

SQLite structurally cannot exhibit writer concurrency (one writer at a
time), so its runs pass vacuously for the interesting half of the design
proof, and the mocked-dialect tests pin statement TEXT and ORDER only.
This module is the one place the sealed-prefix property is witnessed on
the production dialect with genuinely overlapping connections: an
in-flight fenced assignment can never land at or below an
already-captured boundary, overlapping executor generations mint
distinct contiguous counters for the same wallet, the total-unique
index refuses a fence bypass with a loud ``IntegrityError``, the insert
transaction forces READ COMMITTED explicitly over a hostile inherited
``default_transaction_isolation``, and a waiter parked on the fence
BEFORE the holder commits allocates the NEXT counter rather than a
stale-snapshot duplicate.

Every test SKIPS unless the session database URL (``DB_URL``, the
Makefile's ``TEST_DB_URL`` pass-through) is a PostgreSQL URL, so the
default SQLite ``make test`` / ``make check-all`` runs are undisturbed.
Opt in against the docker-compose ``postgres:16`` dev profile with a
migrated scratch database (the ``uat-db-up`` recipe shape)::

    docker compose --profile dev up -d postgres
    docker compose --profile dev exec -T postgres \
        createdb -U snapper snapper_scope_proof
    DB_URL=postgresql+asyncpg://snapper:example@localhost:5432/snapper_scope_proof \
        .venv/bin/python -m snapper db-init
    make test TEST_DB_URL=postgresql+asyncpg://snapper:example@localhost:5432/snapper_scope_proof

Tests write only rows keyed by per-test random wallet identities. The
orders and instruments they seed are deleted on teardown; the executions
are physically immutable (the ``executions_reject_row_mutation`` trigger
refuses DELETE, and TRUNCATE is refused too), so they are never deleted —
per-test isolation comes from the random wallet keys and the scratch
database is reaped by dropping it between runs. Never point it at
production (staged fills would interleave with real ingest).
"""

import asyncio
import os
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_EXCHANGE = "walutomat"
_MODE = "live"
_CAPTURE_TIMEOUT_SECONDS = 5.0
_RACE_TIMEOUT_SECONDS = 10.0
_FENCE_STATEMENT = text("SELECT pg_advisory_xact_lock(hashtext('execution_fence'), hashtext(:wid))")
_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_PG_ACTIVE = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_ANCHOR_PROBE_PREFIX = "SELECT COUNT(*) FROM portfolio_spot_reconciliation_anchors"
_FENCE_LOCK_PREFIX = "LOCK TABLE executions"
_HOSTILE_ISOLATION = "repeatable read"
_INTRUDER_LOCK_TIMEOUT = "2s"
_MIGRATION_PARK_TIMEOUT_SECONDS = 30.0
_INSERT_INSTRUMENT_0028_SQL = (
    "INSERT INTO instruments (public_id, symbol_public_id, exchange, session_id, "
    "sequence_id, timestamp, known_to) VALUES (:public_id, :public_id, :exchange, "
    ":session_id, 1, :timestamp, :known_to)"
)
_INSERT_ORDER_0028_SQL = (
    "INSERT INTO orders (public_id, instrument_public_id, mode, wallet_public_id, "
    "side, order_type, size, status, created_at, session_id, sequence_id, timestamp, "
    "known_to) VALUES (:public_id, :instrument_public_id, :mode, :wallet_public_id, "
    "'buy', 'limit', 1.0, 'filled', :timestamp, :session_id, 1, :timestamp, :known_to)"
)
_INSERT_EXECUTION_0028_SQL = (
    "INSERT INTO executions (public_id, order_public_id, wallet_public_id, side, "
    "status, price, size, fee, fee_asset, session_id, sequence_id, timestamp, known_to) "
    "VALUES (:public_id, :order_public_id, :wallet_public_id, 'buy', 'filled', 1.25, "
    "2.0, 0.1, 'PLN', :session_id, 1, :timestamp, :known_to)"
)


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL.

    Reads the ``DB_URL`` environment variable the Makefile's
    ``TEST_DB_URL`` pass-through sets for every test run; an unset or
    unparseable value maps to the empty string so the module skips
    instead of erroring under ad-hoc invocations.
    """
    url = os.environ.get("DB_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason=(
        "live-PostgreSQL proof module: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


@dataclass(frozen=True)
class _ScopeIdentity:
    """Per-test unique identities isolating one live certification scope.

    Random UUIDs keep parallel xdist workers and successive runs against
    a shared staging database from ever colliding; the ``scope`` fixture
    deletes every row created under them on teardown.
    """

    wallet_public_id: str
    order_public_id: str
    instrument_public_id: str
    session_id: str


@pytest.fixture
async def repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create one repository engine against the live PostgreSQL database."""
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield result
    finally:
        await result.engine.dispose()


@pytest.fixture
async def peer_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create a second independent engine modeling the peer executor generation."""
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield result
    finally:
        await result.engine.dispose()


@pytest.fixture
async def inherited_repeatable_read_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository whose connections DEFAULT to REPEATABLE READ.

    Models the F3 hazard: the production engine configures no isolation,
    so every connection inherits whatever
    ``default_transaction_isolation`` the PostgreSQL role or database
    sets. Rebinding the session factory to the engine's
    ``execution_options(isolation_level=...)`` proxy reproduces that
    inherited default while ``insert_execution`` still runs completely
    unmodified — proving the writer's explicit ``SET TRANSACTION``
    overrides the environment instead of relying on it.
    """
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    result.session_factory = async_sessionmaker(
        result.engine.execution_options(isolation_level="REPEATABLE READ"),
        expire_on_commit=False,
        class_=AsyncSession,
    )
    try:
        yield result
    finally:
        await result.engine.dispose()


async def _wait_until_fence_blocked(repository: SQLAlchemyRepository) -> None:
    """Poll until one backend WAITS on an advisory lock, bounded.

    The waiter scenario needs the racing ingest to be provably parked on
    the fence BEFORE the holder commits; ``pg_stat_activity`` exposes the
    wait state, and the bound converts a scheduling pathology into a loud
    ``TimeoutError`` instead of a stalled worker.
    """
    deadline = asyncio.get_running_loop().time() + _RACE_TIMEOUT_SECONDS
    while asyncio.get_running_loop().time() < deadline:
        async with repository.session() as s:
            waiting = (
                await s.execute(
                    text("SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event = 'advisory'")
                )
            ).scalar_one()
        if int(waiting):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("no backend reached the advisory fence wait in time")


@pytest.fixture
async def scope(repository: SQLAlchemyRepository) -> AsyncIterator[_ScopeIdentity]:
    """Seed one active Order -> Instrument lineage and reap the mutable rows.

    ``insert_execution`` resolves the immutable ``(exchange, mode)`` scope
    from the ACTIVE order and instrument versions and refuses dangling or
    crossed rows, so every test ingests through this seeded lineage.
    Teardown deletes the order and instrument the test created; the
    executions are physically immutable (the append-only trigger refuses
    DELETE), so they are NOT deleted — every test's scope is keyed by a
    unique random wallet, so accumulated ledger rows never collide, and the
    throwaway scratch database is reaped by dropping it between runs.
    """
    identity = _ScopeIdentity(
        wallet_public_id=str(uuid4()),
        order_public_id=str(uuid4()),
        instrument_public_id=str(uuid4()),
        session_id=str(uuid4()),
    )
    async with repository.session() as s:
        s.add_all(
            [
                Instrument(
                    public_id=identity.instrument_public_id,
                    symbol_public_id=identity.instrument_public_id,
                    exchange=_EXCHANGE,
                    timestamp=_NOW - timedelta(days=1),
                    session_id=identity.session_id,
                    sequence_id=1,
                ),
                Order(
                    public_id=identity.order_public_id,
                    instrument_public_id=identity.instrument_public_id,
                    mode=_MODE,
                    wallet_public_id=identity.wallet_public_id,
                    created_at=_NOW - timedelta(hours=2),
                    timestamp=_NOW - timedelta(hours=2),
                    side="buy",
                    order_type="limit",
                    price=4.25,
                    size=100.0,
                    status="filled",
                    session_id=identity.session_id,
                    sequence_id=1,
                ),
            ]
        )
        await s.commit()
    try:
        yield identity
    finally:
        async with repository.session() as s:
            await s.execute(delete(Order).where(Order.public_id == identity.order_public_id))
            await s.execute(
                delete(Instrument).where(Instrument.public_id == identity.instrument_public_id)
            )
            await s.commit()


async def _ingest_fill(
    repository: SQLAlchemyRepository,
    scope: _ScopeIdentity,
    sequence_id: int,
) -> int:
    """Persist one fill through the production ingest path, returning its id."""
    return await repository.insert_execution(
        order_public_id=scope.order_public_id,
        wallet_public_id=scope.wallet_public_id,
        timestamp=_NOW - timedelta(hours=1),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=scope.session_id,
        sequence_id=sequence_id,
    )


async def _capture_watermark(repository: SQLAlchemyRepository, scope: _ScopeIdentity) -> int:
    """Run the production capture, bounded so a blocking regression FAILS.

    The sealed-prefix contract says the unlocked capture never waits on
    any writer, so a hang here is a product defect, not a slow test — the
    bound converts it into a loud ``TimeoutError`` well inside the suite
    timeout instead of a stalled worker.
    """
    watermark, _ = await asyncio.wait_for(
        repository.get_spot_execution_watermark(scope.wallet_public_id, _EXCHANGE, _MODE, _NOW),
        timeout=_CAPTURE_TIMEOUT_SECONDS,
    )
    return watermark


async def _committed_sequences(
    repository: SQLAlchemyRepository, scope: _ScopeIdentity
) -> list[int]:
    """Return the committed counter values for the scope, ascending."""
    async with repository.session() as s:
        rows = (
            await s.execute(
                select(Execution.scope_sequence)
                .where(
                    Execution.wallet_public_id == scope.wallet_public_id,
                    Execution.exchange == _EXCHANGE,
                    Execution.mode == _MODE,
                )
                .order_by(Execution.scope_sequence)
            )
        ).scalars()
        return [int(value) for value in rows]


def _fill_row(scope: _ScopeIdentity, scope_sequence: int, sequence_id: int) -> Execution:
    """Build one ledger row for out-of-band writes staged by a test session."""
    return Execution(
        order_public_id=scope.order_public_id,
        wallet_public_id=scope.wallet_public_id,
        exchange=_EXCHANGE,
        mode=_MODE,
        scope_sequence=scope_sequence,
        timestamp=_NOW - timedelta(minutes=30),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=scope.session_id,
        sequence_id=sequence_id,
    )


@pytest.mark.asyncio
async def test_capture_seals_the_prefix_against_an_in_flight_fenced_assignment(
    repository: SQLAlchemyRepository, scope: _ScopeIdentity
) -> None:
    """An uncommitted fenced assignment stays strictly above the capture.

    The sealed-by-construction witness — the exact property the old
    ``max(id)`` primitive lacked, proven on the production dialect with a
    real in-flight transaction. Session A replays the writer protocol
    verbatim (fence -> committed max+1 -> INSERT) and HOLDS uncommitted,
    because the scenario needs a mid-flight fill and the production API
    correctly never exposes one; the mocked-dialect statement tests pin
    that protocol's text and order, keeping this replica honest.

    Given: Two committed fills, then session A holding the fence with an
        uncommitted third assignment.
    When: Session B runs the unlocked production capture, session A
        commits, and session B re-reads.
    Then: The first capture returns the committed maximum 2 without
        blocking (bounded), A's pending value is strictly above it, and
        the re-read shows A's row landed at exactly W+1 — outside the
        previously captured range.
    """
    await _ingest_fill(repository, scope, 1)
    await _ingest_fill(repository, scope, 2)
    writer = repository.session_factory()
    try:
        await writer.execute(_FENCE_STATEMENT, {"wid": scope.wallet_public_id})
        pending = int(
            (
                await writer.execute(
                    select(func.coalesce(func.max(Execution.scope_sequence), 0) + 1).where(
                        Execution.wallet_public_id == scope.wallet_public_id,
                        Execution.exchange == _EXCHANGE,
                        Execution.mode == _MODE,
                    )
                )
            ).scalar_one()
        )
        writer.add(_fill_row(scope, pending, 3))
        await writer.flush()
        captured = await _capture_watermark(repository, scope)
        assert captured == 2
        assert pending > captured
        await writer.commit()
    finally:
        await writer.close()
    assert await _capture_watermark(repository, scope) == captured + 1
    assert await _committed_sequences(repository, scope) == [1, 2, 3]


@pytest.mark.asyncio
async def test_max_id_contrast_witness_shows_the_hole_the_counter_closed(
    repository: SQLAlchemyRepository, scope: _ScopeIdentity
) -> None:
    """A raw ``max(id)`` capture is NOT sealed — ids commit into its range.

    Regression witness for the superseded primitive's hole, staged
    entirely inside the test (no production code reads ``max(id)``
    anymore): id allocation order is not commit order, so a
    later-committing lower id lands INSIDE an already-captured id range.
    The staged writer takes no fence — modeling the old unfenced ingest —
    and its counter value is parked far above the committed maximum so
    only the id plane is exercised; the counter plane's sealing is proven
    by the fenced witness test, not here.

    Given: One committed fill; session A holding an uncommitted raw
        insert whose id was allocated first; then a later fill committing
        through the production path with a higher id.
    When: A raw scoped ``max(id)`` boundary is captured, and only then
        session A commits.
    Then: A's pending id is already below the captured boundary, and the
        committed population at or below the boundary GROWS after the
        capture — the sealed-prefix property fails for ids.
    """
    await _ingest_fill(repository, scope, 1)
    writer = repository.session_factory()
    try:
        staged = _fill_row(scope, 1000, 2)
        writer.add(staged)
        await writer.flush()
        pending_id = int(staged.id)
        committed_id = await _ingest_fill(repository, scope, 3)
        assert committed_id > pending_id
        async with repository.session() as s:
            boundary = int(
                (
                    await s.execute(
                        select(func.max(Execution.id)).where(
                            Execution.wallet_public_id == scope.wallet_public_id,
                            Execution.exchange == _EXCHANGE,
                            Execution.mode == _MODE,
                        )
                    )
                ).scalar_one()
            )
            population_at_capture = int(
                (
                    await s.execute(
                        select(func.count())
                        .select_from(Execution)
                        .where(
                            Execution.wallet_public_id == scope.wallet_public_id,
                            Execution.exchange == _EXCHANGE,
                            Execution.mode == _MODE,
                            Execution.id <= boundary,
                        )
                    )
                ).scalar_one()
            )
        assert boundary == committed_id
        assert pending_id < boundary
        assert population_at_capture == 2
        await writer.commit()
    finally:
        await writer.close()
    async with repository.session() as s:
        population_after_commit = int(
            (
                await s.execute(
                    select(func.count())
                    .select_from(Execution)
                    .where(
                        Execution.wallet_public_id == scope.wallet_public_id,
                        Execution.exchange == _EXCHANGE,
                        Execution.mode == _MODE,
                        Execution.id <= boundary,
                    )
                )
            ).scalar_one()
        )
    assert population_after_commit == population_at_capture + 1


@pytest.mark.asyncio
async def test_concurrent_same_wallet_generations_mint_distinct_contiguous_counters(
    repository: SQLAlchemyRepository,
    peer_repository: SQLAlchemyRepository,
    scope: _ScopeIdentity,
) -> None:
    """Overlapping executor generations serialize on the fence, gap-free.

    Models the executor restart overlap the design calls out: the old and
    the new process generation (two independent engines, separate
    connections) ingest fills for the SAME wallet at the same time. Under
    READ COMMITTED, unserialized concurrent max+1 allocations would read
    the same committed maximum and collide on the total-unique index — so
    six overlapping ingests completing without a single ``IntegrityError``
    and landing on exactly 1..6 is itself the witness that the fence
    serialized them in commit order.

    Given: Six fills released simultaneously by a barrier, three per
        engine generation, all for one wallet scope.
    When: They ingest concurrently through the production path, bounded so
        a fence deadlock fails instead of hanging.
    Then: All six commit; the committed counters are exactly 1..6 with no
        gap and no duplicate; the capture reports boundary 6.
    """
    barrier = asyncio.Barrier(6)

    async def _race(generation: SQLAlchemyRepository, sequence_id: int) -> int:
        """Hold one fill at the barrier, then ingest it through production code."""
        await barrier.wait()
        return await _ingest_fill(generation, scope, sequence_id)

    await asyncio.wait_for(
        asyncio.gather(
            _race(repository, 1),
            _race(peer_repository, 2),
            _race(repository, 3),
            _race(peer_repository, 4),
            _race(repository, 5),
            _race(peer_repository, 6),
        ),
        timeout=_RACE_TIMEOUT_SECONDS,
    )
    assert await _committed_sequences(repository, scope) == [1, 2, 3, 4, 5, 6]
    assert await _capture_watermark(repository, scope) == 6


@pytest.mark.asyncio
async def test_total_unique_index_refuses_a_fence_bypass_duplicate(
    repository: SQLAlchemyRepository, scope: _ScopeIdentity
) -> None:
    """A raw duplicate (wallet, exchange, mode, counter) fails loudly.

    The total-unique index is the fail-closed guarantee that a fence
    bypass cannot silently corrupt the counted range: on the production
    dialect the refusal must come from ``uq_executions_scope_sequence``
    itself, named in the error.

    Given: One fill ingested through the production path.
    When: A raw write bypassing the fence duplicates its counter
        coordinate.
    Then: PostgreSQL raises ``IntegrityError`` naming the total-unique
        index and the ledger keeps exactly one committed row.
    """
    await _ingest_fill(repository, scope, 1)
    duplicate_row = _fill_row(scope, 1, 2)

    async def _commit_bypassing_the_fence() -> None:
        """Write the duplicate counter coordinate through a raw session."""
        async with repository.session() as s:
            s.add(duplicate_row)
            await s.commit()

    with pytest.raises(IntegrityError, match="uq_executions_scope_sequence"):
        await _commit_bypassing_the_fence()
    assert await _committed_sequences(repository, scope) == [1]


@pytest.mark.asyncio
async def test_insert_transaction_isolation_is_explicitly_read_committed(
    inherited_repeatable_read_repository: SQLAlchemyRepository,
) -> None:
    """The insert transaction opener forces READ COMMITTED over any default.

    The engine inherits the role/database
    ``default_transaction_isolation``; the writer must never rely on that
    server configuration. The opener statement is pinned as the insert
    transaction's FIRST statement by the mocked-dialect protocol test, so
    proving the opener's effect on a live hostile-default connection
    proves the insert transaction's isolation.

    Given: A repository whose connections default to REPEATABLE READ
        (control read proves the hostile default is real).
    When: A fresh transaction runs the production insert-transaction
        opener and reads ``transaction_isolation``.
    Then: The control transaction reports ``repeatable read`` and the
        opened transaction reports ``read committed``.
    """
    repo = inherited_repeatable_read_repository
    async with repo.session() as s:
        inherited = (await s.execute(text("SHOW transaction_isolation"))).scalar_one()
        await s.rollback()
    assert inherited == "repeatable read"
    async with repo.session() as s:
        await repo._begin_execution_insert_transaction(s)
        forced = (await s.execute(text("SHOW transaction_isolation"))).scalar_one()
        await s.rollback()
    assert forced == "read committed"


@pytest.mark.asyncio
async def test_fence_waiter_allocates_the_next_value_not_a_stale_snapshot(
    repository: SQLAlchemyRepository,
    inherited_repeatable_read_repository: SQLAlchemyRepository,
    scope: _ScopeIdentity,
) -> None:
    """A waiter that queued BEFORE the holder committed reads the fresh max.

    The F3 counterexample, defeated live: the waiter's resolve SELECT
    runs BEFORE its fence wait, so an inherited REPEATABLE READ snapshot
    would survive the wait and the max read would return the STALE
    committed maximum — a duplicate allocation the UNIQUE index rejects
    AFTER the fill was already published (permanent omission). With the
    explicit READ COMMITTED opener, every statement takes a fresh
    snapshot and the waiter allocates the NEXT value.

    Given: One committed fill; a holder session holding the fence with an
        uncommitted ``max+1`` assignment; a waiter ingest through an
        engine whose connections DEFAULT to REPEATABLE READ, provably
        parked on the fence BEFORE the holder commits.
    When: The holder commits and the waiter proceeds.
    Then: The waiter commits the NEXT counter (no IntegrityError, no
        stale re-allocation) and the committed sequence is exactly
        1, 2, 3.
    """
    await _ingest_fill(repository, scope, 1)
    holder = repository.session_factory()
    try:
        await holder.execute(_FENCE_STATEMENT, {"wid": scope.wallet_public_id})
        pending = int(
            (
                await holder.execute(
                    select(func.coalesce(func.max(Execution.scope_sequence), 0) + 1).where(
                        Execution.wallet_public_id == scope.wallet_public_id,
                        Execution.exchange == _EXCHANGE,
                        Execution.mode == _MODE,
                    )
                )
            ).scalar_one()
        )
        holder.add(_fill_row(scope, pending, 2))
        await holder.flush()
        waiter = asyncio.ensure_future(_ingest_fill(inherited_repeatable_read_repository, scope, 3))
        await _wait_until_fence_blocked(repository)
        assert not waiter.done()
        await holder.commit()
        await asyncio.wait_for(waiter, timeout=_RACE_TIMEOUT_SECONDS)
    finally:
        await holder.close()
    assert await _committed_sequences(repository, scope) == [1, 2, 3]


async def _admin_connection(base_url: object) -> asyncpg.Connection:
    """Open an autocommit connection to the maintenance ``postgres`` database.

    ``CREATE``/``DROP DATABASE`` cannot run inside a transaction block, so
    the scratch-database lifecycle uses a raw ``asyncpg`` connection (which
    issues statements in autocommit) against the cluster's ``postgres``
    database rather than the session ``DB_URL`` target.
    """
    return await asyncpg.connect(
        user=base_url.username,
        password=base_url.password,
        host=base_url.host,
        port=base_url.port,
        database="postgres",
    )


@pytest.fixture
async def migration_scratch_db() -> AsyncIterator[tuple[str, Config]]:
    """Create a throwaway database migrated to revision 0028 and reap it.

    The migration fence is a property of the 0028 -> 0029 UPGRADE, so this
    scenario needs a database at exactly revision 0028 — never the shared
    session target, which is already at head and shared with the other
    tests here. A per-test uniquely named scratch database is created on
    the same cluster, brought to 0028, yielded, and dropped ``WITH
    (FORCE)`` on teardown so a connection that outlived a failed assertion
    cannot wedge the drop.
    """
    base_url = make_url(os.environ["DB_URL"])
    scratch_name = f"snapper_scope_race_{uuid4().hex}"
    creator = await _admin_connection(base_url)
    try:
        await creator.execute(f'CREATE DATABASE "{scratch_name}"')
    finally:
        await creator.close()
    scratch_url = base_url.set(database=scratch_name)
    rendered = scratch_url.render_as_string(hide_password=False)
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", rendered)
    try:
        await asyncio.to_thread(command.upgrade, config, "0028")
        yield rendered, config
    finally:
        dropper = await _admin_connection(base_url)
        try:
            await dropper.execute(f'DROP DATABASE IF EXISTS "{scratch_name}" WITH (FORCE)')
        finally:
            await dropper.close()


@pytest.mark.asyncio
async def test_migration_fence_refuses_a_crossed_wallet_writer_on_live_postgresql(
    migration_scratch_db: tuple[str, Config],
) -> None:
    """The 0029 upgrade fence blocks a live crossed-wallet 0028 writer.

    The corruption the PostgreSQL fence exists to prevent, witnessed on
    the production dialect with two genuinely concurrent connections. A
    RESOLVABLE crossed-wallet row — stored under wallet B but referencing
    wallet A's VALID active order — survives both defences the native
    ``uuid`` type and transactional DDL provide: it is representable and
    it resolves, so ``_abort_on_broken_lineage`` (which takes only
    ``ACCESS SHARE``) would pass it, and were it to commit between that
    validation read and the first DDL the backfill would derive a valid
    ``(exchange, mode)`` from order A yet PRESERVE wallet B and number it
    into wallet B's partition — certifying a fill into wallet B's
    authoritative watermark. Without the ``LOCK TABLE ... ACCESS
    EXCLUSIVE`` fence the writer's ``ROW EXCLUSIVE`` ``INSERT`` does not
    conflict with the validation ``SELECT`` and commits in that window, so
    this test FAILS (the writer COMMITS); with the fence held from the
    migration's first statement the writer conflicts and is refused.

    The verdict is a theorem, not a race: an engine-level hook parks the
    migration thread at the anchor-emptiness probe — after every lineage
    check has passed, before the first DDL, provably AFTER the ``LOCK``
    executed and still holding it — so the intruder's bounded
    ``lock_timeout`` can only ever expire against a held lock, never win
    a scheduling gap. Because the park sits past the crossed-wallet
    validation, a refusal here can only be the FENCE, never a validation
    catching the row. The park releases the moment the intruder resolves,
    letting the migration complete.

    Given: A scratch database at revision 0028 with wallet A's active
        order/instrument lineage and one canonical wallet-A execution,
        and a second connection that commits a crossed-wallet 0028-shape
        fill (stored wallet B, referencing order A) the instant the
        migration reaches the anchor-emptiness probe (past every
        validation, before the DDL).
    When: Migration 0029 upgrades.
    Then: The intruder is REFUSED with a lock timeout (proved explicitly,
        not inferred from absence), the upgrade still reaches 0029, no row
        is stored under wallet B, and wallet A's scope carries the single
        contiguous counter 1 — no crossed fill was certified.
    """
    rendered, config = migration_scratch_db
    wallet_a = str(uuid4())
    wallet_b = str(uuid4())
    order_a = str(uuid4())
    instrument = str(uuid4())
    canonical_execution = str(uuid4())
    crossed_execution = str(uuid4())
    session_id = str(uuid4())
    seed_engine = create_async_engine(rendered)
    try:
        async with seed_engine.begin() as connection:
            await connection.execute(
                text(_INSERT_INSTRUMENT_0028_SQL),
                {
                    "public_id": instrument,
                    "exchange": _EXCHANGE,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
            await connection.execute(
                text(_INSERT_ORDER_0028_SQL),
                {
                    "public_id": order_a,
                    "instrument_public_id": instrument,
                    "mode": _MODE,
                    "wallet_public_id": wallet_a,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
            await connection.execute(
                text(_INSERT_EXECUTION_0028_SQL),
                {
                    "public_id": canonical_execution,
                    "order_public_id": order_a,
                    "wallet_public_id": wallet_a,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
    finally:
        await seed_engine.dispose()

    lock_held = threading.Event()
    release_migration = threading.Event()

    def park_after_lineage_validation(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        """Hold the migration in the exact finding window while it owns the lock.

        The anchor-emptiness probe runs AFTER every lineage and
        target-domain validation has passed and BEFORE the first DDL — the
        precise window the finding describes, where the crossed-wallet
        check has already seen no offending row. Parking here means an
        unfenced intruder commits its crossed row too late for any
        validation to catch yet in time for the backfill to certify it,
        so a passing verdict proves the fence (not a validation) refused
        the writer.
        """
        if lock_held.is_set():
            return
        if " ".join(statement.split()).startswith(_ANCHOR_PROBE_PREFIX):
            lock_held.set()
            release_migration.wait(timeout=_MIGRATION_PARK_TIMEOUT_SECONDS)

    outcome: dict[str, str] = {}
    intruder_engine = create_async_engine(rendered)
    event.listen(Engine, "before_cursor_execute", park_after_lineage_validation)
    try:
        migrating = asyncio.create_task(asyncio.to_thread(command.upgrade, config, "0029"))
        parked = await asyncio.to_thread(lock_held.wait, _MIGRATION_PARK_TIMEOUT_SECONDS)
        assert parked, "migration never reached the anchor-emptiness probe holding the lock"
        try:
            async with intruder_engine.begin() as connection:
                await connection.execute(text(f"SET lock_timeout = '{_INTRUDER_LOCK_TIMEOUT}'"))
                await connection.execute(
                    text(_INSERT_EXECUTION_0028_SQL),
                    {
                        "public_id": crossed_execution,
                        "order_public_id": order_a,
                        "wallet_public_id": wallet_b,
                        "session_id": session_id,
                        "timestamp": _NOW,
                        "known_to": _PG_ACTIVE,
                    },
                )
            outcome["result"] = "COMMITTED"
        except DBAPIError as exc:
            outcome["result"] = f"BLOCKED: {exc}"
        finally:
            release_migration.set()
        await asyncio.wait_for(migrating, timeout=_MIGRATION_PARK_TIMEOUT_SECONDS)
    finally:
        event.remove(Engine, "before_cursor_execute", park_after_lineage_validation)
        await intruder_engine.dispose()

    assert outcome["result"].startswith(
        "BLOCKED"
    ), f"crossed writer not fenced: {outcome['result']}"
    assert "lock timeout" in outcome["result"]
    verify_engine = create_async_engine(rendered)
    try:
        async with verify_engine.connect() as connection:
            version = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
            crossed_rows = (
                await connection.execute(
                    text("SELECT COUNT(*) FROM executions WHERE wallet_public_id = :wallet"),
                    {"wallet": wallet_b},
                )
            ).scalar_one()
            wallet_a_counters = (
                (
                    await connection.execute(
                        text(
                            "SELECT scope_sequence FROM executions WHERE "
                            "wallet_public_id = :wallet AND exchange = :exchange "
                            "AND mode = :mode ORDER BY scope_sequence"
                        ),
                        {"wallet": wallet_a, "exchange": _EXCHANGE, "mode": _MODE},
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await verify_engine.dispose()
    assert version == "0029"
    assert int(crossed_rows) == 0, "a crossed-wallet fill was certified into wallet B"
    assert [int(value) for value in wallet_a_counters] == [1]


@pytest.mark.asyncio
async def test_migration_named_validator_catches_crossed_writer_under_hostile_isolation(
    migration_scratch_db: tuple[str, Config],
) -> None:
    """A REPEATABLE READ default cannot hide a crossed writer from the validator.

    The N4 hazard, defeated live. The production engine configures no
    isolation, so a migration inherits whatever
    ``default_transaction_isolation`` the role or database sets. Under an
    inherited REPEATABLE READ default the migration transaction's snapshot
    freezes at Alembic's ``alembic_version`` read — BEFORE ``upgrade``
    runs — so a crossed-wallet writer that commits AFTER that read but
    BEFORE the fence ``LOCK`` would be INVISIBLE to
    ``_abort_on_broken_lineage``: the named fail-closed refusal would not
    fire, and only the later ``SET NOT NULL`` DDL would force a rollback —
    a probabilistic backstop, not the clean fail-closed theorem the
    validators exist to guarantee. ``env.py`` forces READ COMMITTED on the
    migration connection before that first read, so every statement takes
    a fresh snapshot and the validator — reading after the ACCESS
    EXCLUSIVE fence has drained in-flight writers — is authoritative.

    The verdict is a theorem, not a race: the scratch database default is
    set to REPEATABLE READ (proved genuinely in effect on a control
    connection), and an engine-level hook parks the migration at the fence
    ``LOCK`` statement — AFTER the ``alembic_version`` read has frozen any
    inherited snapshot, BEFORE the migration holds any lock — so the
    intruder's crossed-wallet INSERT commits deterministically inside the
    exact window the finding describes. Because the migration holds no
    executions lock while parked, the intruder COMMITS (it is not fenced
    out — that is the point: a committed writer that a frozen snapshot
    would hide). That the migration then refuses with the NAMED
    ``crossed wallet lineage`` error BEFORE any DDL — revision still 0028,
    no scope columns added — proves READ COMMITTED overrode the hostile
    default and made the validator, not a late DDL rollback, the authority.

    Given: A scratch database at revision 0028 whose default isolation is
        REPEATABLE READ, holding wallet A's active order/instrument
        lineage and one canonical wallet-A execution, and a second
        connection that commits a crossed-wallet 0028-shape fill (stored
        wallet B, referencing order A) the instant the migration parks at
        the fence LOCK — after the version read, before any lock.
    When: Migration 0029 upgrades.
    Then: The intruder COMMITS (proved, not inferred), the upgrade refuses
        with the NAMED ``crossed wallet lineage`` RuntimeError, the stamped
        revision stays 0028 with no scope columns added, and the crossed
        row is still present — caught by the validator, never certified.
    """
    rendered, config = migration_scratch_db
    scratch_name = make_url(rendered).database
    base_url = make_url(rendered)
    admin = await _admin_connection(base_url)
    try:
        await admin.execute(
            f'ALTER DATABASE "{scratch_name}" SET default_transaction_isolation '
            f"= '{_HOSTILE_ISOLATION}'"
        )
    finally:
        await admin.close()

    control_engine = create_async_engine(rendered)
    try:
        async with control_engine.connect() as connection:
            default_isolation = (
                await connection.execute(text("SHOW default_transaction_isolation"))
            ).scalar_one()
    finally:
        await control_engine.dispose()
    assert default_isolation == _HOSTILE_ISOLATION, "hostile default not in effect"

    wallet_a = str(uuid4())
    wallet_b = str(uuid4())
    order_a = str(uuid4())
    instrument = str(uuid4())
    canonical_execution = str(uuid4())
    crossed_execution = str(uuid4())
    session_id = str(uuid4())
    seed_engine = create_async_engine(rendered)
    try:
        async with seed_engine.begin() as connection:
            await connection.execute(
                text(_INSERT_INSTRUMENT_0028_SQL),
                {
                    "public_id": instrument,
                    "exchange": _EXCHANGE,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
            await connection.execute(
                text(_INSERT_ORDER_0028_SQL),
                {
                    "public_id": order_a,
                    "instrument_public_id": instrument,
                    "mode": _MODE,
                    "wallet_public_id": wallet_a,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
            await connection.execute(
                text(_INSERT_EXECUTION_0028_SQL),
                {
                    "public_id": canonical_execution,
                    "order_public_id": order_a,
                    "wallet_public_id": wallet_a,
                    "session_id": session_id,
                    "timestamp": _NOW,
                    "known_to": _PG_ACTIVE,
                },
            )
    finally:
        await seed_engine.dispose()

    parked_at_lock = threading.Event()
    release_migration = threading.Event()

    def park_before_the_fence_lock(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        """Hold the migration at the fence LOCK — version read done, no lock yet.

        Parking BEFORE the ``LOCK`` executes places the migration in the
        exact N4 window: the ``alembic_version`` read has already frozen
        any inherited REPEATABLE READ snapshot, but the migration owns no
        executions lock, so the intruder's crossed INSERT commits freely.
        Firing once keeps the release-side LOCK re-execution from
        re-parking.
        """
        if parked_at_lock.is_set():
            return
        if " ".join(statement.split()).startswith(_FENCE_LOCK_PREFIX):
            parked_at_lock.set()
            release_migration.wait(timeout=_MIGRATION_PARK_TIMEOUT_SECONDS)

    outcome: dict[str, str] = {}
    intruder_engine = create_async_engine(rendered)
    event.listen(Engine, "before_cursor_execute", park_before_the_fence_lock)
    migration_error: dict[str, BaseException] = {}
    try:
        migrating = asyncio.create_task(asyncio.to_thread(command.upgrade, config, "0029"))
        parked = await asyncio.to_thread(parked_at_lock.wait, _MIGRATION_PARK_TIMEOUT_SECONDS)
        assert parked, "migration never parked at the fence LOCK"
        try:
            async with intruder_engine.begin() as connection:
                await connection.execute(
                    text(_INSERT_EXECUTION_0028_SQL),
                    {
                        "public_id": crossed_execution,
                        "order_public_id": order_a,
                        "wallet_public_id": wallet_b,
                        "session_id": session_id,
                        "timestamp": _NOW,
                        "known_to": _PG_ACTIVE,
                    },
                )
            outcome["result"] = "COMMITTED"
        except DBAPIError as exc:
            outcome["result"] = f"BLOCKED: {exc}"
        finally:
            release_migration.set()
        try:
            await asyncio.wait_for(migrating, timeout=_MIGRATION_PARK_TIMEOUT_SECONDS)
        except RuntimeError as exc:
            migration_error["error"] = exc
    finally:
        event.remove(Engine, "before_cursor_execute", park_before_the_fence_lock)
        await intruder_engine.dispose()

    assert outcome["result"] == "COMMITTED", f"intruder did not commit: {outcome['result']}"
    assert "error" in migration_error, "migration did not refuse the crossed writer"
    assert "crossed wallet lineage" in str(migration_error["error"])

    verify_engine = create_async_engine(rendered)
    try:
        async with verify_engine.connect() as connection:
            version = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
            scope_columns = (
                await connection.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.columns WHERE "
                        "table_name = 'executions' AND column_name = 'scope_sequence'"
                    )
                )
            ).scalar_one()
            crossed_rows = (
                await connection.execute(
                    text("SELECT COUNT(*) FROM executions WHERE wallet_public_id = :wallet"),
                    {"wallet": wallet_b},
                )
            ).scalar_one()
    finally:
        await verify_engine.dispose()
    assert version == "0028", "named validator did not abort before the revision stamp"
    assert int(scope_columns) == 0, "DDL ran — refusal was a late rollback, not the validator"
    assert int(crossed_rows) == 1, "the crossed writer's committed row vanished"
