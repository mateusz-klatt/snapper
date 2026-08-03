"""Tests for the central immutable-ledger guard on the executions table.

These tests prove ONE theorem, not five call-site checks: the physical
``executions`` table is never mutated by any generic mutation primitive,
regardless of how the caller spells the target. The guard resolves the
target to its physical table name (via ``sqlalchemy.inspect`` /
``Mapper.local_table`` / ``AliasedInsp.mapper`` / ``InstanceState.mapper``)
and refuses the registered append-only ledger. The prior ``model is
Execution`` identity checks were bypassable — ``aliased(Execution)`` is
not ``Execution`` and ``Execution.__table__`` is not ``Execution`` — so
each guarded primitive is exercised here with the mapped class, an
``aliased`` proxy, AND the bare ``Table`` object, and every spelling must
be refused. A final test proves the legitimate execution ingest path
(``insert_execution``) still works, because it persists via a direct
fenced write and does not route through any guarded primitive.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import Table
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy.orm import aliased

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Tick
from snapper.data.repository import IMMUTABLE_LEDGER_TABLE_NAMES
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import ExecutionPhysicalMutationError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _guard_immutable_ledger
from snapper.data.repository import _resolve_physical_table_name
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_sync

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-00000000c101"
_SESSION = "00000000-0000-7000-8000-000000000301"
_INSTRUMENT = "00000000-0000-7000-8000-000000000501"
_ORDER = "00000000-0000-7000-8000-000000000601"


def _make_sync_repo_with_one_execution(tmp_path: Path) -> tuple[DatabaseRepository, int]:
    """Create a sync repository holding a single certified execution row.

    The row stands in for a sealed prefix entry: ``scope_sequence`` 3
    inside its ``(wallet, exchange, mode)`` scope, exactly the tip a
    watermark would have captured.

    Returns:
        Tuple of (repository, execution primary-key id).
    """
    repo = DatabaseRepository(f"sqlite:///{tmp_path / 'ledger.db'}")
    cast(Table, Execution.__table__).create(repo.engine)
    with repo.get_session() as session:
        execution = Execution(
            public_id="exec-pub-3",
            order_public_id=_ORDER,
            wallet_public_id=_WALLET,
            exchange="kraken",
            mode="live",
            scope_sequence=3,
            side="buy",
            status="filled",
            price=100.0,
            size=1.0,
            fee=0.1,
            fee_asset="USD",
            session_id=_SESSION,
            sequence_id=1,
            timestamp=_NOW,
            known_to=KNOWN_TO_MAX,
        )
        session.add(execution)
        session.commit()
        row_id = execution.id
    return repo, row_id


def _archive_candidate() -> dict[str, object]:
    """Return one archive-shaped execution row for reintroduction attempts."""
    return {
        "public_id": "exec-pub-2",
        "order_public_id": _ORDER,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 2,
        "side": "buy",
        "status": "filled",
        "price": 99.0,
        "size": 1.0,
        "fee": 0.1,
        "fee_asset": "USD",
        "session_id": _SESSION,
        "sequence_id": 2,
        "timestamp": datetime(2024, 1, 1, 14, 29, tzinfo=UTC),
        "known_to": KNOWN_TO_MAX,
    }


@pytest.fixture
async def async_repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an async repository over the scope-plane tables with lineage.

    Seeds one active Instrument and Order version so ``insert_execution``
    can resolve a fill's scope from the active Order -> Instrument lineage.
    """
    db_path = tmp_path / "ledger-async.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    cast(Table, Instrument.__table__).create(schema_engine)
    cast(Table, Order.__table__).create(schema_engine)
    cast(Table, Execution.__table__).create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                Instrument(
                    public_id=_INSTRUMENT,
                    symbol_public_id=_INSTRUMENT,
                    exchange="walutomat",
                    timestamp=_NOW - timedelta(days=1),
                    session_id=_SESSION,
                    sequence_id=1,
                ),
                Order(
                    public_id=_ORDER,
                    instrument_public_id=_INSTRUMENT,
                    mode="live",
                    wallet_public_id=_WALLET,
                    created_at=_NOW - timedelta(hours=2),
                    timestamp=_NOW - timedelta(hours=2),
                    side="buy",
                    order_type="limit",
                    price=4.25,
                    size=100.0,
                    status="filled",
                    session_id=_SESSION,
                    sequence_id=1,
                ),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def test_registry_holds_the_executions_physical_table_name() -> None:
    """The registry names the physical executions table, not a class.

    Given: The immutable-ledger registry.
    When: Its membership is inspected.
    Then: It contains the physical ``executions`` table name, so the
        guard is a theorem about the table rather than a class identity.
    """
    assert Execution.__tablename__ == "executions"
    assert "executions" in IMMUTABLE_LEDGER_TABLE_NAMES


def test_resolver_maps_the_mapped_class_to_its_physical_table() -> None:
    """A mapped class inspects to a Mapper and yields its physical table.

    Given: The ``Execution`` mapped class.
    When: It is resolved.
    Then: The physical table name ``executions`` is returned.
    """
    assert _resolve_physical_table_name(Execution) == "executions"


def test_resolver_maps_an_aliased_class_to_its_physical_table() -> None:
    """An aliased class inspects to an AliasedInsp and yields its table.

    Given: ``aliased(Execution)`` — the spelling that bypassed the old
        ``is Execution`` identity guard.
    When: It is resolved.
    Then: The physical table name ``executions`` is returned, closing the
        alias bypass.
    """
    assert _resolve_physical_table_name(aliased(Execution)) == "executions"


def test_resolver_maps_the_table_object_to_its_physical_table() -> None:
    """A bare Table inspects to itself and yields its own name.

    Given: ``Execution.__table__`` — the spelling that bypassed the old
        identity guard on the insert primitives.
    When: It is resolved.
    Then: The physical table name ``executions`` is returned, closing the
        ``__table__`` bypass.
    """
    assert _resolve_physical_table_name(cast(Table, Execution.__table__)) == "executions"


def test_resolver_maps_a_lightweight_table_clause_to_its_physical_table() -> None:
    """A lightweight ``table("executions", ...)`` resolves to its name.

    Given: ``sqlalchemy.table("executions", ...)`` — a bare ``TableClause``
        that ``insert``/``delete`` accept as a DML target. It inspects to
        itself, is NOT an instance of ``Table`` (``Table`` is a subclass of
        ``TableClause``), and previously fell through the resolver to
        ``None`` so the guard failed OPEN and let a caller mutate the
        executions ledger.
    When: It is resolved.
    Then: The physical table name ``executions`` is returned, closing the
        lightweight-clause bypass.
    """
    clause = table("executions", *(column(col.name) for col in Execution.__table__.columns))
    assert _resolve_physical_table_name(clause) == "executions"


def test_guard_refuses_a_lightweight_table_clause_aimed_at_the_ledger() -> None:
    """The guard refuses the ``table("executions", ...)`` DML target.

    Given: A lightweight ``TableClause`` naming the executions ledger.
    When: It is passed to the central guard.
    Then: The guard raises, so neither ``_upsert_batch`` nor
        ``bulk_insert_from_archive`` can emit ``INSERT INTO executions``
        through this spelling.
    """
    clause = table("executions", *(column(col.name) for col in Execution.__table__.columns))
    with pytest.raises(ExecutionPhysicalMutationError):
        _guard_immutable_ledger(clause, "ledger insert refused")


def test_guard_fails_closed_on_an_unresolvable_target() -> None:
    """An unresolvable mutation target is refused, never passed.

    Given: A plain object that resolves to no physical table name.
    When: It is passed to the central guard.
    Then: The guard raises fail-closed, so no future unforeseen spelling
        that resolves to ``None`` can reach a mutation primitive against
        the ledger the way the lightweight ``TableClause`` once did. A
        generic primitive is only ever legitimately handed a mapped class
        or a table, both of which resolve, so refusing the unresolvable
        target breaks no legitimate caller.
    """
    with pytest.raises(ExecutionPhysicalMutationError):
        _guard_immutable_ledger(object(), "unresolved target refused")


def test_guard_allows_a_non_ledger_mapped_class() -> None:
    """A resolvable non-ledger target passes the guard.

    Given: The ``Instrument`` mapped class, which resolves to a physical
        table outside the immutable-ledger registry.
    When: It is passed to the central guard.
    Then: The guard does not raise, proving fail-closed refuses only
        ledger and unresolvable targets — not every generic mutation.
    """
    _guard_immutable_ledger(Instrument, "must not raise")


def test_resolver_maps_a_mapped_instance_to_its_physical_table() -> None:
    """A mapped instance inspects to an InstanceState and yields its table.

    Given: A constructed ``Execution`` instance.
    When: It is resolved.
    Then: The physical table name ``executions`` is returned.
    """
    instance = Execution(
        public_id="exec-pub-x",
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        exchange="kraken",
        mode="live",
        scope_sequence=1,
        side="buy",
        status="filled",
        price=1.0,
        size=1.0,
        fee=0.0,
        fee_asset="USD",
        session_id=_SESSION,
        sequence_id=1,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )
    assert _resolve_physical_table_name(instance) == "executions"


def test_resolver_returns_the_raw_string_verbatim() -> None:
    """A raw table-name string has no inspection path and passes through.

    Given: The raw string ``"executions"``.
    When: It is resolved.
    Then: The same string is returned, so a string target is also matched
        against the registry.
    """
    assert _resolve_physical_table_name("executions") == "executions"


def test_resolver_returns_none_for_a_non_inspectable_target() -> None:
    """A target with no mapped table resolves to None and is not a ledger.

    Given: A plain object with no SQLAlchemy inspection.
    When: It is resolved.
    Then: ``None`` is returned, so the guard leaves non-mapped targets to
        pass rather than raising.
    """
    assert _resolve_physical_table_name(object()) is None


def test_resolver_maps_a_non_ledger_model_to_its_own_table() -> None:
    """A non-ledger model resolves to its own table and is not registered.

    Given: The ``Tick`` model, which is not an immutable ledger.
    When: It is resolved.
    Then: Its physical table name is returned and is absent from the
        registry, so the guard passes it through.
    """
    resolved = _resolve_physical_table_name(Tick)
    assert resolved == Tick.__tablename__
    assert resolved not in IMMUTABLE_LEDGER_TABLE_NAMES


def test_guard_raises_for_every_execution_spelling() -> None:
    """The central guard refuses the ledger under all four spellings.

    Given: The shared ``_guard_immutable_ledger`` and the class, aliased,
        table, and string spellings of the executions ledger.
    When: Each is passed to the guard.
    Then: Every spelling raises ``ExecutionPhysicalMutationError``, so the
        refusal is a theorem about the physical table.
    """
    for target in (
        Execution,
        aliased(Execution),
        cast(Table, Execution.__table__),
        "executions",
    ):
        with pytest.raises(ExecutionPhysicalMutationError, match="refused"):
            _guard_immutable_ledger(target, "refused")


def test_guard_passes_for_a_non_ledger_model() -> None:
    """The central guard is transparent to non-ledger models.

    Given: The ``Tick`` model.
    When: It is passed to the guard.
    Then: No error is raised, so generic primitives keep working for every
        table that is not a registered ledger.
    """
    _guard_immutable_ledger(Tick, "unused")


def test_delete_rows_by_id_refuses_aliased_execution(tmp_path: Path) -> None:
    """Physical delete refuses an aliased executions target, preserving rows.

    Concrete bypass this closes: ``delete(aliased(Execution)).where(...)``
    emits ``DELETE FROM executions`` while ``aliased(Execution) is
    Execution`` is False, so the old identity guard let it through.

    Given: A DB holding one certified execution row.
    When: ``delete_rows_by_id`` is called with ``aliased(Execution)``.
    Then: ``ExecutionPhysicalMutationError`` is raised and the row survives.
    """
    repo, row_id = _make_sync_repo_with_one_execution(tmp_path)
    s5778_value_1 = cast(type[object], aliased(Execution))
    with pytest.raises(
        ExecutionPhysicalMutationError, match="physical execution deletion is refused"
    ):
        repo.delete_rows_by_id(s5778_value_1, [row_id])
    with repo.get_session() as session:
        surviving = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert surviving == 1
    repo.dispose()


def test_delete_rows_by_id_refuses_execution_table_object(tmp_path: Path) -> None:
    """Physical delete refuses the bare executions Table, preserving rows.

    Given: A DB holding one certified execution row.
    When: ``delete_rows_by_id`` is called with ``Execution.__table__``.
    Then: ``ExecutionPhysicalMutationError`` is raised and the row survives.
    """
    repo, row_id = _make_sync_repo_with_one_execution(tmp_path)
    s5778_value_1 = cast(type[object], Execution.__table__)
    with pytest.raises(
        ExecutionPhysicalMutationError, match="physical execution deletion is refused"
    ):
        repo.delete_rows_by_id(s5778_value_1, [row_id])
    with repo.get_session() as session:
        surviving = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert surviving == 1
    repo.dispose()


def test_bulk_insert_from_archive_refuses_aliased_execution(tmp_path: Path) -> None:
    """Bulk insert refuses an aliased executions target, inserting nothing.

    Given: A DB holding one certified execution row.
    When: ``bulk_insert_from_archive`` is called with ``aliased(Execution)``.
    Then: ``ExecutionPhysicalMutationError`` is raised and no row is added.
    """
    repo, _row_id = _make_sync_repo_with_one_execution(tmp_path)
    s5778_value_1 = cast(type[object], aliased(Execution))
    s5778_value_2 = _archive_candidate()
    with pytest.raises(
        ExecutionPhysicalMutationError, match="physical execution reintroduction is refused"
    ):
        repo.bulk_insert_from_archive(s5778_value_1, [s5778_value_2])
    with repo.get_session() as session:
        count = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert count == 1
    repo.dispose()


def test_bulk_insert_from_archive_refuses_execution_table_object(tmp_path: Path) -> None:
    """Bulk insert refuses the bare executions Table, inserting nothing.

    Concrete bypass this closes: ``insert(Execution.__table__)`` accepts a
    ``Table`` and inserts into ``executions`` while ``Execution.__table__
    is Execution`` is False, so the old identity guard let it through.

    Given: A DB holding one certified execution row.
    When: ``bulk_insert_from_archive`` is called with ``Execution.__table__``.
    Then: ``ExecutionPhysicalMutationError`` is raised and no row is added.
    """
    repo, _row_id = _make_sync_repo_with_one_execution(tmp_path)
    s5778_value_1 = cast(type[object], Execution.__table__)
    s5778_value_2 = _archive_candidate()
    with pytest.raises(
        ExecutionPhysicalMutationError, match="physical execution reintroduction is refused"
    ):
        repo.bulk_insert_from_archive(s5778_value_1, [s5778_value_2])
    with repo.get_session() as session:
        count = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert count == 1
    repo.dispose()


def test_close_and_insert_sync_refuses_execution(tmp_path: Path) -> None:
    """The sync SCD2 supersede primitive refuses the executions ledger.

    Given: A sync session over the executions table.
    When: ``close_and_insert_sync`` is called with the ``Execution`` model.
    Then: ``ExecutionPhysicalMutationError`` is raised before any query, so
        no committed ``scope_sequence`` can be superseded in place.
    """
    repo, _row_id = _make_sync_repo_with_one_execution(tmp_path)
    with (
        repo.get_session() as session,
        pytest.raises(
            ExecutionPhysicalMutationError, match="physical execution supersede is refused"
        ),
    ):
        close_and_insert_sync(session, Execution, [], {}, _NOW)
    repo.dispose()


@pytest.mark.asyncio
async def test_close_and_insert_refuses_every_execution_spelling(
    async_repo: SQLAlchemyRepository,
) -> None:
    """The async SCD2 supersede primitive refuses every executions spelling.

    Given: An async session over the executions table.
    When: ``close_and_insert`` is called with the mapped class, an aliased
        proxy, and the bare Table.
    Then: Each spelling raises ``ExecutionPhysicalMutationError`` before any
        query, closing the supersede vector under every target shape.
    """
    async with async_repo.session() as session:
        for target in (Execution, aliased(Execution), cast(Table, Execution.__table__)):
            with pytest.raises(
                ExecutionPhysicalMutationError, match="physical execution supersede is refused"
            ):
                await close_and_insert(session, target, [], {}, _NOW)


@pytest.mark.asyncio
async def test_upsert_batch_refuses_every_execution_spelling(
    async_repo: SQLAlchemyRepository,
) -> None:
    """The batch upsert primitive refuses every executions spelling.

    Given: The async repository over the executions table.
    When: ``_upsert_batch`` is called with the mapped class, an aliased
        proxy, and the bare Table.
    Then: Each spelling raises ``ExecutionPhysicalMutationError`` before the
        statement is built, so no conflict-do-nothing insert reaches the
        ledger under any target shape.
    """
    for target in (Execution, aliased(Execution), cast(Table, Execution.__table__)):
        s5778_value_1 = cast(type[Execution], target)
        s5778_value_2 = _archive_candidate()
        with pytest.raises(
            ExecutionPhysicalMutationError, match="physical execution upsert is refused"
        ):
            await async_repo._upsert_batch(
                s5778_value_1,
                [s5778_value_2],
                ["wallet_public_id", "exchange", "mode", "scope_sequence"],
            )


@pytest.mark.asyncio
async def test_upsert_batch_refuses_a_gap_below_the_watermark(
    async_repo: SQLAlchemyRepository,
) -> None:
    """The batch upsert primitive refuses an arbitrary counter gap insert.

    Concrete hazard this closes: ``_upsert_batch`` is append-only, but it
    can INSERT a caller-chosen ``scope_sequence`` below the watermark —
    a gap the counted range proof cannot detect on the insert side.

    Given: One committed fill at counter 1 (watermark 1).
    When: ``_upsert_batch`` is asked to insert a row at gap counter 5.
    Then: ``ExecutionPhysicalMutationError`` is raised and the ledger keeps
        exactly the one legitimately ingested row.
    """
    await async_repo.insert_execution(
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_NOW - timedelta(hours=1),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=1,
    )
    gap_row = _archive_candidate()
    gap_row["scope_sequence"] = 5
    with pytest.raises(
        ExecutionPhysicalMutationError, match="physical execution upsert is refused"
    ):
        await async_repo._upsert_batch(
            Execution,
            [gap_row],
            ["wallet_public_id", "exchange", "mode", "scope_sequence"],
        )
    async with async_repo.session() as s:
        count = (await s.execute(select(func.count()).select_from(Execution))).scalar_one()
    assert count == 1


@pytest.mark.asyncio
async def test_legitimate_execution_ingest_still_works(
    async_repo: SQLAlchemyRepository,
) -> None:
    """The legit ingest path survives the blanket primitive refusal.

    The guard blocks the generic primitives, but ``insert_execution``
    persists via a direct fenced ``session.add`` after allocating
    ``scope_sequence`` under the per-wallet fence — it does not route
    through any guarded primitive. This is the load-bearing check that the
    theorem does not break production ingest.

    Given: A seeded Order -> Instrument lineage.
    When: Two fills are ingested through ``insert_execution``.
    Then: Both persist with contiguous counters 1 and 2 in their scope.
    """
    first = await async_repo.insert_execution(
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_NOW - timedelta(hours=1),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=1,
    )
    second = await async_repo.insert_execution(
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_NOW - timedelta(minutes=30),
        side="sell",
        status="filled",
        price=4.30,
        size=50.0,
        fee=0.25,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=2,
    )
    assert first != second
    async with async_repo.session() as s:
        counters = (
            (await s.execute(select(Execution.scope_sequence).order_by(Execution.id)))
            .scalars()
            .all()
        )
    assert list(counters) == [1, 2]
