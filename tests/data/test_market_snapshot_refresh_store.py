"""Exercise atomic selected snapshot transactions and exact version proof on isolated SQLite."""

import asyncio
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL
from uuid import uuid5

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.market_snapshot_refresh import freshness as freshness_module
from snapper.application.pricing import usd_converter as converter_module
from snapper.core import market_snapshot_freshness as freshness_policy
from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshot
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotBatch
from snapper.core.market_snapshot_refresh_scope import FrozenRefreshScope
from snapper.core.market_snapshot_refresh_scope import RefreshTargetMetadata
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.data import market_snapshot_refresh_store as store_module
from snapper.data.market_snapshot_refresh_rows import revalidate_refresh_aliases
from snapper.data.market_snapshot_refresh_rows import snapshot_operand
from snapper.data.market_snapshot_refresh_rows import verify_refresh_receipt
from snapper.data.market_snapshot_refresh_store import MarketSnapshotRefreshStore
from snapper.data.market_snapshot_refresh_transactions import lock_refresh_metadata
from snapper.data.market_snapshot_refresh_transactions import lock_refresh_snapshots
from snapper.data.market_snapshot_refresh_transactions import refresh_transaction
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _market_snapshot_active_rows_query

_NOW = datetime(2090, 1, 2, 3, 4, 5, tzinfo=UTC)
_START = _NOW - timedelta(seconds=1)


def _id(name: str) -> str:
    """Derive stable UUID fixture identities without depending on runtime data."""
    return str(uuid5(NAMESPACE_URL, f"atomic-snapshot-refresh:{name}"))


def _budget() -> RefreshBudget:
    """Use a fixed synthetic clock for individual isolated operations."""
    return RefreshBudget(deadline=45.0, clock=lambda: 0.0)


def _metadata(name: str, exchange: str = "kraken") -> RefreshTargetMetadata:
    """Describe a required target or an excluded competing FX venue."""
    return {
        "instrument_public_id": _id(name),
        "native_symbol": "BTC/EUR" if name == "main" else f"EUR/USD-{name}",
        "exchange": exchange,
        "base": "BTC" if name == "main" else "EUR",
        "quote": "EUR" if name == "main" else "USD",
        "asset_type": "crypto" if name == "main" else "forex",
    }


def _scope() -> FrozenRefreshScope:
    """Freeze the complete required and excluded metadata universe."""
    return freeze_refresh_scope(
        [_id("main")],
        ["kraken"],
        [_metadata("main"), _metadata("fx"), _metadata("excluded", "polygon")],
    )


def _operand(name: str, timestamp: datetime = _NOW) -> RefreshSnapshot:
    """Create a synthetic collected operand with distinct producer sequence provenance."""
    return RefreshSnapshot(
        instrument_public_id=_id(name),
        timestamp=timestamp,
        session_id=_id("run"),
        sequence_id={"main": 1, "fx": 2, "excluded": 3}.get(name, 4),
        last_price=10.0,
        bid=9.0,
        ask=11.0,
    )


def _alias_proofs(scope: FrozenRefreshScope) -> tuple[RefreshAliasProof, ...]:
    """Bind deterministic fixture alias rows created in required-instrument order."""
    targets = [row for row in scope.required if row.exchange == "kraken"]
    return tuple(
        RefreshAliasProof(
            instrument_public_id=row.instrument_public_id,
            symbol_public_id=_id(row.instrument_public_id),
            alias_public_id=_id(f"alias-{row.instrument_public_id}"),
            alias_row_id=index,
            exchange="kraken",
            channel="ws",
            exchange_symbol=f"{row.base}/{row.quote}",
        )
        for index, row in enumerate(targets, start=1)
    )


def _batch(scope: FrozenRefreshScope, timestamp: datetime = _NOW) -> RefreshSnapshotBatch:
    """Build an exact complete batch including the original subscription start."""
    return RefreshSnapshotBatch(
        scope_digest=scope.scope_digest,
        collection_started_at=_START,
        alias_proofs=_alias_proofs(scope),
        snapshots=(_operand("main", timestamp), _operand("fx", timestamp)),
    )


@dataclass(frozen=True)
class _Fixture:
    """Two readers and a store sharing only the temporary test database."""

    sync: DatabaseRepository
    asynchronous: SQLAlchemyRepository
    store: MarketSnapshotRefreshStore
    scope: FrozenRefreshScope


def _seed_target(repository: DatabaseRepository, metadata: RefreshTargetMetadata) -> None:
    """Seed complete active resolver metadata for one synthetic market-data target."""
    identity = metadata["instrument_public_id"]
    symbol_id = _id(identity)
    common = {
        "timestamp": _NOW - timedelta(minutes=10),
        "session_id": _id("seed"),
        "sequence_id": 1,
    }
    with repository.get_session() as session:
        session.add(
            Symbol(
                public_id=symbol_id,
                native_symbol=metadata["native_symbol"],
                base=metadata["base"],
                quote=metadata["quote"],
                asset_type=metadata["asset_type"],
                created_at=_NOW,
                **common,
            )
        )
        session.add(
            Instrument(
                public_id=identity,
                symbol_public_id=symbol_id,
                exchange=metadata["exchange"],
                **common,
            )
        )
        session.add(
            SymbolExchangeCapability(
                public_id=_id(f"cap-{identity}"),
                symbol_public_id=symbol_id,
                exchange=metadata["exchange"],
                can_market_data=True,
                can_trade=False,
                created_at=_NOW,
                **common,
            )
        )
        if metadata["exchange"] == "kraken":
            session.add(
                SymbolAlias(
                    public_id=_id(f"alias-{identity}"),
                    symbol_public_id=symbol_id,
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol=f"{metadata['base']}/{metadata['quote']}",
                    created_at=_NOW,
                    **common,
                )
            )
        session.commit()


@pytest.fixture
async def fixture(tmp_path: Path) -> AsyncIterator[_Fixture]:
    """Create only the five preexisting market and alias tables in a test-owned database."""
    path = tmp_path / "atomic-refresh.db"
    synchronous = DatabaseRepository(f"sqlite:///{path}")
    asynchronous = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path}")
    for model in (Symbol, Instrument, SymbolExchangeCapability, SymbolAlias, MarketSnapshot):
        model.__table__.create(synchronous.engine)
    scope = _scope()
    for row in (*scope.required, *scope.excluded):
        _seed_target(synchronous, row.metadata())
    try:
        yield _Fixture(
            synchronous, asynchronous, MarketSnapshotRefreshStore(asynchronous, lambda: _NOW), scope
        )
    finally:
        await asynchronous.engine.dispose()
        synchronous.engine.dispose()


def _rows(fixture: _Fixture) -> list[MarketSnapshot]:
    """Load detached versions for durable post-operation assertions."""
    with fixture.sync.get_session() as session:
        return list(session.scalars(select(MarketSnapshot).order_by(MarketSnapshot.id)).all())


async def _persist(fixture: _Fixture) -> RefreshCommitReceipt:
    """Atomically persist a complete batch under its producer session identity."""
    return await fixture.store.persist(_id("run"), fixture.scope, _batch(fixture.scope), _budget())


def _seed_old_snapshots(fixture: _Fixture, timestamp: datetime) -> None:
    """Insert previous versions from an unrelated producer session."""
    with fixture.sync.get_session() as session:
        for name in ("main", "fx"):
            operand = _operand(name, timestamp).model_copy(update={"session_id": _id("old")})
            session.add(MarketSnapshot(**operand.model_dump(), public_id=_id(f"old-{name}")))
        session.commit()


async def test_exact_receipt_proves_complete_committed_batch(fixture: _Fixture) -> None:
    """Version proof binds exact rows, scope, producer session, sequence and all operands.

    Given: A complete explicit scope, current aliases and one collected batch.
    When: The batch is committed and its exact physical versions are read back.
    Then: The receipt is verified and repeating the same timestamps is refused.
    """
    receipt = await _persist(fixture)
    assert len(receipt.snapshots) == 2
    assert receipt.recorded_at == _NOW and receipt.collection_started_at == _START
    assert receipt.batch_digest == _batch(fixture.scope).batch_digest
    assert await fixture.store.verify(fixture.scope, receipt, _budget()) == receipt
    assert len(_rows(fixture)) == 2
    with pytest.raises(ValueError, match="strictly newer"):
        await _persist(fixture)


async def test_strict_newer_scd2_preserves_original_identity(fixture: _Fixture) -> None:
    """The complete update closes all previous versions and preserves their logical identities.

    Given: Both selected instruments already have older active snapshot versions.
    When: A complete newer batch is committed atomically.
    Then: Old intervals close at receive time and new versions retain the logical identities.
    """
    _seed_old_snapshots(fixture, _START)
    before = _rows(fixture)
    receipt = await _persist(fixture)
    after = _rows(fixture)
    assert len(after) == 4
    assert [row.known_to for row in after[:2]] == [_NOW, _NOW]
    assert {row.public_id for row in after[2:]} == {row.public_id for row in before}
    assert all(row.known_to == KNOWN_TO_MAX for row in after[2:])
    assert await fixture.store.verify(fixture.scope, receipt, _budget()) == receipt


@pytest.mark.parametrize("problem", ["missing", "duplicate", "extra", "digest"])
async def test_partial_or_wrong_batch_never_writes(fixture: _Fixture, problem: str) -> None:
    """Invalid exact-set evidence cannot close or insert any snapshot version.

    Given: A batch has missing, duplicated, extra or incorrectly scoped operands.
    When: The store is asked to persist that invalid set.
    Then: The entire request refuses without creating any snapshot version.
    """
    batch = _batch(fixture.scope)
    changes = {
        "missing": {"snapshots": batch.snapshots[:1]},
        "duplicate": {"snapshots": (batch.snapshots[0], batch.snapshots[0])},
        "extra": {"snapshots": (*batch.snapshots, _operand("excluded"))},
        "digest": {"scope_digest": "different"},
    }[problem]
    with pytest.raises(ValueError, match="exact frozen"):
        await fixture.store.persist(
            _id("run"), fixture.scope, batch.model_copy(update=changes), _budget()
        )
    assert _rows(fixture) == []


@pytest.mark.parametrize("problem", ["session", "duplicate_sequence", "zero_sequence"])
async def test_batch_requires_exact_producer_provenance(fixture: _Fixture, problem: str) -> None:
    """A batch cannot mix producer sessions or reuse a nonpositive or duplicate sequence.

    Given: A batch contains a foreign session or duplicated or nonpositive sequence.
    When: The store validates its producer provenance.
    Then: Persistence refuses before any market snapshot is written.
    """
    batch = _batch(fixture.scope)
    changes = {
        "session": {"session_id": _id("wrong")},
        "duplicate_sequence": {"sequence_id": 2},
        "zero_sequence": {"sequence_id": 0},
    }[problem]
    first = batch.snapshots[0].model_copy(update=changes)
    invalid = batch.model_copy(update={"snapshots": (first, batch.snapshots[1])})
    with pytest.raises(ValueError, match="producer"):
        await fixture.store.persist(_id("run"), fixture.scope, invalid, _budget())
    assert _rows(fixture) == []


@pytest.mark.parametrize("offset", [0, 1])
async def test_equal_or_older_batch_refuses_before_closing_any_row(
    fixture: _Fixture, offset: int
) -> None:
    """A future or equal active version on one target refuses all members atomically.

    Given: One active snapshot is equal to or newer than the incoming receive time.
    When: The complete batch reaches the atomic write boundary.
    Then: Every member is refused and the existing row remains open-ended.
    """
    with fixture.sync.get_session() as session:
        session.add(MarketSnapshot(**_operand("fx", _NOW + timedelta(seconds=offset)).model_dump()))
        session.commit()
    with pytest.raises(ValueError, match="strictly newer"):
        await _persist(fixture)
    rows = _rows(fixture)
    assert len(rows) == 1 and rows[0].known_to == KNOWN_TO_MAX


@pytest.mark.parametrize("change", ["excluded_added", "main_changed"])
async def test_metadata_changes_refuse_complete_batch(fixture: _Fixture, change: str) -> None:
    """Required identity edits and newly excluded FX candidates invalidate the full digest.

    Given: An excluded FX candidate is added or a required native identity changes.
    When: The store re-resolves metadata after collection.
    Then: The changed complete scope digest refuses the whole batch.
    """
    if change == "excluded_added":
        _seed_target(fixture.sync, _metadata("new-excluded", "polygon"))
    else:
        with fixture.sync.get_session() as session:
            symbol = session.scalar(select(Symbol).where(Symbol.public_id == _id(_id("main"))))
            assert symbol is not None
            symbol.native_symbol = "BTC-EUR"
            session.commit()
    with pytest.raises(ValueError, match="metadata changed"):
        await _persist(fixture)
    assert _rows(fixture) == []


async def test_rollback_cannot_claim_success_from_old_fresh_rows(
    fixture: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Usable 120-second-old prices are not proof that the attempted batch committed.

    Given: Existing snapshots are only 120 seconds old but belong to an earlier producer.
    When: The new batch fails immediately before commit.
    Then: The exception propagates and all original versions remain active and unchanged.
    """
    _seed_old_snapshots(fixture, _NOW - timedelta(seconds=120))
    original = AsyncSession.commit

    async def fail_commit(session: AsyncSession) -> None:
        """Refuse the transaction after all closes and inserts but before commit."""
        raise RuntimeError("synthetic precommit failure")

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    with pytest.raises(RuntimeError, match="precommit"):
        await _persist(fixture)
    monkeypatch.setattr(AsyncSession, "commit", original)
    rows = _rows(fixture)
    assert len(rows) == 2
    assert all(row.known_to == KNOWN_TO_MAX and row.session_id == _id("old") for row in rows)


async def test_lost_commit_ack_is_non_success_without_retry(
    fixture: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A durable commit followed by transport failure still raises and cannot repeat its batch.

    Given: The database durably commits but its acknowledgment is lost.
    When: The original operation raises and the same collected timestamps are submitted again.
    Then: No success is inferred and the repeated batch cannot create duplicate versions.
    """
    original = AsyncSession.commit

    async def lose_ack(session: AsyncSession) -> None:
        """Commit durably, then emulate a lost client acknowledgement."""
        await original(session)
        raise ConnectionError("synthetic lost commit acknowledgement")

    monkeypatch.setattr(AsyncSession, "commit", lose_ack)
    with pytest.raises(ConnectionError, match="acknowledgement"):
        await _persist(fixture)
    monkeypatch.setattr(AsyncSession, "commit", original)
    assert len(_rows(fixture)) == 2
    with pytest.raises(ValueError, match="strictly newer"):
        await _persist(fixture)


async def test_retained_closed_rows_still_prove_original_commit(fixture: _Fixture) -> None:
    """A later ordinary SCD2 close does not erase a receipt's original collection operands.

    Given: A committed receipt names versions that later have their SCD2 intervals closed.
    When: The store verifies those exact retained physical rows.
    Then: Unchanged original operands still prove the acknowledged batch.
    """
    receipt = await _persist(fixture)
    with fixture.sync.get_session() as session:
        for snapshot in receipt.snapshots:
            row = session.get(MarketSnapshot, snapshot.row_id)
            assert row is not None
            row.known_to = _NOW + timedelta(seconds=1)
        session.commit()
    assert await fixture.store.verify(fixture.scope, receipt, _budget()) == receipt


@pytest.mark.parametrize(
    "corruption", ["missing", "price", "identity", "batch", "null_price", "run", "scope"]
)
async def test_unprovable_receipt_refuses_readback(fixture: _Fixture, corruption: str) -> None:
    """Missing or changed retained versions and corrupt intent bindings cannot be verified.

    Given: A retained row or its receipt identity, scope or digest has been corrupted.
    When: Exact version readback is requested.
    Then: The inconsistent evidence is refused rather than reported as verified.
    """
    receipt = await _persist(fixture)
    with fixture.sync.get_session() as session:
        row = session.get(MarketSnapshot, receipt.snapshots[0].row_id)
        assert row is not None
        if corruption == "missing":
            session.delete(row)
        elif corruption == "price":
            row.last_price = 12.0
        elif corruption == "null_price":
            row.last_price = None
        elif corruption == "identity":
            row.public_id = _id("wrong")
        session.commit()
    fields = {"batch": "batch_digest", "run": "run_id", "scope": "scope_digest"}
    if corruption in fields:
        receipt = receipt.model_copy(update={fields[corruption]: _id("wrong")})
    with pytest.raises(ValueError):
        await fixture.store.verify(fixture.scope, receipt, _budget())


async def test_two_engines_serialize_same_batch_without_duplicate_versions(
    fixture: _Fixture,
) -> None:
    """The actual write connections serialize complete batches across independent pools.

    Given: Two independent SQLite connection pools target the same selected instruments.
    When: Both attempt to commit the same complete receive-time batch concurrently.
    Then: Exactly one batch commits and the other refuses without duplicate versions.
    """
    second_repository = SQLAlchemyRepository(str(fixture.asynchronous.engine.url))
    second = MarketSnapshotRefreshStore(second_repository, lambda: _NOW)
    try:
        results = await asyncio.gather(
            _persist(fixture),
            second.persist(_id("run"), fixture.scope, _batch(fixture.scope), _budget()),
            return_exceptions=True,
        )
        assert sum(isinstance(result, RefreshCommitReceipt) for result in results) == 1
        assert sum(isinstance(result, ValueError) for result in results) == 1
        assert len(_rows(fixture)) == 2
    finally:
        await second_repository.engine.dispose()


async def test_late_old_batch_cannot_replace_newer_commit(fixture: _Fixture) -> None:
    """A delayed old worker acquiring the fence later cannot replace a newer complete batch.

    Given: A newer complete batch has already committed.
    When: A delayed older batch acquires the transaction fence afterward.
    Then: The strict timestamp guard preserves every newer active version.
    """
    await _persist(fixture)
    older = _batch(fixture.scope, _START)
    with pytest.raises(ValueError, match="strictly newer"):
        await fixture.store.persist(_id("run"), fixture.scope, older, _budget())
    assert all(row.timestamp == _NOW for row in _rows(fixture))


@pytest.mark.parametrize("run_id", ["bad", " " + _id("run")])
async def test_invalid_run_identity_refuses_before_write(fixture: _Fixture, run_id: str) -> None:
    """Malformed or padded producer identities are not forwarded to the database driver.

    Given: The supplied producer run identity is malformed or padded.
    When: The store validates it before persistence.
    Then: The invalid identity is refused without a database write.
    """
    with pytest.raises(ValueError):
        await fixture.store.persist(run_id, fixture.scope, _batch(fixture.scope), _budget())


async def test_naive_store_clock_refuses_write(fixture: _Fixture) -> None:
    """The store never silently interprets an ambiguous injected wall clock.

    Given: The injected store clock returns a timestamp without a timezone.
    When: The batch is admitted for persistence.
    Then: The ambiguous wall clock causes refusal before any write.
    """
    store = MarketSnapshotRefreshStore(fixture.asynchronous, lambda: _NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware"):
        await store.persist(_id("run"), fixture.scope, _batch(fixture.scope), _budget())


async def test_signed_zero_optional_fields_have_stable_sqlite_receipt(fixture: _Fixture) -> None:
    """Equivalent IEEE signed zero cannot make a valid database round trip unverifiable.

    Given: Collected optional fields contain mathematically equivalent negative zero.
    When: The batch is persisted and its SQLite versions are verified.
    Then: Canonical zero fingerprints survive the round trip unchanged.
    """
    batch = _batch(fixture.scope)
    snapshots = tuple(
        RefreshSnapshot(**(row.model_dump() | {"spread": -0.0})) for row in batch.snapshots
    )
    batch = batch.model_copy(update={"snapshots": snapshots})
    assert all(math.copysign(1, row.spread or 0.0) == 1.0 for row in snapshots)
    receipt = await fixture.store.persist(_id("run"), fixture.scope, batch, _budget())
    assert await fixture.store.verify(fixture.scope, receipt, _budget()) == receipt


@pytest.mark.parametrize("offset", [-2, 1])
async def test_receive_time_must_belong_to_original_collection(
    fixture: _Fixture, offset: int
) -> None:
    """Pre-subscription and future ticks cannot be relabeled as a current collected batch.

    Given: An incoming tick predates subscription or lies after the current wall clock.
    When: The store checks the original collection interval.
    Then: The entire batch refuses without relabeling the receive time.
    """
    with pytest.raises(ValueError, match="collection interval"):
        await fixture.store.persist(
            _id("run"),
            fixture.scope,
            _batch(fixture.scope, _NOW + timedelta(seconds=offset)),
            _budget(),
        )
    assert _rows(fixture) == []


@pytest.mark.parametrize("age, accepted", [(300, True), (301, False)])
async def test_receive_freshness_exact_boundary(
    fixture: _Fixture, age: int, accepted: bool
) -> None:
    """The exact shared staleness boundary remains inclusive without a duplicated threshold.

    Given: A complete batch is exactly 300 seconds old or one second older.
    When: The shared staleness boundary is enforced at persistence.
    Then: The exact boundary is accepted and the older batch writes nothing.
    """
    batch = _batch(fixture.scope, _NOW - timedelta(seconds=age)).model_copy(
        update={"collection_started_at": _NOW - timedelta(seconds=age + 1)}
    )
    if accepted:
        await fixture.store.persist(_id("run"), fixture.scope, batch, _budget())
        assert len(_rows(fixture)) == 2
    else:
        with pytest.raises(ValueError, match="stale receive"):
            await fixture.store.persist(_id("run"), fixture.scope, batch, _budget())
        assert _rows(fixture) == []


async def test_final_clock_after_awaited_writes_can_refuse_transaction(fixture: _Fixture) -> None:
    """A clock step after I/O cannot commit operands that became stale during the transaction.

    Given: The initial clock admits the batch but advances beyond freshness during I/O.
    When: The store reads its final clock before committing.
    Then: The stale transaction rolls back every inserted version.
    """
    times = iter((_NOW, _NOW + timedelta(seconds=301)))
    store = MarketSnapshotRefreshStore(fixture.asynchronous, lambda: next(times))
    with pytest.raises(ValueError, match="stale receive"):
        await store.persist(_id("run"), fixture.scope, _batch(fixture.scope), _budget())
    assert _rows(fixture) == []


@pytest.mark.parametrize("time", [_NOW.replace(tzinfo=None), _NOW + timedelta(seconds=1)])
async def test_receipt_time_cannot_postdate_verification(fixture: _Fixture, time: datetime) -> None:
    """Naive or future receipt times cannot justify a supposedly post-write observation.

    Given: A receipt has a naive timestamp or claims a time after verification.
    When: Exact receipt verification begins.
    Then: The invalid receipt clock is refused.
    """
    receipt = (await _persist(fixture)).model_copy(update={"recorded_at": time})
    with pytest.raises(ValueError, match="receipt time"):
        await fixture.store.verify(fixture.scope, receipt, _budget())


async def test_unknown_dialect_refuses_before_write(
    fixture: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsupported backends never silently substitute a weaker fence.

    Given: The repository reports an unsupported database backend.
    When: The store attempts to establish its transaction strategy.
    Then: It refuses without substituting a weaker fence or writing market data.
    """
    monkeypatch.setattr(SQLAlchemyRepository, "dialect_name", property(lambda self: "other"))
    with pytest.raises(ValueError, match="supports PostgreSQL and SQLite"):
        await _persist(fixture)


async def test_expired_budget_starts_no_transaction(fixture: _Fixture) -> None:
    """An exhausted supervisor deadline cannot authorize a fresh transaction.

    Given: The inherited absolute deadline is already exhausted.
    When: A complete batch is submitted to the store.
    Then: No new transaction or snapshot version is authorized.
    """
    with pytest.raises(TimeoutError):
        await fixture.store.persist(
            _id("run"), fixture.scope, _batch(fixture.scope), RefreshBudget(0.0, lambda: 1.0)
        )
    assert _rows(fixture) == []


class _PostgresControlSession:
    """Provide unbound sessions for SQL-shape tests without creating a database engine."""

    dialect_name = "postgresql"

    def session(self) -> AsyncSession:
        """Return an unbound session whose execute method is supplied by the test."""
        return AsyncSession()


async def test_postgresql_fence_bounds_and_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PostgreSQL setup binds local server timeouts and advisory lock on the write session.

    Given: A fixture-free PostgreSQL session records control statements without a server.
    When: The transaction fence and metadata locks are requested under a short budget.
    Then: The exact namespace and bounded local timeouts are applied on that session.
    """
    calls: list[tuple[str, object]] = []

    async def execute(session: AsyncSession, statement: object, params: object = None) -> None:
        """Record transaction control without contacting a PostgreSQL server."""
        calls.append((str(statement), params))

    monkeypatch.setattr(AsyncSession, "execute", execute)
    async with refresh_transaction(
        _PostgresControlSession(), RefreshBudget(0.8, lambda: 0.0)
    ) as session:
        await lock_refresh_metadata(session, "postgresql")
        await lock_refresh_snapshots(session, "postgresql")
    assert calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert calls[1:5] == [
        (
            "SELECT set_config(:setting, :milliseconds, true)",
            {"setting": "transaction_timeout", "milliseconds": "800ms"},
        ),
        (
            "SELECT set_config(:setting, :milliseconds, true)",
            {"setting": "statement_timeout", "milliseconds": "800ms"},
        ),
        (
            "SELECT set_config(:setting, :milliseconds, true)",
            {"setting": "lock_timeout", "milliseconds": "250ms"},
        ),
        (
            "SELECT set_config(:setting, :milliseconds, true)",
            {"setting": "idle_in_transaction_session_timeout", "milliseconds": "800ms"},
        ),
    ]
    assert calls[5] == (
        "SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:domain))",
        {
            "namespace": "snapper.selected_market_snapshot_refresh",
            "domain": "market_snapshot_refresh_v1",
        },
    )
    assert (
        calls[6][0]
        == "LOCK TABLE instruments, symbols, symbol_exchange_capabilities, symbol_aliases IN SHARE MODE"
    )
    assert calls[7][0] == "LOCK TABLE market_snapshots IN SHARE ROW EXCLUSIVE MODE"


async def test_store_refuses_unresolved_fx_and_bad_classification(fixture: _Fixture) -> None:
    """A superficially frozen scope cannot bypass dependency or classification proof.

    Given: A scope lacks configured FX evidence or wrongly classifies excluded metadata.
    When: The store revalidates that frozen scope.
    Then: Both inconsistent scopes refuse before snapshot persistence.
    """
    unresolved = freeze_refresh_scope(
        [_id("main")], ["walutomat"], [_metadata("main"), _metadata("fx")]
    )
    with pytest.raises(ValueError, match="unresolved"):
        await fixture.store.persist(_id("run"), unresolved, _batch(unresolved), _budget())
    wrong = fixture.scope.model_copy(
        update={"required": (*fixture.scope.required, *fixture.scope.excluded), "excluded": ()}
    )
    with pytest.raises(ValueError, match="classification"):
        await fixture.store.persist(_id("run"), wrong, _batch(wrong), _budget())


def test_default_clock_is_aware() -> None:
    """The production wall-clock supplier returns explicit UTC evidence.

    Given: The store uses its production default clock supplier.
    When: The current wall-clock instant is requested.
    Then: The result explicitly uses UTC.
    """
    assert store_module._utc_now().tzinfo is UTC


@pytest.mark.parametrize("ids", [[], [_id("main")] * 1001])
def test_active_query_requires_bounded_explicit_ids(ids: list[str]) -> None:
    """The full-row store helper cannot be used as a realm-wide or oversized query.

    Given: An active-row request is empty or exceeds the selected-scope limit.
    When: The repository helper is asked to construct its query.
    Then: It refuses rather than admitting an implicit or oversized universe.
    """
    with pytest.raises(ValueError, match="bounded explicit"):
        _market_snapshot_active_rows_query(ids)


def test_collection_start_requires_timezone() -> None:
    """A batch cannot infer the time zone of its subscription start.

    Given: A batch supplies a naive subscription-start timestamp.
    When: The typed collection contract is constructed.
    Then: Validation refuses to infer the missing timezone.
    """
    with pytest.raises(ValidationError, match="timezone-aware"):
        RefreshSnapshotBatch(
            scope_digest=_scope().scope_digest,
            collection_started_at=_START.replace(tzinfo=None),
            alias_proofs=_alias_proofs(_scope()),
            snapshots=(_operand("main"), _operand("fx")),
        )


def test_freshness_constant_extraction_preserves_existing_imports() -> None:
    """Core, converter and existing observation imports preserve the exact inclusive limits.

    Given: Core policy, the converter and the existing freshness module expose their limits.
    When: The established import paths are compared after extraction.
    Then: All retain the same 300-second staleness and 5-second future-skew limits.
    """
    assert (
        freshness_policy.STALENESS_THRESHOLD_SECONDS
        == converter_module.STALENESS_THRESHOLD_SECONDS
        == freshness_module.STALENESS_THRESHOLD_SECONDS
        == 300
    )
    assert (
        freshness_policy.MAX_FUTURE_SKEW_SECONDS
        == converter_module.MAX_FUTURE_SKEW_SECONDS
        == freshness_module.MAX_FUTURE_SKEW_SECONDS
        == 5
    )


async def test_verification_final_clock_cannot_precede_write(fixture: _Fixture) -> None:
    """The post-I/O verification clock must still be at or after the receipt timestamp.

    Given: The clock initially admits a receipt but moves backward during readback.
    When: Verification performs its final post-I/O clock check.
    Then: The receipt cannot be accepted as evidence from the future.
    """
    receipt = await _persist(fixture)
    times = iter((_NOW, _START))
    store = MarketSnapshotRefreshStore(fixture.asynchronous, lambda: next(times))
    with pytest.raises(ValueError, match="clock moved"):
        await store.verify(fixture.scope, receipt, _budget())


def test_snapshot_operand_requires_price() -> None:
    """A legacy NULL-price version can never prove a committed refresh operand.

    Given: A legacy snapshot has no last-price evidence.
    When: It is converted into a proposed refresh operand.
    Then: The missing price is refused rather than replaced with a guessed value.
    """
    with pytest.raises(ValueError, match="no last price"):
        snapshot_operand(MarketSnapshot(last_price=None))


@pytest.mark.parametrize("price", [0.0, -1.0, math.nan, math.inf, -math.inf])
def test_snapshot_refuses_invalid_price(price: float) -> None:
    """Every persisted last-price operand must be finite and strictly positive.

    Given: The collected last price is zero, negative or nonfinite.
    When: The typed snapshot contract is constructed.
    Then: Every invalid last-price value is rejected.
    """
    with pytest.raises(ValidationError):
        RefreshSnapshot(**(_operand("main").model_dump() | {"last_price": price}))


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_snapshot_refuses_nonfinite_optional_evidence(value: float) -> None:
    """Optional market fields cannot smuggle nonfinite operands into a committed receipt.

    Given: An optional numeric market field contains NaN or an infinity.
    When: The typed snapshot contract validates the operand.
    Then: Nonfinite evidence is rejected even though the field is optional.
    """
    with pytest.raises(ValidationError):
        RefreshSnapshot(**(_operand("main").model_dump() | {"spread": value}))


def test_snapshot_refuses_naive_receive_time() -> None:
    """Receive timestamps retain an explicit timezone rather than an inferred local instant.

    Given: The original tick timestamp lacks a timezone.
    When: The receive-time contract is constructed.
    Then: Validation refuses instead of attaching a guessed timezone.
    """
    with pytest.raises(ValidationError):
        _operand("main", _NOW.replace(tzinfo=None))


def test_budget_never_renews_absolute_deadline() -> None:
    """Phase ceilings and millisecond conversion consume one existing monotonic budget.

    Given: One synthetic monotonic deadline has a known remaining interval.
    When: Phase ceilings, cleanup reserve and millisecond bounds are requested.
    Then: They consume the same budget without moving its absolute expiry.
    """
    budget = RefreshBudget(10.0, lambda: 7.5)
    assert budget.remaining() == 2.5
    assert budget.timeout_seconds(4.0, reserve=0.5) == 2.0
    assert budget.timeout_ms(0.1239) == 123
    assert budget.deadline == 10.0


@pytest.mark.parametrize(
    "deadline, now, reserve",
    [(math.inf, 1.0, 0.0), (5.0, math.nan, 0.0), (5.0, 1.0, -1.0), (5.0, 1.0, math.inf)],
)
def test_budget_rejects_nonfinite_or_negative_values(
    deadline: float, now: float, reserve: float
) -> None:
    """Invalid clock and reserve inputs fail before any I/O timeout is constructed.

    Given: A deadline, clock observation or reserve is nonfinite or the reserve is negative.
    When: The remaining shared budget is evaluated.
    Then: Invalid clock arithmetic is refused before I/O starts.
    """
    with pytest.raises(ValueError):
        RefreshBudget(deadline, lambda: now).remaining(reserve)


@pytest.mark.parametrize("ceiling", [0.0, -1.0, math.inf, math.nan])
def test_budget_rejects_invalid_phase_ceiling(ceiling: float) -> None:
    """A timeout ceiling cannot disable the finite absolute deadline.

    Given: A requested phase ceiling is nonpositive or nonfinite.
    When: The bounded timeout is computed.
    Then: The invalid ceiling cannot disable the absolute deadline.
    """
    with pytest.raises(ValueError):
        _budget().timeout_seconds(ceiling)


def test_expired_and_submillisecond_budget_refuse() -> None:
    """Exhausted time is never rounded up into new execution authority.

    Given: No usable time remains, or less than one database millisecond is available.
    When: The budget supplies remaining time or an integer millisecond timeout.
    Then: It refuses instead of rounding upward or renewing execution authority.
    """
    with pytest.raises(TimeoutError):
        RefreshBudget(1.0, lambda: 1.0).remaining()
    with pytest.raises(TimeoutError):
        RefreshBudget(1.0001, lambda: 1.0).timeout_ms(1.0)


def test_scope_permutation_and_alias_uuid_are_digest_stable() -> None:
    """Canonical UUID aliases and metadata permutations preserve the established scope digest.

    Given: Equivalent UUID spellings and reordered metadata describe the same scope.
    When: The explicit scope is frozen after canonicalization and deduplication.
    Then: Its digest and required identities remain unchanged.
    """
    targets = [_metadata("main"), _metadata("fx"), _metadata("excluded", "polygon")]
    reversed_scope = freeze_refresh_scope(
        [_id("main").upper(), _id("main")], ["kraken", "kraken"], list(reversed(targets))
    )
    assert reversed_scope.scope_digest == _scope().scope_digest
    assert reversed_scope.required_instrument_ids == tuple(sorted((_id("main"), _id("fx"))))
    assert reversed_scope.unresolved_fx_currencies == ()


@pytest.mark.parametrize("ids", [[], ["bad"], [" " + _id("main")], [_id("main")] * 101])
def test_scope_refuses_invalid_requested_ids(ids: list[str]) -> None:
    """Malformed, padded, empty and oversized scope requests fail before repository calls.

    Given: Instrument inputs are empty, malformed, padded or oversized.
    When: The scope normalizer validates the requested identities.
    Then: Every invalid request is refused before repository access.
    """
    with pytest.raises(ValueError):
        freeze_refresh_scope(ids, ["kraken"], [_metadata("main"), _metadata("fx")])


@pytest.mark.parametrize("venues", [[], ["polygon"], ["kraken"] * 101])
def test_scope_refuses_invalid_fx_selection(venues: list[str]) -> None:
    """Only a bounded explicit supported FX venue list may define readiness requirements.

    Given: The FX venue selection is empty, unsupported or oversized.
    When: The caller attempts to freeze a refresh scope.
    Then: The implicit or invalid venue selection is refused.
    """
    with pytest.raises(ValueError):
        freeze_refresh_scope([_id("main")], venues, [_metadata("main"), _metadata("fx")])


@pytest.mark.parametrize(
    "problem", ["duplicate", "missing", "unsupported", "blank", "wildcard", "overflow"]
)
def test_scope_refuses_incomplete_metadata(problem: str) -> None:
    """Incomplete and ambiguous resolver evidence never becomes a supposedly frozen scope.

    Given: Resolver metadata is duplicated, missing, unsupported, malformed or oversized.
    When: The requested scope is frozen from that evidence.
    Then: The incomplete or ambiguous universe is refused.
    """
    targets = [_metadata("main"), _metadata("fx")]
    if problem == "duplicate":
        targets.append(_metadata("main"))
    elif problem == "missing":
        targets = [_metadata("fx")]
    elif problem == "unsupported":
        targets[0] = _metadata("main", "polygon")
    elif problem == "blank":
        targets[0]["base"] = " "
    elif problem == "wildcard":
        targets[0]["native_symbol"] = "*"
    else:
        targets = [_metadata("main")] * 1001
    with pytest.raises(ValueError):
        freeze_refresh_scope([_id("main")], ["kraken"], targets)


@pytest.mark.parametrize("change", ["wire", "replacement"])
async def test_alias_version_drift_refuses_before_any_snapshot(
    fixture: _Fixture, change: str
) -> None:
    """Changed or reverted alias versions cannot borrow a previously collected batch proof.

    Given: A selected wire alias changes or is replaced under the same public identity.
    When: The store revalidates its exact physical alias proof before writing.
    Then: The changed alias version refuses the entire collected batch.
    """
    with fixture.sync.get_session() as session:
        alias = session.scalar(
            select(SymbolAlias).where(SymbolAlias.symbol_public_id == _id(_id("main")))
        )
        assert alias is not None
        if change == "wire":
            alias.exchange_symbol = "XBT/EUR"
        else:
            alias.known_to = _START
            session.flush()
            session.add(
                SymbolAlias(
                    public_id=alias.public_id,
                    symbol_public_id=alias.symbol_public_id,
                    exchange=alias.exchange,
                    channel=alias.channel,
                    exchange_symbol=alias.exchange_symbol,
                    timestamp=_START,
                    session_id=_id("replacement"),
                    sequence_id=1,
                    created_at=_START,
                )
            )
        session.commit()
    with pytest.raises(ValueError, match="wire alias identity changed"):
        await _persist(fixture)
    assert _rows(fixture) == []


async def test_receipt_requires_current_matching_alias_proof(fixture: _Fixture) -> None:
    """Exact row existence alone cannot hide a mapping change before post-write verification.

    Given: A committed batch exists but its selected alias changes before verification.
    When: The store verifies the receipt against current locked metadata.
    Then: Exact snapshot rows cannot hide the alias drift.
    """
    receipt = await _persist(fixture)
    with fixture.sync.get_session() as session:
        alias = session.scalar(
            select(SymbolAlias).where(SymbolAlias.symbol_public_id == _id(_id("main")))
        )
        assert alias is not None
        alias.exchange_symbol = "XBT/EUR"
        session.commit()
    with pytest.raises(ValueError, match="wire alias identity changed"):
        await fixture.store.verify(fixture.scope, receipt, _budget())


@pytest.mark.parametrize("problem", ["missing", "duplicate", "extra"])
async def test_alias_proof_scope_must_be_exact(fixture: _Fixture, problem: str) -> None:
    """A supplied proof list must bind each required Kraken target exactly once.

    Given: The proof list omits, duplicates or adds a Kraken instrument.
    When: The store checks the proofs against the required scope.
    Then: The mismatch refuses before any snapshot is written.
    """
    batch = _batch(fixture.scope)
    proofs = {
        "missing": batch.alias_proofs[:1],
        "duplicate": (batch.alias_proofs[0], batch.alias_proofs[0]),
        "extra": (
            *batch.alias_proofs,
            batch.alias_proofs[0].model_copy(update={"instrument_public_id": _id("excluded")}),
        ),
    }[problem]
    with pytest.raises(ValueError, match="alias proofs must cover"):
        await fixture.store.persist(
            _id("run"), fixture.scope, batch.model_copy(update={"alias_proofs": proofs}), _budget()
        )
    assert _rows(fixture) == []


async def test_non_kraken_scope_does_not_invent_kraken_alias_query(fixture: _Fixture) -> None:
    """The Kraken-specific alias proof reader never broadens an empty Kraken subset.

    Given: The required scope contains no Kraken instruments.
    When: Kraken alias revalidation is requested with an empty proof subset.
    Then: No alias query is issued and no broader scope is inferred.
    """
    metadata = _metadata("fx", "walutomat")
    scope = freeze_refresh_scope([_id("fx")], ["walutomat"], [metadata])
    session = AsyncMock(spec=AsyncSession)
    await revalidate_refresh_aliases(session, scope, (), _budget())
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("problem", ["oversize", "duplicate_row"])
async def test_receipt_validation_bounds_query_before_execute(
    fixture: _Fixture, problem: str
) -> None:
    """Malformed retained-row lists are refused before generating an unbounded ID query.

    Given: A receipt has an oversized version list or repeats one physical row identity.
    When: The exact retained-version proof is requested.
    Then: Validation refuses before constructing or executing the snapshot query.
    """
    receipt = await _persist(fixture)
    if problem == "oversize":
        snapshots = receipt.snapshots * 1001
    else:
        first, second = receipt.snapshots
        snapshots = (first, second.model_copy(update={"row_id": first.row_id}))
    session = AsyncMock(spec=AsyncSession)
    with pytest.raises(ValueError, match="exact bounded scope"):
        await verify_refresh_receipt(
            session, fixture.scope, receipt.model_copy(update={"snapshots": snapshots})
        )
    session.execute.assert_not_awaited()


def test_alias_proof_digest_is_order_stable_and_version_sensitive() -> None:
    """Alias ordering is irrelevant but a new physical alias version changes batch intent.

    Given: A batch has two exact physical alias proofs.
    When: Proof order is reversed or one physical version identity is changed.
    Then: Reordering preserves the batch digest while a changed version changes it.
    """
    batch = _batch(_scope())
    reordered = batch.model_copy(update={"alias_proofs": tuple(reversed(batch.alias_proofs))})
    assert reordered.batch_digest == batch.batch_digest
    changed = batch.alias_proofs[0].model_copy(update={"alias_row_id": 99})
    assert (
        batch.model_copy(update={"alias_proofs": (changed, batch.alias_proofs[1])}).batch_digest
        != batch.batch_digest
    )
    assert reordered.scope_digest == batch.scope_digest


def test_alias_proof_requires_positive_physical_version_id() -> None:
    """A proof without a persisted physical alias version is unusable.

    Given: An alias proof names no valid persisted physical row.
    When: The typed alias proof is constructed with row ID zero.
    Then: The invalid version identity is rejected.
    """
    with pytest.raises(ValidationError):
        RefreshAliasProof(**(_alias_proofs(_scope())[0].model_dump() | {"alias_row_id": 0}))


@pytest.mark.parametrize("field", ["session_id", "sequence_id", "timestamp"])
async def test_receipt_explicit_provenance_matches_retained_version(
    fixture: _Fixture, field: str
) -> None:
    """Unchanged payload hashes cannot disguise altered explicit provenance fields.

    Given: A receipt keeps its payload hash but changes explicit session, sequence or receive time.
    When: The store compares it with the retained physical snapshot version.
    Then: The changed provenance is refused despite the unchanged hash.
    """
    receipt = await _persist(fixture)
    original = receipt.snapshots[0]
    value: object = {
        "session_id": _id("different-producer"),
        "sequence_id": 999,
        "timestamp": _START,
    }[field]
    changed = original.model_copy(update={field: value})
    assert changed.payload_digest == original.payload_digest
    invalid = receipt.model_copy(update={"snapshots": (changed, *receipt.snapshots[1:])})
    with pytest.raises(ValueError, match="retained version is inconsistent"):
        await fixture.store.verify(fixture.scope, invalid, _budget())


async def test_receipt_carries_original_producer_and_receive_time(fixture: _Fixture) -> None:
    """Each exact physical version receipt exposes the original collected provenance.

    Given: A complete batch contains original producer session, sequence and receive-time evidence.
    When: The store commits it and returns exact version receipts.
    Then: Every receipt preserves those original provenance fields.
    """
    receipt = await _persist(fixture)
    expected = {row.instrument_public_id: row for row in _batch(fixture.scope).snapshots}
    for snapshot in receipt.snapshots:
        operand = expected[snapshot.instrument_public_id]
        assert snapshot.session_id == operand.session_id
        assert snapshot.sequence_id == operand.sequence_id
        assert snapshot.timestamp == operand.timestamp
