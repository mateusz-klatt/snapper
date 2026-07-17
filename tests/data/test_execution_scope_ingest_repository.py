"""Tests for scope resolution and counter assignment in execution ingest.

``insert_execution`` validates and canonicalizes the wallet identity ONCE,
resolves the fill's immutable ``(exchange, mode)`` scope from the ACTIVE
Order -> Instrument lineage (writer-resolves), refuses malformed,
unresolvable, or crossed rows pre-persistence with the typed
``ExecutionScopeResolutionError``, and allocates ``scope_sequence`` as the
committed per-scope max + 1 inside a ``BEGIN IMMEDIATE`` transaction on
SQLite (the write reservation serializes allocation across connections —
the engine's page-level single writer does not). These tests exercise the
real SQLite ingest path: per-scope contiguity across interleaved scopes
and wallets, alias-spelling canonicalization into ONE counter scope,
gap-free survival of a rolled-back insert, cross-connection allocation
serialization, the fail-closed refusals, and the total-unique index as
the raw-write backstop.

Two tests exist to discharge a PRECONDITION of the sealed-prefix proof
rather than to cover a branch. The allocator derives ``K`` from the table,
so the proof needs ``K`` to be monotone — a premise the fence does NOT
supply and that survives only while nothing deletes a committed row. The
physical ``executions_reject_delete`` trigger now makes that deleted-tip
hazard unreachable by construction, so both tests issue the raw ``DELETE``
a manual console, a restore tool, or a bug would take and prove the
database REFUSES it (``append-only`` ``DBAPIError``), leaving the tip row,
the watermark, and the counted range proof intact — a stronger discharge
of the monotonicity obligation than a documented archiver refusal, and one
a reader who relaxes it has to answer first. The wallet constants
deliberately contain ``a-f`` hex letters so alias-spelling tests exercise
a REAL case change instead of passing tautologically.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError

from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import ExecutionScopeResolutionError
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-00000000c101"
_OTHER_WALLET = "0000face-0000-7000-8000-00000000c102"
_WAITER_TIMEOUT_SECONDS = 10.0
_SESSION = "00000000-0000-7000-8000-000000000301"
_LIVE_INSTRUMENT = "00000000-0000-7000-8000-000000000501"
_FOREIGN_INSTRUMENT = "00000000-0000-7000-8000-000000000502"
_MISSING_INSTRUMENT = "00000000-0000-7000-8000-000000000503"
_LIVE_ORDER = "00000000-0000-7000-8000-000000000601"
_PAPER_ORDER = "00000000-0000-7000-8000-000000000602"
_FOREIGN_ORDER = "00000000-0000-7000-8000-000000000603"
_OTHER_WALLET_ORDER = "00000000-0000-7000-8000-000000000604"
_MISSING_ORDER = "00000000-0000-7000-8000-000000000605"
_ORPHAN_INSTRUMENT_ORDER = "00000000-0000-7000-8000-000000000606"


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create one isolated repository containing only the scope-plane tables."""
    db_path = tmp_path / "scope-ingest.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    schema_engine.dispose()
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield result
    finally:
        await result.engine.dispose()


@pytest.fixture()
async def peer_repository(
    tmp_path: Path, repository: SQLAlchemyRepository
) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a second independent engine over the SAME SQLite database.

    Models a peer writer connection (e.g. an executor restart overlap):
    two engines mean two SQLite connections, which is exactly the
    concurrency the ``BEGIN IMMEDIATE`` write reservation must serialize.
    """
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'scope-ingest.db'}")
    try:
        yield result
    finally:
        await result.engine.dispose()


def _instrument(public_id: str, exchange: str) -> Instrument:
    """Build one active instrument version for an ingest lineage leg."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=public_id,
        exchange=exchange,
        timestamp=_NOW - timedelta(days=1),
        session_id=_SESSION,
        sequence_id=1,
    )


def _order(
    public_id: str,
    instrument_public_id: str,
    mode: str,
    wallet_public_id: str,
) -> Order:
    """Build one active order version carrying the mode and instrument scope."""
    return Order(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
        mode=mode,
        wallet_public_id=wallet_public_id,
        created_at=_NOW - timedelta(hours=2),
        timestamp=_NOW - timedelta(hours=2),
        side="buy",
        order_type="limit",
        price=4.25,
        size=100.0,
        status="filled",
        session_id=_SESSION,
        sequence_id=1,
    )


async def _seed_lineage(repository: SQLAlchemyRepository) -> None:
    """Insert the active order and instrument versions the ingest resolves."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(_LIVE_INSTRUMENT, "walutomat"),
                _instrument(_FOREIGN_INSTRUMENT, "kraken"),
                _order(_LIVE_ORDER, _LIVE_INSTRUMENT, "live", _WALLET),
                _order(_PAPER_ORDER, _LIVE_INSTRUMENT, "paper", _WALLET),
                _order(_FOREIGN_ORDER, _FOREIGN_INSTRUMENT, "live", _WALLET),
                _order(_OTHER_WALLET_ORDER, _LIVE_INSTRUMENT, "live", _OTHER_WALLET),
            ]
        )
        await s.commit()


async def _insert_fill(
    repository: SQLAlchemyRepository,
    order_public_id: str,
    wallet_public_id: str,
    sequence_id: int,
    *,
    side: str = "buy",
) -> int:
    """Persist one fill through the production ingest path, returning its id."""
    return await repository.insert_execution(
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        timestamp=_NOW - timedelta(hours=1),
        side=side,
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


async def _stored_scopes(
    repository: SQLAlchemyRepository,
) -> list[tuple[str, str, str, int]]:
    """Return every stored (wallet, exchange, mode, scope_sequence) by row id."""
    async with repository.session() as s:
        rows = (
            await s.execute(
                select(
                    Execution.wallet_public_id,
                    Execution.exchange,
                    Execution.mode,
                    Execution.scope_sequence,
                ).order_by(Execution.id)
            )
        ).all()
    return [(str(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in rows]


async def _fill_identity_at_sequence(
    repository: SQLAlchemyRepository, sequence: int
) -> tuple[int, str]:
    """Return the ``(sequence_id, side)`` of the fill holding one live counter.

    ``Execution.id`` deliberately does NOT identify the fill here. SQLite
    reuses the ROWID of a deleted max row (the F2b hazard, moot for
    certification but very much alive for identity), so a fill re-minted
    after a tip delete lands on the freed counter AND on the freed id —
    the id discriminates nothing. The event's own ``sequence_id`` and its
    economic ``side`` are what distinguish one fill from another.
    """
    async with repository.session() as s:
        row = (
            await s.execute(
                select(Execution.sequence_id, Execution.side).where(
                    Execution.wallet_public_id == _WALLET,
                    Execution.exchange == "walutomat",
                    Execution.mode == "live",
                    Execution.scope_sequence == sequence,
                )
            )
        ).one()
    return int(row[0]), str(row[1])


async def _attempt_tip_delete(repository: SQLAlchemyRepository, row_id: int) -> None:
    """Attempt to delete one execution row by raw SQL, letting it be refused.

    This is the raw path a manual ``DELETE``, a restore/import tool, or a
    bug would take — outside every supported path and outside the six
    Python-guarded ORM primitives, so ONLY the physical
    ``executions_reject_delete`` trigger can be doing the rejecting. The
    ``DELETE`` is expected to raise before the ``commit`` is ever reached;
    the exception propagates to the caller, which asserts it and that the
    row survived. The point is now that the DATABASE physically refuses the
    removal, so the deleted-tip hazard is unreachable by construction.
    """
    async with repository.session() as s:
        await s.execute(text("DELETE FROM executions WHERE id = :row_id"), {"row_id": row_id})
        await s.commit()


async def _counted_range_complete(
    repository: SQLAlchemyRepository, anchor_seq: int, watermark: int
) -> bool:
    """Evaluate the design's counted range proof over the live scope.

    Mirrors the formula the S4c-4 replay evaluator will own —
    ``count(rows in (anchor_seq, W]) == W - anchor_seq``
    (`design_2026_07_17_execution_scope_counter.md` §5). That evaluator has
    not shipped (``SpotReplayBoundary.range_complete`` is still
    caller-supplied evidence), so these tests evaluate the proof itself
    against real stored rows rather than a production caller.
    """
    async with repository.session() as s:
        counted = (
            await s.execute(
                select(func.count())
                .select_from(Execution)
                .where(
                    Execution.wallet_public_id == _WALLET,
                    Execution.exchange == "walutomat",
                    Execution.mode == "live",
                    Execution.scope_sequence > anchor_seq,
                    Execution.scope_sequence <= watermark,
                )
            )
        ).scalar_one()
    return int(counted) == watermark - anchor_seq


@pytest.mark.asyncio
async def test_scope_is_resolved_from_active_lineage_for_live_and_paper(
    repository: SQLAlchemyRepository,
) -> None:
    """The writer stamps the fill with its lineage-resolved immutable scope.

    Given: Active live-walutomat and paper-walutomat order lineages.
    When: One fill is ingested through each order.
    Then: Each stored row carries the exchange from the active instrument
        and the mode from the active order, and each scope starts its own
        counter at 1.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _PAPER_ORDER, _WALLET, 2)
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "paper", 1),
    ]


@pytest.mark.asyncio
async def test_counter_is_contiguous_per_scope_across_interleaved_ingest(
    repository: SQLAlchemyRepository,
) -> None:
    """Each scope numbers its own fills 1..N regardless of interleaving.

    Given: Fills ingested alternately across three scopes — the wallet's
        live walutomat scope, its paper scope, its live kraken scope — and
        a second wallet's live scope.
    When: All fills are persisted through the production path.
    Then: Every scope carries exactly the contiguous counters 1..N in its
        own ingest order, with no cross-scope influence.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _PAPER_ORDER, _WALLET, 2)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 3)
    await _insert_fill(repository, _OTHER_WALLET_ORDER, _OTHER_WALLET, 4)
    await _insert_fill(repository, _FOREIGN_ORDER, _WALLET, 5)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 6)
    await _insert_fill(repository, _PAPER_ORDER, _WALLET, 7)
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "paper", 1),
        (_WALLET, "walutomat", "live", 2),
        (_OTHER_WALLET, "walutomat", "live", 1),
        (_WALLET, "kraken", "live", 1),
        (_WALLET, "walutomat", "live", 3),
        (_WALLET, "walutomat", "paper", 2),
    ]


@pytest.mark.asyncio
async def test_rolled_back_insert_leaves_no_counter_gap(
    repository: SQLAlchemyRepository,
) -> None:
    """An aborted assignment is discarded — the counter is a column write.

    The contiguity proof depends on aborts leaving NO gaps: the value is
    written with the row, not drawn from a sequence, so a rollback returns
    it implicitly and the next committed fill re-mints it.

    Given: One committed fill, then an ingest attempt that fails AFTER
        counter assignment (its CHECK-violating side aborts the INSERT).
    When: A valid fill is ingested next.
    Then: The failed attempt persisted nothing and the valid fill carries
        counter 2 — contiguous, gap-free.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    with pytest.raises(IntegrityError):
        await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2, side="hold")
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 3)
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "live", 2),
    ]


@pytest.mark.asyncio
async def test_dangling_order_lineage_refuses_the_fill_pre_persistence(
    repository: SQLAlchemyRepository,
) -> None:
    """A fill without an active order version is refused entry.

    An unresolvable row could never be scoped, so it must not enter the
    ledger at all — this is what keeps boundary captures free of
    wallet-wide lineage scans.

    Given: A seeded lineage and an order public id with no active version.
    When: A fill referencing that order is ingested.
    Then: The typed refusal names the dangling-order reason and no row is
        persisted.
    """
    await _seed_lineage(repository)
    with pytest.raises(ExecutionScopeResolutionError, match="dangling_execution_order_lineage"):
        await _insert_fill(repository, _MISSING_ORDER, _WALLET, 1)
    assert await _stored_scopes(repository) == []


@pytest.mark.asyncio
async def test_crossed_wallet_lineage_refuses_the_fill_pre_persistence(
    repository: SQLAlchemyRepository,
) -> None:
    """A fill referencing another wallet's order is a hard refusal.

    Given: A seeded lineage where the referenced order belongs to the
        other wallet.
    When: A fill labelled with this wallet is ingested through it.
    Then: The typed refusal names the crossed-wallet reason and no row is
        persisted.
    """
    await _seed_lineage(repository)
    with pytest.raises(ExecutionScopeResolutionError, match="crossed_wallet_execution_lineage"):
        await _insert_fill(repository, _OTHER_WALLET_ORDER, _WALLET, 1)
    assert await _stored_scopes(repository) == []


@pytest.mark.asyncio
async def test_non_uuid_stored_order_wallet_refuses_the_fill(
    repository: SQLAlchemyRepository,
) -> None:
    """A corrupt non-UUID wallet on the stored order fails the fill closed.

    The caller's wallet is canonicalized and validated at the top of the
    ingest, but the ORDER row's wallet is read from storage and SQLite
    keeps ``UUIDColumn`` text verbatim, so a legacy or hand-written row
    can hold a non-UUID identity. The scope-key comparison falls back to
    a raw comparison for such a value rather than crashing, and the
    resulting mismatch refuses the fill instead of minting a counter
    against unresolvable lineage.

    Given: An active order whose stored wallet identity is not a UUID.
    When: A fill labelled with a canonical wallet is ingested through it.
    Then: The typed crossed-wallet refusal fires and no row is persisted.
    """
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(_LIVE_INSTRUMENT, "walutomat"),
                _order(_LIVE_ORDER, _LIVE_INSTRUMENT, "live", "legacy-wallet-identity"),
            ]
        )
        await s.commit()
    with pytest.raises(ExecutionScopeResolutionError, match="crossed_wallet_execution_lineage"):
        await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    assert await _stored_scopes(repository) == []


@pytest.mark.asyncio
async def test_alias_wallet_spellings_share_one_canonical_counter_scope(
    repository: SQLAlchemyRepository,
) -> None:
    """Alias UUID spellings canonicalize into ONE persisted counter scope.

    SQLite stores ``UUIDColumn`` values verbatim, so without ingest-side
    canonicalization ``...ABC...`` and ``...abc...`` would each mint
    sequence 1 in separate text-keyed scopes and the canonical watermark
    capture would see only one spelling. The wallet must be canonicalized
    ONCE and used for the crossed-wallet gate, the max read, AND the
    persisted row.

    Given: An active order stored under the canonical wallet spelling.
    When: One fill is ingested under the canonical spelling and one under
        the uppercase alias spelling.
    Then: Both rows persist under the CANONICAL spelling with contiguous
        counters 1 and 2, and the capture watermark for the alias
        spelling reads 2 — one logical scope.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET.upper(), 2)
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "live", 2),
    ]
    watermark, _ = await repository.get_spot_execution_watermark(
        _WALLET.upper(), "walutomat", "live", _NOW
    )
    assert watermark == 2


@pytest.mark.asyncio
async def test_malformed_wallet_identity_refuses_the_fill_pre_persistence(
    repository: SQLAlchemyRepository,
) -> None:
    """A non-UUID wallet identity is a typed fail-closed refusal.

    A malformed identity could never commit into PostgreSQL's native
    ``uuid`` column, and on SQLite it would mint a raw-text scope the
    canonical watermark capture can never read — so the writer refuses it
    with the same contained error type as the lineage refusals (the
    fill-persistence caller logs and continues; the executor pipeline is
    never crashed).

    Given: A seeded lineage and a wallet identity that is not a UUID in
        any spelling.
    When: A fill is ingested under it.
    Then: The typed refusal names the invalid-wallet reason and no row is
        persisted.
    """
    await _seed_lineage(repository)
    with pytest.raises(ExecutionScopeResolutionError, match="invalid_execution_wallet_identity"):
        await _insert_fill(repository, _LIVE_ORDER, "not-a-wallet-uuid", 1)
    assert await _stored_scopes(repository) == []


@pytest.mark.asyncio
async def test_waiter_connection_blocks_then_allocates_the_next_counter(
    repository: SQLAlchemyRepository,
    peer_repository: SQLAlchemyRepository,
) -> None:
    """A second connection waits out the write reservation, never colliding.

    The SQLite ``BEGIN IMMEDIATE`` opener is the allocation serializer:
    while one connection holds the reservation with an uncommitted
    ``max+1`` assignment, a peer connection's ingest must WAIT (not read
    the same committed maximum) and then allocate the NEXT value. Without
    the reservation both connections read the same maximum and the loser
    becomes a permanently lost published fill.

    Given: One committed fill, then a holder session replaying the writer
        protocol (BEGIN IMMEDIATE -> max+1 -> INSERT) held uncommitted.
    When: A peer engine's production ingest starts while the holder is
        mid-flight, and the holder then commits.
    Then: The peer ingest is still pending while the reservation is held,
        completes after the commit, and the committed counters are the
        contiguous 1, 2, 3 — both fills persisted, no collision.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    holder = repository.session_factory()
    try:
        await holder.execute(text("BEGIN IMMEDIATE"))
        pending = int(
            (
                await holder.execute(
                    select(func.coalesce(func.max(Execution.scope_sequence), 0) + 1).where(
                        Execution.wallet_public_id == _WALLET,
                        Execution.exchange == "walutomat",
                        Execution.mode == "live",
                    )
                )
            ).scalar_one()
        )
        holder.add(
            Execution(
                order_public_id=_LIVE_ORDER,
                wallet_public_id=_WALLET,
                exchange="walutomat",
                mode="live",
                scope_sequence=pending,
                timestamp=_NOW - timedelta(minutes=30),
                side="buy",
                status="filled",
                price=4.25,
                size=100.0,
                fee=0.5,
                fee_asset="PLN",
                session_id=_SESSION,
                sequence_id=2,
            )
        )
        await holder.flush()
        waiter = asyncio.ensure_future(_insert_fill(peer_repository, _LIVE_ORDER, _WALLET, 3))
        await asyncio.sleep(0.3)
        assert not waiter.done()
        await holder.commit()
        await asyncio.wait_for(waiter, timeout=_WAITER_TIMEOUT_SECONDS)
    finally:
        await holder.close()
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "live", 2),
        (_WALLET, "walutomat", "live", 3),
    ]


@pytest.mark.asyncio
async def test_concurrent_connections_mint_distinct_contiguous_counters(
    repository: SQLAlchemyRepository,
    peer_repository: SQLAlchemyRepository,
) -> None:
    """Racing ingests across two engines keep the counter gap-free.

    A genuine two-connection race: six fills released by one barrier,
    three per engine. Unserialized allocation would read duplicate
    maxima and lose fills on ``uq_executions_scope_sequence``; six
    ingests completing without an ``IntegrityError`` and landing on
    exactly 1..6 is the witness that the write reservation serialized
    them.

    Given: Six fills held at a barrier, three per engine generation.
    When: They ingest concurrently through the production path, bounded
        so a reservation deadlock fails instead of hanging.
    Then: All six commit and the committed counters are exactly 1..6.
    """
    await _seed_lineage(repository)
    barrier = asyncio.Barrier(6)

    async def _race(generation: SQLAlchemyRepository, sequence_id: int) -> int:
        """Hold one fill at the barrier, then ingest it through production code."""
        await barrier.wait()
        return await _insert_fill(generation, _LIVE_ORDER, _WALLET, sequence_id)

    await asyncio.wait_for(
        asyncio.gather(
            _race(repository, 1),
            _race(peer_repository, 2),
            _race(repository, 3),
            _race(peer_repository, 4),
            _race(repository, 5),
            _race(peer_repository, 6),
        ),
        timeout=_WAITER_TIMEOUT_SECONDS,
    )
    assert [row[3] for row in await _stored_scopes(repository)] == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_dangling_instrument_lineage_refuses_the_fill_pre_persistence(
    repository: SQLAlchemyRepository,
) -> None:
    """A fill whose order lacks an active instrument is refused entry.

    Given: A seeded lineage plus an active order referencing an instrument
        public id with no active version.
    When: A fill is ingested through that order.
    Then: The typed refusal names the dangling-instrument reason and no
        row is persisted.
    """
    await _seed_lineage(repository)
    async with repository.session() as s:
        s.add(_order(_ORPHAN_INSTRUMENT_ORDER, _MISSING_INSTRUMENT, "live", _WALLET))
        await s.commit()
    with pytest.raises(
        ExecutionScopeResolutionError, match="dangling_execution_instrument_lineage"
    ):
        await _insert_fill(repository, _ORPHAN_INSTRUMENT_ORDER, _WALLET, 1)
    assert await _stored_scopes(repository) == []


@pytest.mark.asyncio
async def test_unique_index_backstops_a_raw_duplicate_counter_write(
    repository: SQLAlchemyRepository,
) -> None:
    """A writer bypassing the fence hits the total-unique index, loudly.

    The index is the fail-closed backstop of the contiguity proof: a
    double allocation (or a future SCD2 supersede attempt, which would
    re-insert the same scope and counter) must fail with
    ``IntegrityError`` instead of silently corrupting the counted range.

    Given: One fill ingested through the production path.
    When: A raw ORM write duplicates its (wallet, exchange, mode,
        scope_sequence) coordinate.
    Then: The commit raises ``IntegrityError`` and the ledger keeps one row.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    async with repository.session() as s:
        s.add(
            Execution(
                order_public_id=_LIVE_ORDER,
                wallet_public_id=_WALLET,
                exchange="walutomat",
                mode="live",
                scope_sequence=1,
                timestamp=_NOW,
                side="buy",
                status="filled",
                price=4.25,
                size=100.0,
                fee=0.5,
                fee_asset="PLN",
                session_id=_SESSION,
                sequence_id=2,
            )
        )
        with pytest.raises(IntegrityError):
            await s.commit()
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
    ]


@pytest.mark.asyncio
async def test_deleting_the_scope_tip_is_physically_rejected_by_the_ledger_trigger(
    repository: SQLAlchemyRepository,
) -> None:
    """A raw tip DELETE is physically refused, so the freed-counter re-mint cannot occur.

    Discharges the monotonicity PRECONDITION of the sealed-prefix proof
    (`design_2026_07_17_execution_scope_counter.md` §0) — and upgrades it
    from a documented refusal into a DATABASE theorem. ``K`` is DERIVED
    from the table on every insert — ``coalesce(max(scope_sequence), 0) + 1``
    — never stored, so the fence proves allocation order == commit order
    but not that ``K`` is monotone. That premise used to survive only
    because no supported path deletes an execution row; now the physical
    ``executions_reject_delete`` trigger makes the deleted-tip hazard
    unreachable by construction. This test issues the raw ``DELETE`` a
    manual console, a restore tool, or a bug would take and proves the
    database refuses it, so the freed-counter re-mint the old test
    demonstrated can never happen.

    Given: A live scope holding the contiguous counters 1..3, the tip
        (counter 3) being a BUY.
    When: The tip row is deleted by raw SQL — the path a manual DELETE or
        a bug would take.
    Then: The database raises an ``append-only`` ``DBAPIError``, the tip
        row is still present, the captured watermark stays at 3, counter 3
        still denotes the original BUY fill, and the stored scopes are
        unchanged — the monotonicity premise is a DB theorem, not caution.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
    tip_id = await _insert_fill(repository, _LIVE_ORDER, _WALLET, 3, side="buy")
    before, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert before == 3
    assert await _fill_identity_at_sequence(repository, 3) == (3, "buy")
    with pytest.raises(DBAPIError, match="append-only"):
        await _attempt_tip_delete(repository, tip_id)
    unchanged, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert unchanged == 3
    assert await _fill_identity_at_sequence(repository, 3) == (3, "buy")
    assert await _stored_scopes(repository) == [
        (_WALLET, "walutomat", "live", 1),
        (_WALLET, "walutomat", "live", 2),
        (_WALLET, "walutomat", "live", 3),
    ]


@pytest.mark.asyncio
async def test_tip_delete_is_rejected_so_the_counted_range_proof_can_never_be_falsified(
    repository: SQLAlchemyRepository,
) -> None:
    """The counted proof stays whole because the substitution is prevented at the DB layer.

    Executes §6.4's counterexample against real rows and evaluates §5's
    counted range proof directly — but proves the substitution is
    PREVENTED at the database layer rather than merely detected-then-
    self-healed. The old hazard was that once the allocator re-mints a
    freed counter the range is arithmetically whole again while carrying a
    DIFFERENT fill than the one certified; that reintroduction is now
    impossible because the raw tip ``DELETE`` never commits. The counted
    proof therefore reads COMPLETE before AND after the refused delete, and
    the certified counter keeps its original fill.

    Given: A live scope with counters 1..3 under an anchor watermark of 1,
        so the certified range is (1, 3] and its counted proof holds over
        a tip that is a BUY.
    When: The tip is deleted by raw SQL.
    Then: The delete raises an ``append-only`` ``DBAPIError``, the counted
        proof is STILL COMPLETE, and certified counter 3 STILL holds the
        original BUY fill — the substitution the proof once could not see
        can never be constructed.
    """
    await _seed_lineage(repository)
    anchor_seq = 1
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
    tip_id = await _insert_fill(repository, _LIVE_ORDER, _WALLET, 3, side="buy")
    watermark, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert watermark == 3
    assert await _counted_range_complete(repository, anchor_seq, watermark) is True
    assert await _fill_identity_at_sequence(repository, 3) == (3, "buy")
    with pytest.raises(DBAPIError, match="append-only"):
        await _attempt_tip_delete(repository, tip_id)
    assert await _counted_range_complete(repository, anchor_seq, watermark) is True
    still_sealed, _ = await repository.get_spot_execution_watermark(
        _WALLET, "walutomat", "live", _NOW
    )
    assert still_sealed == watermark
    assert await _fill_identity_at_sequence(repository, 3) == (3, "buy")
