"""Tests for the unlocked, column-scoped spot execution watermark capture.

The watermark is the committed per-scope ``max(scope_sequence)`` append
boundary captured BEFORE a venue balance read. The counter is allocated by
``insert_execution`` as committed max + 1 while holding the per-wallet
execution fence, so the committed values per scope are contiguous and the
UNLOCKED capture read is a sealed prefix by construction — the capture
takes no lock, sets no ``lock_timeout``, and probes no id sequence. Scope
membership is the stored immutable ``exchange``/``mode`` columns, so the
functional tests seed real fills through ``insert_execution`` (which
resolves scope from the active Order -> Instrument lineage) in an isolated
SQLite database, while the PostgreSQL-only statement protocol is pinned
through the repository's mocked-dialect pattern (statement text and order,
without a live server).
"""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_WALLET = "0000c0de-0000-7000-8000-00000000a101"
_OTHER_WALLET = "0000c0de-0000-7000-8000-00000000a102"
_SESSION = "00000000-0000-7000-8000-000000000301"
_LIVE_INSTRUMENT = "00000000-0000-7000-8000-000000000501"
_FOREIGN_INSTRUMENT = "00000000-0000-7000-8000-000000000502"
_LIVE_ORDER = "00000000-0000-7000-8000-000000000601"
_PAPER_ORDER = "00000000-0000-7000-8000-000000000602"
_FOREIGN_ORDER = "00000000-0000-7000-8000-000000000603"
_OTHER_WALLET_ORDER = "00000000-0000-7000-8000-000000000604"
_FENCE_STATEMENT_MARKERS = ("pg_advisory_xact_lock", "lock_timeout", "pg_sequence")


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create one isolated repository containing only the scope-plane tables."""
    db_path = tmp_path / "watermark-async.db"
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
) -> int:
    """Persist one fill through the production ingest path, returning its id."""
    return await repository.insert_execution(
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        timestamp=_NOW - timedelta(hours=1),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


async def _seed_scoped_history(repository: SQLAlchemyRepository) -> None:
    """Ingest one in-scope fill among LATER fills in three sibling scopes.

    The live walutomat scope for the wallet receives exactly one fill
    FIRST; the paper scope, the foreign-exchange scope, and the other
    wallet's scope each receive a later fill, so any scope leak in the
    capture would inflate the boundary above 1.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _OTHER_WALLET_ORDER, _OTHER_WALLET, 2)
    await _insert_fill(repository, _PAPER_ORDER, _WALLET, 3)
    await _insert_fill(repository, _FOREIGN_ORDER, _WALLET, 4)


@pytest.mark.asyncio
async def test_empty_history_yields_zero_watermark_and_the_as_of_echo(
    repository: SQLAlchemyRepository,
) -> None:
    """An account with no execution history is bounded by watermark zero.

    Given: An empty executions table.
    When: The scoped watermark is captured.
    Then: The boundary is 0 (valid per the anchor watermark >= 0 contract)
        and the exact pinning instant travels back with it.
    """
    watermark, as_of = await repository.get_spot_execution_watermark(
        _WALLET, "walutomat", "live", _NOW
    )
    assert watermark == 0
    assert as_of == _NOW


@pytest.mark.asyncio
async def test_watermark_isolates_wallet_exchange_and_mode_via_stored_columns(
    repository: SQLAlchemyRepository,
) -> None:
    """Foreign-wallet, paper-mode, and foreign-exchange fills never leak in.

    Given: One in-scope fill followed by later fills that each live in a
        sibling scope (other wallet, paper mode, foreign exchange), all
        ingested through the counter-assigning production path.
    When: The scoped watermark is captured for the live walutomat account.
    Then: The boundary is exactly 1 — the in-scope fill's own counter —
        unaffected by every later out-of-scope append.
    """
    await _seed_scoped_history(repository)
    watermark, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert watermark == 1


@pytest.mark.asyncio
async def test_watermark_advances_with_scoped_appends(
    repository: SQLAlchemyRepository,
) -> None:
    """A later in-scope fill moves the boundary to the new scope maximum.

    Given: A seeded history whose latest rows are out-of-scope fills.
    When: One more in-scope fill is ingested and the watermark is re-read.
    Then: The boundary advances to exactly 2 — the fresh fill's counter.
    """
    await _seed_scoped_history(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 5)
    watermark, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert watermark == 2


@pytest.mark.asyncio
async def test_as_of_returned_is_the_exact_instant_passed(
    repository: SQLAlchemyRepository,
) -> None:
    """The capture echoes the identical pinning instant it received.

    ``as_of`` no longer defines scope membership (the stored columns do);
    it pins the S4c-4 replay bundle's temporal reads — instrument
    identity, specs, asset precisions — so the capture's contract is to
    hand back the exact instant, never a fresh clock read.

    Given: A seeded scoped history and one explicit capture instant.
    When: The watermark is captured at that instant.
    Then: The returned ``as_of`` equals the passed instant exactly.
    """
    await _seed_scoped_history(repository)
    capture_instant = _NOW + timedelta(minutes=7)
    _, as_of = await repository.get_spot_execution_watermark(
        _WALLET, "walutomat", "live", capture_instant
    )
    assert as_of == capture_instant


@pytest.mark.asyncio
async def test_watermark_reads_stored_scope_columns_not_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """Scope membership is the stored columns, never a join reconstruction.

    Lineage integrity moved to ingest; the capture must count a row by its
    immutable stored scope even when that scope disagrees with what a
    fresh Order/Instrument join would derive — the stored columns are the
    certification truth the counter was allocated under.

    Given: A seeded history plus one raw ledger row stored IN the live
        walutomat scope with counter 2 while referencing the paper order.
    When: The live walutomat watermark is captured.
    Then: The boundary is 2 — the stored scope counted the row; no join
        recomputation excluded it.
    """
    await _seed_scoped_history(repository)
    async with repository.session() as s:
        s.add(
            Execution(
                order_public_id=_PAPER_ORDER,
                wallet_public_id=_WALLET,
                exchange="walutomat",
                mode="live",
                scope_sequence=2,
                timestamp=_NOW - timedelta(minutes=30),
                side="buy",
                status="filled",
                price=4.25,
                size=100.0,
                fee=0.5,
                fee_asset="PLN",
                session_id=_SESSION,
                sequence_id=9,
            )
        )
        await s.commit()
    watermark, _ = await repository.get_spot_execution_watermark(_WALLET, "walutomat", "live", _NOW)
    assert watermark == 2


@pytest.mark.asyncio
async def test_alias_wallet_spelling_reads_the_canonical_history(
    repository: SQLAlchemyRepository,
) -> None:
    """Alias UUID spellings are canonicalized before the scope filter.

    Given: A seeded history stored under the canonical wallet spelling.
    When: The watermark is requested with an uppercase alias spelling.
    Then: The same boundary is returned as for the canonical spelling.
    """
    await _seed_scoped_history(repository)
    watermark, _ = await repository.get_spot_execution_watermark(
        _WALLET.upper(), "walutomat", "live", _NOW
    )
    assert watermark == 1


@pytest.mark.asyncio
async def test_malformed_wallet_identity_raises_like_reconciliation_writers(
    repository: SQLAlchemyRepository,
) -> None:
    """A malformed wallet identity fails loudly instead of scanning nothing.

    Given: A wallet identity that is not a UUID in any spelling.
    When: The scoped watermark is requested.
    Then: ``ValueError`` propagates, matching the reconciliation writers.
    """
    with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
        await repository.get_spot_execution_watermark("not-a-uuid", "walutomat", "live", _NOW)


@contextmanager
def _postgres_mocked_repository(
    repository: SQLAlchemyRepository,
    sessions: list[AsyncMock],
) -> Iterator[None]:
    """Patch the dialect and session plumbing for mocked-PostgreSQL calls.

    The repository keeps its real SQLite engine but reports ``postgresql``
    and hands out the given mocked sessions in order, so tests can pin the
    exact PG statement protocol without a live server.
    """
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session") as session_context,
    ):
        session_context.return_value.__aenter__.side_effect = sessions
        session_context.return_value.__aexit__.return_value = None
        yield


class TestPostgresStatements:
    """PostgreSQL statement protocol pinned by text and order (no live PG)."""

    @pytest.mark.asyncio
    async def test_capture_emits_one_unlocked_scoped_read_and_nothing_else(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """The capture is a single unlocked read — fence machinery is GONE.

        The counter's contiguity seals the committed prefix, so the
        capture must emit NO advisory lock, NO ``lock_timeout``, and NO
        ``pg_sequence`` probe — their absence is the structural proof that
        the capture can never serialize with or stall behind fill ingest.

        Given: A repository whose dialect reports ``postgresql`` over a
            mocked session.
        When: The watermark capture runs.
        Then: Exactly one statement executes — the scoped
            ``max(scope_sequence)`` SELECT — and no statement mentions any
            fence marker; the empty scope maps to the zero boundary with
            the pinning instant echoed.
        """
        read_result = MagicMock()
        read_result.scalar.return_value = None
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[read_result])
        with _postgres_mocked_repository(repository, [session]):
            watermark, as_of = await repository.get_spot_execution_watermark(
                _WALLET, "walutomat", "live", _NOW
            )
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert len(statements) == 1
        assert "scope_sequence" in statements[0]
        for marker in _FENCE_STATEMENT_MARKERS:
            assert not any(marker in statement for statement in statements)
        assert watermark == 0
        assert as_of == _NOW

    @pytest.mark.asyncio
    async def test_insert_emits_isolation_resolve_fence_max_and_insert_in_order(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """The insert runs isolation -> resolve -> fence -> max+1 -> INSERT.

        The explicit ``READ COMMITTED`` is the transaction's FIRST
        statement — the engine inherits the role/database default, and an
        inherited REPEATABLE READ snapshot pinned by the resolve SELECT
        would read a stale committed maximum after waiting out another
        fence holder. The resolve precedes the fence so lineage work never
        lengthens the lock hold, and the committed-max read happens
        strictly AFTER the fence acquisition — the lock-before-max+1
        ordering is the load-bearing precondition of the contiguity proof.

        Given: A repository whose dialect reports ``postgresql`` over a
            mocked session with one ordered call log.
        When: ``insert_execution`` runs (with an UPPERCASE alias wallet
            spelling, which must never leak past canonicalization).
        Then: The isolation statement is first, the scope-resolve SELECT
            second, the execution-fence advisory lock third (keyed by the
            CANONICAL wallet spelling), the scoped max read fourth, and
            only then the ORM add/commit that emit the ``INSERT``.
        """
        calls: list[str] = []
        resolve_result = MagicMock()
        resolve_result.first.return_value = SimpleNamespace(
            mode="live", wallet_public_id=_WALLET, exchange="walutomat"
        )
        max_result = MagicMock()
        max_result.scalar_one.return_value = 1

        async def _record_execute(statement: object, params: object = None) -> MagicMock:
            text = str(statement)
            if "SET TRANSACTION" in text:
                calls.append("isolation")
                return MagicMock()
            if "pg_advisory_xact_lock" in text:
                calls.append("fence")
                return MagicMock()
            if "max" in text and "scope_sequence" in text:
                calls.append("max")
                return max_result
            calls.append("resolve")
            return resolve_result

        def _record_add(_obj: object) -> None:
            calls.append("add")

        async def _record_commit() -> None:
            calls.append("commit")

        session = AsyncMock(add=MagicMock(side_effect=_record_add))
        session.execute = AsyncMock(side_effect=_record_execute)
        session.commit = AsyncMock(side_effect=_record_commit)
        with _postgres_mocked_repository(repository, [session]):
            await repository.insert_execution(
                order_public_id=_LIVE_ORDER,
                wallet_public_id=_WALLET.upper(),
                timestamp=_NOW,
                side="buy",
                status="filled",
                price=4.25,
                size=100.0,
                fee=0.5,
                fee_asset="PLN",
                session_id=_SESSION,
                sequence_id=1,
            )
        assert calls == ["isolation", "resolve", "fence", "max", "add", "commit"]
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        assert "orders" in statements[1]
        assert "instruments" in statements[1]
        assert (
            statements[2]
            == "SELECT pg_advisory_xact_lock(hashtext('execution_fence'), hashtext(:wid))"
        )
        assert session.execute.await_args_list[2].args[1] == {"wid": _WALLET}

    @pytest.mark.asyncio
    async def test_unsupported_dialect_fails_the_insert_transaction_closed(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Unknown dialects raise before any allocation statement runs.

        The transaction opener carries the dialect's allocation
        guarantee (explicit isolation on PostgreSQL, the write
        reservation on SQLite); a dialect without one must fail closed
        like the fence rather than allocate without a guarantee.

        Given: A repository whose dialect reports ``mysql``.
        When: The insert transaction opener runs.
        Then: ``NotImplementedError`` is raised and no SQL is executed.
        """
        session = AsyncMock()
        with (
            patch.object(
                SQLAlchemyRepository,
                "dialect_name",
                new_callable=PropertyMock,
                return_value="mysql",
            ),
            pytest.raises(NotImplementedError, match="mysql"),
        ):
            await repository._begin_execution_insert_transaction(session)
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_fence_lock_key_is_canonicalized_for_alias_spellings(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Alias UUID spellings hash to the canonical fence key.

        PostgreSQL stores wallets in a native ``uuid`` column, so an
        alias-spelled insert allocates in the same stored scope while
        ``hashtext`` would otherwise key a different lock — the fence must
        canonicalize.

        Given: A mocked-PG session and an uppercase wallet spelling.
        When: The fence lock is acquired.
        Then: The bound key is the canonical lowercase UUID spelling.
        """
        session = AsyncMock()
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ):
            await repository._acquire_execution_fence_lock(session, _WALLET.upper())
        assert session.execute.await_args.args[1] == {"wid": _WALLET}

    @pytest.mark.asyncio
    async def test_fence_lock_key_falls_back_to_raw_non_uuid_identity(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A non-UUID identity keys the fence with its raw spelling.

        Harmless on PostgreSQL — such a value can never commit into the
        native ``uuid`` wallet column — but the helper must not raise and
        drop a fill on the ingest path.

        Given: A mocked-PG session and a non-UUID wallet identity.
        When: The fence lock is acquired.
        Then: The bound key is the raw identity string.
        """
        session = AsyncMock()
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ):
            await repository._acquire_execution_fence_lock(session, "wallet-1")
        assert session.execute.await_args.args[1] == {"wid": "wallet-1"}

    @pytest.mark.asyncio
    async def test_unsupported_dialect_fails_the_fence_closed(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Unknown dialects raise instead of silently skipping the fence.

        Given: A repository whose dialect reports ``mysql``.
        When: The execution fence lock is acquired.
        Then: ``NotImplementedError`` is raised and no SQL is executed.
        """
        session = AsyncMock()
        with (
            patch.object(
                SQLAlchemyRepository,
                "dialect_name",
                new_callable=PropertyMock,
                return_value="mysql",
            ),
            pytest.raises(NotImplementedError, match="mysql"),
        ):
            await repository._acquire_execution_fence_lock(session, _WALLET)
        session.execute.assert_not_called()


class TestSqliteFenceDegeneration:
    """SQLite serializes via the BEGIN IMMEDIATE reservation, not the fence."""

    @pytest.mark.asyncio
    async def test_sqlite_insert_takes_the_reservation_and_capture_takes_nothing(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """The SQLite insert opens with BEGIN IMMEDIATE; the capture is bare.

        The write reservation — not the engine's page-level single writer
        — is what serializes the read-then-write ``max+1`` allocation
        across connections, so the insert transaction must OPEN with it;
        the unlocked capture must stay lock-free in every dialect.

        Given: A real SQLite repository with an SQL statement recorder
            split between the ingest and the capture.
        When: One fill is ingested and the scoped watermark is captured.
        Then: The insert portion opens with ``BEGIN IMMEDIATE`` and emits
            its ``INSERT``, the capture portion emits NEITHER a
            reservation nor any statement at all beyond its single SELECT,
            and no recorded statement mentions the advisory fence, the
            lock timeout, or the retired sequence-cache probe.
        """
        async with repository.session() as s:
            s.add_all(
                [
                    _instrument(_LIVE_INSTRUMENT, "walutomat"),
                    _order(_LIVE_ORDER, _LIVE_INSTRUMENT, "live", _WALLET),
                ]
            )
            await s.commit()
        statements: list[str] = []

        def _record(*args: object) -> None:
            """Record every SQL string the engine is about to execute."""
            if len(args) > 2 and isinstance(args[2], str):
                statements.append(args[2])

        event.listen(repository.engine.sync_engine, "before_cursor_execute", _record)
        try:
            await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
            insert_statements = list(statements)
            statements.clear()
            watermark, _ = await repository.get_spot_execution_watermark(
                _WALLET, "walutomat", "live", _NOW
            )
            capture_statements = list(statements)
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", _record)
        assert watermark == 1
        assert insert_statements[0] == "BEGIN IMMEDIATE"
        assert any(
            statement.lstrip().upper().startswith("INSERT") for statement in insert_statements
        )
        assert len(capture_statements) == 1
        assert "scope_sequence" in capture_statements[0]
        assert "BEGIN IMMEDIATE" not in capture_statements
        for marker in _FENCE_STATEMENT_MARKERS:
            assert not any(
                marker in statement for statement in [*insert_statements, *capture_statements]
            )


@pytest.mark.asyncio
async def test_witness_rows_return_the_contiguous_sealed_prefix(
    repository: SQLAlchemyRepository,
) -> None:
    """Two committed live walutomat fills return their scope sequences in order.

    Given: Two committed fills for the live walutomat scope,
    When: The witness rows are read up to watermark 2,
    Then: Both scope sequences and their exec ids project in ascending order.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
    rows = await repository.get_spot_execution_witness_rows(_WALLET, "walutomat", "live", 2)
    assert rows == [
        {"scope_sequence": 1, "exec_id": None},
        {"scope_sequence": 2, "exec_id": None},
    ]


@pytest.mark.asyncio
async def test_witness_rows_fail_closed_on_a_non_contiguous_prefix(
    repository: SQLAlchemyRepository,
) -> None:
    """A gap in the sealed prefix fails closed rather than seal an unwitnessed ledger.

    Given: One committed fill but a watermark of 2 (a purge or tamper gap),
    When: The witness rows are read,
    Then: ExecutionChainError is raised.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    with pytest.raises(ExecutionChainError, match="non-contiguous"):
        await repository.get_spot_execution_witness_rows(_WALLET, "walutomat", "live", 2)


@pytest.mark.asyncio
async def test_witness_rows_range_read_starts_after_the_from_watermark(
    repository: SQLAlchemyRepository,
) -> None:
    """A from_watermark range read returns only the rows after the trusted anchor.

    Given: Three committed fills for the live walutomat scope,
    When: The witness rows are read for the range (1, 3],
    Then: Only scope sequences 2 and 3 project, in ascending order.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 3)
    rows = await repository.get_spot_execution_witness_rows(
        _WALLET, "walutomat", "live", 3, from_watermark=1
    )
    assert rows == [
        {"scope_sequence": 2, "exec_id": None},
        {"scope_sequence": 3, "exec_id": None},
    ]


@pytest.mark.asyncio
async def test_witness_rows_fail_closed_on_a_non_contiguous_range(
    repository: SQLAlchemyRepository,
) -> None:
    """A gap inside a from_watermark range fails closed like a gapped prefix.

    Given: Two committed fills but a range (1, 3] expecting two rows above 1,
    When: The witness rows are read with from_watermark=1,
    Then: ExecutionChainError names the non-contiguous range.
    """
    await _seed_lineage(repository)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 1)
    await _insert_fill(repository, _LIVE_ORDER, _WALLET, 2)
    with pytest.raises(ExecutionChainError, match="range"):
        await repository.get_spot_execution_witness_rows(
            _WALLET, "walutomat", "live", 3, from_watermark=1
        )
