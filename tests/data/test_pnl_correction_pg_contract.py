"""Driver-boundary coverage for scoped PostgreSQL correction budgets and locks.

The companion disposable PG18 witness executes these limits on a real server,
including shorter-setting preservation and rollback. These tests verify that
each repository entry point actually requests the guarded driver path.
"""

import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import PortfolioPnlCorrectionFrontier
from snapper.data.models import PortfolioPnlOpeningCheckpoint
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Symbol
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.pnl_correction_state import PortfolioPnlCorrectionConflictError
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlFxRatePlane
from tests.data import test_pnl_timeline_sample_repository as samples
from tests.data.test_pnl_fx_rate_repository import _candle as _fx_candle
from tests.data.test_pnl_fx_rate_repository import _instrument as _fx_instrument
from tests.data.test_pnl_fx_rate_repository import _symbol as _fx_symbol


@pytest.mark.parametrize("read_evidence", [False, True])
async def test_postgres_control_reads_apply_local_budgets_before_reading(
    read_evidence: bool,
) -> None:
    """Both public control readers select the PostgreSQL budget path.

    Args:
        read_evidence: Whether the full chunk witness or the checkpoint is read.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession, add=MagicMock())
    result = MagicMock()
    result.scalars.return_value.one_or_none.return_value = None
    result.scalars.return_value.__iter__.return_value = iter(())
    result.scalar.return_value = None
    session.execute.return_value = result
    try:
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ), patch.object(repo, "session_factory", return_value=session):
            if read_evidence:
                evidence = await repo.get_portfolio_pnl_correction_evidence(
                    samples._scope(), samples._M1, samples._M3
                )
                assert evidence.versions == () and evidence.prior_peak is None
            else:
                assert await repo.get_portfolio_pnl_correction_frontier(samples._scope()) is None
        first = str(session.execute.await_args_list[0].args[0])
        assert "set_config('statement_timeout'" in first
        assert "set_config('lock_timeout'" in first
        session.close.assert_awaited_once()
    finally:
        await repo.engine.dispose()


async def test_postgres_writer_bounds_lock_wait_before_acquiring_the_shared_fence() -> None:
    """The correction writer configures limits before its potentially blocking lock."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    try:
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ):
            await repo._begin_portfolio_pnl_sample_write_transaction(
                session, samples._scope(), bounded=True
            )
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert statements[0].startswith("SET TRANSACTION")
        assert "set_config('lock_timeout'" in statements[1]
        assert "pg_advisory_xact_lock" in statements[2]
        assert not any("LOCK TABLE" in statement for statement in statements)
    finally:
        await repo.engine.dispose()


async def test_postgres_opening_checkpoint_read_and_upsert_compile_pg_statements() -> None:
    """Verify PostgreSQL checkpoint reads bound timeouts and upserts emit ON CONFLICT DO UPDATE.

    Given: A ``SQLAlchemyRepository`` with ``dialect_name`` mocked to ``"postgresql"``
        and an isolated ``AsyncSession`` mock.
    When: ``get_portfolio_pnl_opening_checkpoint`` and ``_persist_pnl_opening_checkpoint``
        execute under the mocked PostgreSQL dialect.
    Then: The read sets local timeouts first and the upsert emits a PostgreSQL
        ``INSERT ... ON CONFLICT ... DO UPDATE`` targeting all non-PK columns.
    """
    scope, _, key = _pg_scope()
    m1 = scope.anchor_point_time + timedelta(minutes=1)
    m3 = scope.anchor_point_time + timedelta(minutes=3)
    m5 = scope.anchor_point_time + timedelta(minutes=5)
    frontier = PnlCorrectionFrontier(
        key=key,
        generation=1,
        revision=2,
        window_start=m1,
        next_minute=None,
        tip=m3,
        as_of=m5,
        baseline_watermarks={"kraken": 2},
        pending_watermarks={"kraken": 2},
        prior_peak=None,
        repair_from=None,
        raw_execution_watermarks={"kraken": 2},
        execution_manifest_digest=_PG_DIGEST_A,
        has_fill_gap=False,
        expected_venues=frozenset({"kraken"}),
        venue_event_watermarks={"kraken:spot": 4},
        fill_witness_digest=_PG_DIGEST_B,
    )
    chunk = PnlCorrectionChunk(m3, (), None, PnlCorrectionEvidence(key, m1, m3, (), None))
    write = PnlOpeningCheckpointWrite(_pg_certificate(frontier), '{"pg":true}')
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.scalars.return_value.one_or_none.return_value = None
    session.execute.return_value = result
    try:
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ), patch.object(repo, "session_factory", return_value=session):
            assert await repo.get_portfolio_pnl_opening_checkpoint(scope) is None
            await repo._persist_pnl_opening_checkpoint(session, frontier, chunk, write)
        first = str(session.execute.await_args_list[0].args[0])
        upsert = str(session.execute.await_args_list[2].args[0])
        assert "set_config('statement_timeout'" in first
        assert "ON CONFLICT" in upsert
        assert "DO UPDATE SET" in upsert
    finally:
        await repo.engine.dispose()


async def test_postgres_event_fx_candle_fingerprint_compiles_pg_query() -> None:
    """Verify PostgreSQL FX candle fingerprint disables JIT and uses shared candidate CTE.

    Given: A ``SQLAlchemyRepository`` with ``dialect_name`` mocked to ``"postgresql"``
        and an isolated ``AsyncSession`` mock.
    When: ``get_pnl_event_fx_candle_fingerprint`` runs for a plane and open minute.
    Then: The session first executes ``SET LOCAL jit = off`` and then compiles the
        PostgreSQL candidate-instrument array aggregate over ``candles``.
    """
    m1 = datetime(2026, 7, 20, 12, 1, tzinfo=UTC)
    m5 = datetime(2026, 7, 20, 12, 5, tzinfo=UTC)
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.one.return_value = (0, None, None, None)
    session.execute.return_value = result
    try:
        with patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ), patch.object(repo, "session_factory", return_value=session):
            agg = await repo.get_pnl_event_fx_candle_fingerprint(
                [("EUR", "USD", "kraken")], [m1], m5
            )
        assert agg == IdAggregate(0, 0, 0, None)
        first = str(session.execute.await_args_list[0].args[0])
        stmt = session.execute.await_args_list[1].args[0]
        query = " ".join(str(stmt.compile(dialect=postgresql.dialect())).split())
        assert first == "SET LOCAL jit = off"
        assert "WITH pnl_fx_load_candidate_instruments AS MATERIALIZED" in query
        assert "AS UUID[]" in query
        assert "sum(candles.id)" in query
    finally:
        await repo.engine.dispose()


_PG_DIGEST_A = "a" * 64
_PG_DIGEST_B = "b" * 64
_PG_SESSION = "00000000-0000-7000-8000-000000000799"


@pytest.fixture
async def pg_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated PostgreSQL database from the ``PNL_TEST_POSTGRES_URL`` opt-in.

    Follows the repository's PostgreSQL opt-in convention: the tests never touch
    ``DB_URL``, create a throwaway database with the full schema, and drop it on
    teardown, so no shared or live database is ever written.

    Yields:
        A repository bound to the freshly created database.
    """
    configured = os.environ.get("PNL_TEST_POSTGRES_URL")
    if configured is None:
        pytest.skip("requires isolated PNL_TEST_POSTGRES_URL")
    base = make_url(configured)
    assert base.get_backend_name() == "postgresql"
    database_name = f"pnl_opening_checkpoint_{uuid4().hex}"
    maintenance = SQLAlchemyRepository(configured)
    created = False
    repo: SQLAlchemyRepository | None = None
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created = True
        repo = SQLAlchemyRepository(
            base.set(database=database_name).render_as_string(hide_password=False)
        )
        await repo.create_all()
        yield repo
    finally:
        if repo is not None:
            await repo.engine.dispose()
        if created:
            async with maintenance.engine.connect() as connection:
                admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        await maintenance.engine.dispose()


def _pg_scope() -> tuple[PortfolioPnlSampleScope, PortfolioPnlPoint, PnlCorrectionKey]:
    """Create unique per-test scope, anchor row, and correction key on live PostgreSQL.

    Returns:
        Tuple of sample scope, anchor ORM object, and correction key.
    """
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=10)
    wallet = str(uuid4())
    epoch = str(uuid4())
    anchor_id = str(uuid4())
    scope = PortfolioPnlSampleScope(wallet, "live", "USD", epoch, t0)
    anchor = PortfolioPnlPoint(
        public_id=anchor_id,
        session_id=_PG_SESSION,
        sequence_id=1,
        timestamp=t0,
        wallet_public_id=wallet,
        mode="live",
        valuation_ccy="USD",
        point_time=t0,
        point_kind="anchor",
        epoch_public_id=epoch,
        calc_version="5A.13",
        valuation_status="complete",
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        external_flow_adjustment=0.0,
        unrealized_pnl=5.0,
        mark_source="finalized_1m",
        mark_time=t0,
        opening_basket_json="{}",
        known_to=KNOWN_TO_MAX,
    )
    key = PnlCorrectionKey(wallet, "live", "USD", epoch, PNL_SAMPLE_CALC_VERSION)
    return scope, anchor, key


def _pg_certificate(frontier: PnlCorrectionFrontier) -> PnlOpeningCheckpointCertificate:
    """Build a valid opening-checkpoint certificate for ``frontier``.

    Args:
        frontier: Frontier whose generation, tip, and horizon the certificate binds.

    Returns:
        Validated ``PnlOpeningCheckpointCertificate`` ready to persist.
    """
    return PnlOpeningCheckpointCertificate(
        key=frontier.key,
        timeline_calc_version="5A.16",
        fx_policy_version="pnl-fiat-pointwise-v1",
        schema_version=1,
        anchor_public_id=str(uuid4()),
        anchor_digest=_PG_DIGEST_B,
        source_generation=frontier.generation,
        checkpoint_minute=frontier.tip,
        as_of=frontier.as_of,
        cutoff_execution_watermarks={"kraken": 2},
        execution_chain_digests={"kraken": _PG_DIGEST_A},
        cutoff_execution_counts={"kraken": 2},
        applied_watermarks_by_exchange={"kraken": 2},
        deferred_execution_sequences={},
        prefix_has_idless_executions=False,
        execution_manifest_digest=_PG_DIGEST_A,
        prefix_annulment_digest=_PG_DIGEST_A,
        accrual_prefix_fingerprint=IdAggregate(0, 0, 0, None),
        prefix_lineage_digest=_PG_DIGEST_A,
        prefix_fill_witness_digest=_PG_DIGEST_B,
        prefix_venue_event_watermarks={"kraken:spot": 4},
        source_expected_venues=frontier.expected_venues or frozenset({"kraken"}),
        source_venue_event_watermarks=frontier.venue_event_watermarks or {"kraken:spot": 4},
        source_fill_witness_digest=frontier.fill_witness_digest or _PG_DIGEST_B,
        symbol_ref_digest=_PG_DIGEST_A,
        consumed_fx_pairs=[("EUR", "USD")],
        consumed_fx_open_ats=[frontier.tip],
        event_fx_exchanges_digest=_PG_DIGEST_A,
        event_fx_candles_fingerprint=IdAggregate(3, (3 << 62) + 6, (1 << 62) + 3, frontier.tip),
    )


async def _cleanup_pg_scope(repo: SQLAlchemyRepository, wallet: str) -> None:
    """Delete isolated rows written for ``wallet`` on PostgreSQL teardown.

    Args:
        repo: Connected live PostgreSQL repository.
        wallet: Per-test random wallet public id.
    """
    async with repo.session() as session:
        await session.execute(
            delete(PortfolioPnlOpeningCheckpoint).where(
                PortfolioPnlOpeningCheckpoint.wallet_public_id == wallet
            )
        )
        await session.execute(
            delete(PortfolioPnlCorrectionFrontier).where(
                PortfolioPnlCorrectionFrontier.wallet_public_id == wallet
            )
        )
        await session.execute(
            delete(PortfolioPnlPoint).where(PortfolioPnlPoint.wallet_public_id == wallet)
        )
        await session.execute(delete(AccrualLedger).where(AccrualLedger.wallet_public_id == wallet))
        await session.commit()


class TestOpeningCheckpointPgContract:
    """Opt-in live PostgreSQL persistence, concurrency, and bigint sum contracts."""

    async def test_pg_upsert_and_read_round_trip(self, pg_repository: SQLAlchemyRepository) -> None:
        """Upsert and read an opening checkpoint with 64-bit-exceeding id sum on PG.

        Given: A fresh PostgreSQL scope and settled chunk write.
        When: ``commit_portfolio_pnl_correction_chunk`` commits with ``checkpoint`` and
            ``get_portfolio_pnl_opening_checkpoint`` reads it back.
        Then: All certificate fields, including ``event_fx_candles_fingerprint.id_sum``
            above ``2**64``, round-trip losslessly.
        """
        scope, anchor, key = _pg_scope()
        m1 = scope.anchor_point_time + timedelta(minutes=1)
        m3 = scope.anchor_point_time + timedelta(minutes=3)
        m5 = scope.anchor_point_time + timedelta(minutes=5)
        try:
            async with pg_repository.session() as session:
                session.add(anchor)
                await session.commit()
            req = PnlCorrectionRequest(
                start=m1,
                tip=m3,
                as_of=m5,
                baseline_watermarks={"kraken": 2},
                pending_watermarks={"kraken": 2},
                prior_peak=None,
                raw_execution_watermarks={"kraken": 2},
                execution_manifest_digest=_PG_DIGEST_A,
                has_fill_gap=False,
                expected_venues=frozenset({"kraken"}),
                venue_event_watermarks={"kraken:spot": 4},
                fill_witness_digest=_PG_DIGEST_B,
            )
            started = await pg_repository.invalidate_portfolio_pnl_correction(scope, req, None)
            chunk = PnlCorrectionChunk(m3, (), None, PnlCorrectionEvidence(key, m1, m3, (), None))
            write = PnlOpeningCheckpointWrite(
                _pg_certificate(advance_correction_frontier(started, chunk)),
                '{"pg":true}',
            )
            await pg_repository.commit_portfolio_pnl_correction_chunk(
                scope, started, chunk, checkpoint=write
            )
            record = await pg_repository.get_portfolio_pnl_opening_checkpoint(scope)
            assert record is not None
            assert record.certificate == write.certificate
            assert record.state_json == '{"pg":true}'
        finally:
            await _cleanup_pg_scope(pg_repository, scope.wallet_public_id)

    async def test_pg_lost_cas_writes_neither_frontier_nor_checkpoint(
        self, pg_repository: SQLAlchemyRepository
    ) -> None:
        """Refuse stale CAS commit on PG without altering frontier or writing checkpoint.

        Given: Generation 1 started on PostgreSQL with revision 1.
        When: A stale claim (``revision=0``) commits with a checkpoint write.
        Then: ``PortfolioPnlCorrectionConflictError`` is raised and neither the frontier
            nor ``portfolio_pnl_opening_checkpoints`` is modified.
        """
        scope, anchor, key = _pg_scope()
        m1 = scope.anchor_point_time + timedelta(minutes=1)
        m3 = scope.anchor_point_time + timedelta(minutes=3)
        m5 = scope.anchor_point_time + timedelta(minutes=5)
        try:
            async with pg_repository.session() as session:
                session.add(anchor)
                await session.commit()
            req = PnlCorrectionRequest(
                start=m1,
                tip=m3,
                as_of=m5,
                baseline_watermarks={"kraken": 2},
                pending_watermarks={"kraken": 2},
                prior_peak=None,
                raw_execution_watermarks={"kraken": 2},
                execution_manifest_digest=_PG_DIGEST_A,
                has_fill_gap=False,
                expected_venues=frozenset({"kraken"}),
                venue_event_watermarks={"kraken:spot": 4},
                fill_witness_digest=_PG_DIGEST_B,
            )
            started = await pg_repository.invalidate_portfolio_pnl_correction(scope, req, None)
            stale = replace(started, revision=0)
            chunk = PnlCorrectionChunk(m3, (), None, PnlCorrectionEvidence(key, m1, m3, (), None))
            write = PnlOpeningCheckpointWrite(
                _pg_certificate(advance_correction_frontier(started, chunk)),
                '{"pg":true}',
            )
            with pytest.raises(PortfolioPnlCorrectionConflictError):
                await pg_repository.commit_portfolio_pnl_correction_chunk(
                    scope, stale, chunk, checkpoint=write
                )
            assert await pg_repository.get_portfolio_pnl_correction_frontier(scope) == started
            assert await pg_repository.get_portfolio_pnl_opening_checkpoint(scope) is None
        finally:
            await _cleanup_pg_scope(pg_repository, scope.wallet_public_id)

    async def test_pg_rewind_deletes_and_forward_generation_keeps_checkpoint(
        self, pg_repository: SQLAlchemyRepository
    ) -> None:
        """Delete stored checkpoint on PG rewind and preserve it on forward generation.

        Given: A persisted checkpoint at ``M3`` on PostgreSQL.
        When: Generation 2 invalidates forward at ``M4`` and generation 3 rewinds to ``M3``.
        Then: Generation 2 retains the checkpoint while generation 3 deletes it.
        """
        scope, anchor, key = _pg_scope()
        m1 = scope.anchor_point_time + timedelta(minutes=1)
        m3 = scope.anchor_point_time + timedelta(minutes=3)
        m4 = scope.anchor_point_time + timedelta(minutes=4)
        m5 = scope.anchor_point_time + timedelta(minutes=5)
        try:
            async with pg_repository.session() as session:
                session.add(anchor)
                await session.commit()
            req_g1 = PnlCorrectionRequest(
                start=m1,
                tip=m3,
                as_of=m5,
                baseline_watermarks={"kraken": 2},
                pending_watermarks={"kraken": 2},
                prior_peak=None,
                raw_execution_watermarks={"kraken": 2},
                execution_manifest_digest=_PG_DIGEST_A,
                has_fill_gap=False,
                expected_venues=frozenset({"kraken"}),
                venue_event_watermarks={"kraken:spot": 4},
                fill_witness_digest=_PG_DIGEST_B,
            )
            g1 = await pg_repository.invalidate_portfolio_pnl_correction(scope, req_g1, None)
            chunk = PnlCorrectionChunk(m3, (), None, PnlCorrectionEvidence(key, m1, m3, (), None))
            write = PnlOpeningCheckpointWrite(
                _pg_certificate(advance_correction_frontier(g1, chunk)),
                '{"pg":1}',
            )
            settled = await pg_repository.commit_portfolio_pnl_correction_chunk(
                scope, g1, chunk, checkpoint=write
            )
            g2 = await pg_repository.invalidate_portfolio_pnl_correction(
                scope, replace(req_g1, start=m4, tip=m5), settled
            )
            assert await pg_repository.get_portfolio_pnl_opening_checkpoint(scope) is not None
            await pg_repository.invalidate_portfolio_pnl_correction(
                scope, replace(req_g1, start=m3, tip=m5), g2
            )
            assert await pg_repository.get_portfolio_pnl_opening_checkpoint(scope) is None
        finally:
            await _cleanup_pg_scope(pg_repository, scope.wallet_public_id)

    async def test_pg_aggregate_reads_match_python_recomputation(
        self, pg_repository: SQLAlchemyRepository
    ) -> None:
        """Verify accrual, event-FX candle, and execution prefix aggregates on PG.

        Given: Active and out-of-window accruals, candles, and executions on PostgreSQL.
        When: All three repository aggregate reads execute at ``as_of``.
        Then: Every aggregate matches independent Python recomputation.
        """
        scope, anchor, _ = _pg_scope()
        t0 = scope.anchor_point_time
        m1 = t0 + timedelta(minutes=1)
        m2 = t0 + timedelta(minutes=2)
        m5 = t0 + timedelta(minutes=5)
        fx_inst = str(uuid4())
        sym_id = str(uuid4())
        fx_exchange = f"pgfx{fx_inst[:8]}"
        fx_plane: PnlFxRatePlane = ("EUR", "USD", fx_exchange)
        try:
            fx_symbol = _fx_symbol(sym_id, f"EUR-USD-{fx_inst[:8]}", "EUR", "USD", timestamp=t0)
            fx_instrument = _fx_instrument(fx_inst, sym_id, fx_exchange, timestamp=t0)
            accrual = AccrualLedger(
                instrument_public_id=fx_inst,
                wallet_public_id=scope.wallet_public_id,
                operator_public_id=None,
                mode="live",
                accrual_type="funding",
                accrued_at=m2,
                amount=2.5,
                amount_asset="USD",
                rate=0.0001,
                notional=1000.0,
                position_quantity_at_accrual=1.0,
                exchange="kraken",
                timestamp=m1,
                known_to=KNOWN_TO_MAX,
                session_id=_PG_SESSION,
                sequence_id=1,
            )
            candle = _fx_candle(fx_inst, m2, 1.2)
            candle.timestamp = m1
            candle.session_id = _PG_SESSION
            execution = Execution(
                public_id=str(uuid4()),
                order_public_id=str(uuid4()),
                wallet_public_id=scope.wallet_public_id,
                operator_public_id=None,
                exchange="kraken",
                mode="live",
                scope_sequence=1,
                exec_id=f"pg-exec-{scope.wallet_public_id[:8]}",
                trade_id=None,
                side="buy",
                status="filled",
                price=10.0,
                size=1.0,
                fee=0.0,
                fee_asset="USD",
                liquidity_role="taker",
                session_id=_PG_SESSION,
                sequence_id=1,
                timestamp=m1,
                executed_at=m1,
                known_to=KNOWN_TO_MAX,
            )
            async with pg_repository.session() as session:
                session.add_all([anchor, accrual, fx_symbol, fx_instrument, candle, execution])
                await session.commit()
            accrual_fp = await pg_repository.get_pnl_accrual_prefix_fingerprint(
                scope.wallet_public_id, "live", t0, m2, m5
            )
            assert accrual_fp == IdAggregate(1, accrual.id, accrual.id, m1)
            candle_fp = await pg_repository.get_pnl_event_fx_candle_fingerprint(
                [fx_plane], [m2], m5
            )
            replay_rows = await pg_repository.get_pnl_fx_rate_candles_at([fx_plane], [m2], m5)
            assert len(replay_rows) == 1
            assert candle_fp == IdAggregate(
                1,
                replay_rows[0]["candle_id"],
                replay_rows[0]["candle_id"],
                replay_rows[0]["candle_timestamp"],
            )
            assert candle_fp == IdAggregate(1, candle.id, candle.id, m1)
            exec_counts = await pg_repository.get_pnl_execution_prefix_counts(
                scope.wallet_public_id, "live", {"kraken": 2}
            )
            assert exec_counts == {"kraken": (1, 1)}
        finally:
            async with pg_repository.session() as session:
                await session.execute(delete(Candle).where(Candle.instrument_public_id == fx_inst))
                await session.execute(delete(Instrument).where(Instrument.public_id == fx_inst))
                await session.execute(delete(Symbol).where(Symbol.public_id == sym_id))
                await session.execute(
                    delete(Execution).where(Execution.wallet_public_id == scope.wallet_public_id)
                )
                await session.commit()
            await _cleanup_pg_scope(pg_repository, scope.wallet_public_id)

    async def test_pg_event_fx_id_sum_above_2_pow_63_stays_exact(
        self, pg_repository: SQLAlchemyRepository
    ) -> None:
        """Keep ``SUM(bigint)`` exact above ``2**63`` using PostgreSQL numeric aggregation.

        Given: Three 1m candles on a proven FX plane with explicit large ``BIGINT`` ids
            summing past ``2**63``.
        When: ``get_pnl_event_fx_candle_fingerprint`` executes on PostgreSQL.
        Then: ``id_sum`` equals the exact integer sum without 64-bit signed overflow and
            matches the replay read.
        """
        now = datetime.now(UTC).replace(second=0, microsecond=0)
        fx_inst = str(uuid4())
        sym_id = str(uuid4())
        fx_exchange = f"pgfx{fx_inst[:8]}"
        fx_plane: PnlFxRatePlane = ("EUR", "USD", fx_exchange)
        opens = [now - timedelta(minutes=5 + offset) for offset in range(3)]
        t_meta = min(opens) - timedelta(minutes=1)
        large_ids = [(1 << 62) + 1, (1 << 62) + 2, (1 << 62) + 3]
        fx_symbol = _fx_symbol(sym_id, f"EUR-USD-{fx_inst[:8]}", "EUR", "USD", timestamp=t_meta)
        fx_instrument = _fx_instrument(fx_inst, sym_id, fx_exchange, timestamp=t_meta)
        candles: list[Candle] = []
        for idx, (candle_id, open_at) in enumerate(zip(large_ids, opens, strict=True), start=1):
            row = _fx_candle(fx_inst, open_at, 1.2)
            row.id = candle_id
            row.session_id = _PG_SESSION
            row.sequence_id = idx
            candles.append(row)
        try:
            async with pg_repository.session() as session:
                session.add_all([fx_symbol, fx_instrument, *candles])
                await session.commit()
            agg = await pg_repository.get_pnl_event_fx_candle_fingerprint([fx_plane], opens, now)
            replay_rows = await pg_repository.get_pnl_fx_rate_candles_at([fx_plane], opens, now)
            assert len(replay_rows) == 3
            assert agg == IdAggregate(
                len(replay_rows),
                sum(row["candle_id"] for row in replay_rows),
                max(row["candle_id"] for row in replay_rows),
                max(row["candle_timestamp"] for row in replay_rows),
            )
            assert agg == IdAggregate(3, sum(large_ids), max(large_ids), max(opens))
            assert agg.id_sum > (1 << 63)
        finally:
            async with pg_repository.session() as session:
                await session.execute(delete(Candle).where(Candle.instrument_public_id == fx_inst))
                await session.execute(delete(Instrument).where(Instrument.public_id == fx_inst))
                await session.execute(delete(Symbol).where(Symbol.public_id == sym_id))
                await session.commit()
