"""Driver-boundary coverage for scoped PostgreSQL correction budgets and locks.

The companion disposable PG18 witness executes these limits on a real server,
including shorter-setting preservation and rollback. These tests verify that
each repository entry point actually requests the guarded driver path.
"""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.repository import SQLAlchemyRepository
from tests.data import test_pnl_timeline_sample_repository as samples


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
