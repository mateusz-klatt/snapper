"""Opt-in live-PostgreSQL witness for checkpoint shadow-failure isolation.

The witness migrates a private database, captures a real opening checkpoint, and
injects a server-side statement timeout into only the resumed shadow read. It
proves that the cold anchor plan still commits, then restores the read and proves
that a later recovery tick resumes and commits normally.

Every test SKIPS unless ``PNL_TEST_POSTGRES_URL`` names a PostgreSQL server with
permission to create and drop private databases. Use only a disposable server,
never production.
"""

import asyncio
import os
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import NoReturn
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from asyncpg.exceptions import QueryCanceledError
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError

from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import _CheckpointDispatchRequest
from snapper.application.portfolio.pnl_snapshotter import _PlanBasis
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlTimelineExecutionSuffixRequest
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_e2e as e2e
from tests.application.portfolio import test_pnl_snapshotter_lower_id_rewind as idless
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)


def _configured_backend_name() -> str:
    """Return the opt-in PostgreSQL backend, mapping unset or invalid URLs to empty."""
    url = os.environ.get("PNL_TEST_POSTGRES_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason=(
        "live-PostgreSQL witness module: opt in by setting "
        "PNL_TEST_POSTGRES_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


async def _create_database(base_url: str, database_name: str) -> None:
    """Create one private database through the probe e2e's autocommit pattern."""
    maintenance = SQLAlchemyRepository(base_url)
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
    finally:
        await maintenance.engine.dispose()


async def _drop_database(base_url: str, database_name: str) -> None:
    """Drop the private database even when the proof fails."""
    maintenance = SQLAlchemyRepository(base_url)
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
    finally:
        await maintenance.engine.dispose()


async def _clear_migration_catalog(url: str) -> None:
    """Remove migration-owned reference rows before using the probe's fixed catalog fixture."""
    async with idless._repository(url) as repo, repo.session() as session:
        for table in ("symbol_exchange_capabilities", "symbol_aliases", "symbols"):
            await session.execute(text(f"DELETE FROM {table}"))
        await session.commit()


async def _committed_minutes(
    url: str, scope: PortfolioPnlSampleScope, first: int, last: int
) -> list[datetime]:
    """Read active sample minutes through a fresh repository and explicit fresh session."""
    async with idless._repository(url) as observer, observer.session() as session:
        result = await session.execute(
            select(PortfolioPnlPoint.point_time)
            .where(
                PortfolioPnlPoint.wallet_public_id == scope.wallet_public_id,
                PortfolioPnlPoint.mode == scope.mode,
                PortfolioPnlPoint.valuation_ccy == scope.valuation_ccy,
                PortfolioPnlPoint.epoch_public_id == scope.epoch_public_id,
                PortfolioPnlPoint.calc_version == PNL_SAMPLE_CALC_VERSION,
                PortfolioPnlPoint.point_kind == "sample",
                PortfolioPnlPoint.known_to == KNOWN_TO_MAX,
                PortfolioPnlPoint.point_time >= samples._T0 + first * _MINUTE,
                PortfolioPnlPoint.point_time <= samples._T0 + last * _MINUTE,
            )
            .order_by(PortfolioPnlPoint.point_time)
        )
        return list(result.scalars().all())


async def _run_failing_shadow_tick(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> list[DBAPIError]:
    """Fail one genuine suffix read only while the shadow branch is active."""
    captured: list[DBAPIError] = []
    failing_tick = PortfolioPnlSnapshotter(
        repo=repo,
        disabled=False,
        clock=lambda: samples._T0 + 7 * _MINUTE,
    )
    original_shadow = failing_tick._run_shadow_branch
    shadow_active = False
    shadow_entries = 0

    async def cancel_suffix_read(request: PnlTimelineExecutionSuffixRequest) -> NoReturn:
        """Cancel a genuine PostgreSQL statement only inside the shadow branch."""
        del request
        assert shadow_active
        async with repo.session() as session:
            await session.execute(text("SET LOCAL statement_timeout = 1"))
            try:
                await session.execute(text("SELECT pg_sleep(0.1)"))
            except DBAPIError as exc:
                captured.append(exc)
                raise
        raise AssertionError("pg_sleep unexpectedly completed")

    async def guarded_shadow(
        request: _CheckpointDispatchRequest,
        basis: _PlanBasis,
        anchor_pair: tuple[ChunkWindow, PnlWalletSeriesResult, PlannedPnlCorrection],
    ) -> None:
        """Arm the fault only after the authoritative commit has entered shadow dispatch."""
        nonlocal shadow_active, shadow_entries
        shadow_active = True
        shadow_entries += 1
        try:
            await original_shadow(request, basis, anchor_pair)
        finally:
            shadow_active = False

    with monkeypatch.context() as patch:
        patch.setattr(repo, "get_pnl_timeline_execution_suffix_at_watermarks", cancel_suffix_read)
        patch.setattr(failing_tick, "_run_shadow_branch", guarded_shadow)
        await failing_tick._tick_once()
    assert shadow_entries == 1
    return captured


async def _assert_failed_anchor_commit(
    url: str, scope: PortfolioPnlSampleScope, captured: list[DBAPIError]
) -> str:
    """Prove the 57014 was isolated and the anchor transaction became durable."""
    assert len(captured) == 1
    dbapi_error = captured[0].orig
    driver_error = getattr(dbapi_error, "orig", None)
    assert isinstance(driver_error, QueryCanceledError)
    assert driver_error.sqlstate == "57014"
    assert getattr(dbapi_error, "sqlstate", None) == "57014"
    reason = f"shadow_error:{type(captured[0]).__name__}"
    assert checkpoint_metrics().shadow_skipped == {reason: 1}
    assert checkpoint_metrics().resumes_attempted == 1
    assert await _committed_minutes(url, scope, 2, 5) == [
        samples._T0 + offset * _MINUTE for offset in range(2, 6)
    ]
    async with idless._repository(url) as observer:
        frontier = await observer.get_portfolio_pnl_correction_frontier(scope)
        assert frontier is not None
        assert frontier.tip == samples._T0 + 5 * _MINUTE
        assert frontier.next_minute is None
        assert await observer.get_portfolio_pnl_opening_checkpoint(scope) is not None
    return reason


async def _exercise_shadow_cancellation(url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Capture, fail a real resumed read, verify its anchor commit, then recover."""
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    await _clear_migration_catalog(url)
    scope = await idless._seed(url, complete=False)
    async with idless._repository(url) as repo:
        async with repo.session() as session:
            version = await session.scalar(text("SHOW server_version_num"))
        assert isinstance(version, str) and version.startswith("18")
        first_tick = PortfolioPnlSnapshotter(
            repo=repo,
            disabled=False,
            clock=lambda: samples._T0 + 3 * _MINUTE,
        )
        await first_tick._tick_once()
        checkpoint = await repo.get_portfolio_pnl_opening_checkpoint(scope)
        assert checkpoint is not None
        assert checkpoint.certificate.checkpoint_minute == samples._T0 + _MINUTE
        assert checkpoint_metrics().captures_committed == 1
        captured = await _run_failing_shadow_tick(repo, monkeypatch)
        reason = await _assert_failed_anchor_commit(url, scope, captured)
        await e2e._add_valuation_minutes(repo, 6, 6)
        recovery_tick = PortfolioPnlSnapshotter(
            repo=repo,
            disabled=False,
            clock=lambda: samples._T0 + 8 * _MINUTE,
        )
        await recovery_tick._tick_once()
        assert await _committed_minutes(url, scope, 1, 6) == [
            samples._T0 + offset * _MINUTE for offset in range(1, 7)
        ]
        metrics = checkpoint_metrics()
        assert metrics.resumes_attempted == 2
        assert metrics.resumes_succeeded == 1
        assert metrics.shadow_comparisons == 1
        assert metrics.shadow_mismatches == 0
        assert metrics.shadow_skipped == {reason: 1}


@pytest.mark.timeout(600)
def test_pg18_shadow_statement_timeout_preserves_anchor_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove a server-side 57014 in shadow resume cannot abort authoritative writes.

    Given: A migrated PG18 wallet with a real checkpoint and a pending suffix chunk.
    When: PostgreSQL cancels only the resumed shadow read with statement timeout 57014.
    Then: The anchor plan commits, the failure is counted, and a recovery tick succeeds.

    Args:
        monkeypatch: Environment and shadow-only repository fault-injection seam.
    """
    configured = os.environ["PNL_TEST_POSTGRES_URL"]
    parsed = make_url(configured)
    assert parsed.get_backend_name() == "postgresql"
    base_url = parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    database_name = f"pnl_shadow_cancel_{uuid4().hex}"
    target_url = parsed.set(
        drivername="postgresql+asyncpg", database=database_name
    ).render_as_string(hide_password=False)
    asyncio.run(_create_database(base_url, database_name))
    try:
        root = Path(__file__).resolve().parents[3]
        config = Config(str(root / "alembic.ini"))
        config.set_main_option("sqlalchemy.url", target_url.replace("%", "%%"))
        command.upgrade(config, "head")
        asyncio.run(_exercise_shadow_cancellation(target_url, monkeypatch))
    finally:
        asyncio.run(_drop_database(base_url, database_name))
