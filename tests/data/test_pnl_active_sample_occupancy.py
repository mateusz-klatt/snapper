"""Verify SQLAlchemyRepository.has_active_portfolio_pnl_samples occupancy checks."""

import os
from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.data import test_pnl_timeline_sample_repository as samples


def _sample_point(
    repository: SQLAlchemyRepository,
    row: PortfolioPnlSampleRow,
    *,
    known_to: datetime = KNOWN_TO_MAX,
) -> PortfolioPnlPoint:
    """Build one PortfolioPnlPoint ORM row from a sample payload.

    Args:
        repository: Repository owning the canonical ORM mapping helper.
        row: Canonical sample dictionary.
        known_to: SCD2 visibility upper bound.

    Returns:
        An ORM instance ready for persistence.
    """
    return PortfolioPnlPoint(**repository._portfolio_pnl_sample_orm_kwargs(row), known_to=known_to)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provision an isolated SQLite sample table with an active activation anchor.

    Args:
        tmp_path: Temporary pytest directory.

    Yields:
        A repository bound to the disposable SQLite database.
    """
    db_path = tmp_path / "occupancy.db"
    engine = create_engine(f"sqlite:///{db_path}")
    PortfolioPnlPoint.__table__.create(engine)
    engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as session:
        session.add(samples._anchor_orm())
        await session.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.fixture(params=("sqlite", "postgresql"))
async def backend_repository(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide SQLite and opt-in PostgreSQL repositories for occupancy checks.

    Args:
        request: Parametrized backend name.
        tmp_path: Temporary directory for SQLite storage.

    Yields:
        A schema-initialized repository with one activation anchor.
    """
    if request.param == "sqlite":
        db_path = tmp_path / "occupancy-backend.db"
        engine = create_engine(f"sqlite:///{db_path}")
        PortfolioPnlPoint.__table__.create(engine)
        engine.dispose()
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
        async with repo.session() as session:
            session.add(samples._anchor_orm())
            await session.commit()
        try:
            yield repo
        finally:
            await repo.engine.dispose()
        return

    configured = os.environ.get("PNL_TEST_POSTGRES_URL")
    if configured is None:
        pytest.skip("requires isolated PNL_TEST_POSTGRES_URL")
    base = make_url(configured)
    assert base.get_backend_name() == "postgresql"
    database_name = f"pnl_occupancy_{uuid4().hex}"
    maintenance = SQLAlchemyRepository(configured)
    created = False
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created = True
        target_url = base.set(database=database_name).render_as_string(hide_password=False)
        repo = SQLAlchemyRepository(target_url)
        try:
            async with repo.engine.begin() as connection:
                await connection.run_sync(PortfolioPnlPoint.__table__.create)
            async with repo.session() as session:
                session.add(samples._anchor_orm())
                await session.commit()
            yield repo
        finally:
            await repo.engine.dispose()
    finally:
        if created:
            async with maintenance.engine.connect() as connection:
                admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        await maintenance.engine.dispose()


async def test_has_active_portfolio_pnl_samples_ignores_closed_anchor_and_out_of_range(
    repository: SQLAlchemyRepository,
) -> None:
    """Ignore closed history, anchor rows, out-of-range samples, and empty ranges.

    Given: An active anchor row at T0, an SCD2-closed sample inside [M2 .. M4],
        and active samples strictly before M2 and strictly after M4.
    When: has_active_portfolio_pnl_samples checks [M2 .. M4], [T0 .. M1-1s],
        and an inverted window [M4 .. M2].
    Then: All three queries return False without treating closed, anchor, or
        adjacent-minute rows as in-window occupancy.
    """
    closed_row = samples._complete_sample(samples._M3)
    before_row = samples._complete_sample(samples._M1)
    after_row = samples._complete_sample(samples._M5)
    async with repository.session() as session:
        session.add_all(
            [
                _sample_point(repository, closed_row, known_to=samples._M4),
                _sample_point(repository, before_row),
                _sample_point(repository, after_row),
            ]
        )
        await session.commit()

    scope = samples._scope()
    assert not await repository.has_active_portfolio_pnl_samples(scope, samples._M2, samples._M4)
    assert not await repository.has_active_portfolio_pnl_samples(
        scope, samples._T0, samples._M1 - timedelta(seconds=1)
    )
    assert not await repository.has_active_portfolio_pnl_samples(scope, samples._M4, samples._M2)


async def test_has_active_portfolio_pnl_samples_ignores_other_wallet_mode_and_currency(
    repository: SQLAlchemyRepository,
) -> None:
    """Restrict occupancy checks strictly to the scope's wallet, mode, and currency.

    Given: Active samples in [M1 .. M5] belonging to another wallet, paper mode,
        or EUR valuation currency.
    When: has_active_portfolio_pnl_samples queries the live USD target scope.
    Then: It returns False because none of those rows occupy the target uniqueness key.
    """
    other_wallet = samples._mutate(
        samples._complete_sample(samples._M1), wallet_public_id=samples._OTHER_WALLET
    )
    other_mode = samples._mutate(samples._complete_sample(samples._M2), mode="paper")
    other_currency = samples._mutate(samples._complete_sample(samples._M3), valuation_ccy="EUR")
    async with repository.session() as session:
        session.add_all(
            [
                _sample_point(repository, other_wallet),
                _sample_point(repository, other_mode),
                _sample_point(repository, other_currency),
            ]
        )
        await session.commit()

    assert not await repository.has_active_portfolio_pnl_samples(
        samples._scope(), samples._M1, samples._M5
    )


@pytest.mark.parametrize(
    "variant",
    ["same_epoch_complete", "same_epoch_incomplete", "foreign_epoch", "legacy_calc_version"],
)
async def test_has_active_portfolio_pnl_samples_detects_active_variants_across_epochs_and_versions(
    repository: SQLAlchemyRepository,
    variant: str,
) -> None:
    """Detect active samples across epochs, calculation versions, and statuses.

    Given: One active sample row at M3 representing current complete, current
        incomplete, foreign-epoch, or legacy calc_version evidence.
    When: has_active_portfolio_pnl_samples checks windows ending before M3 or
        including M3 as an exact endpoint.
    Then: Windows excluding M3 return False and windows covering M3 return True.
    """
    if variant == "same_epoch_complete":
        row = samples._complete_sample(samples._M3)
    elif variant == "same_epoch_incomplete":
        row = samples._incomplete_sample(samples._M3)
    elif variant == "foreign_epoch":
        row = samples._mutate(
            samples._complete_sample(samples._M3), epoch_public_id=samples._OTHER_EPOCH
        )
    else:
        row = samples._mutate(samples._complete_sample(samples._M3), calc_version="5C.3")

    async with repository.session() as session:
        session.add(_sample_point(repository, row))
        await session.commit()

    scope = samples._scope()
    assert not await repository.has_active_portfolio_pnl_samples(scope, samples._M1, samples._M2)
    assert not await repository.has_active_portfolio_pnl_samples(scope, samples._M4, samples._M5)
    assert await repository.has_active_portfolio_pnl_samples(scope, samples._M1, samples._M3)
    assert await repository.has_active_portfolio_pnl_samples(scope, samples._M3, samples._M5)
    assert await repository.has_active_portfolio_pnl_samples(scope, samples._M3, samples._M3)


async def test_has_active_portfolio_pnl_samples_normalizes_scope_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Canonicalize wallet casing and valuation currency before matching samples.

    Given: An active sample persisted under canonical lowercase wallet UUID and USD.
    When: A caller queries occupancy using uppercase hex wallet UUID and lowercase usd.
    Then: The EXISTS probe normalizes the scope and returns True.
    """
    await repository.record_portfolio_pnl_samples(
        [samples._complete_sample(samples._M2)], samples._scope()
    )
    unnormalized_scope = PortfolioPnlSampleScope(
        wallet_public_id=samples._WALLET.upper(),
        mode="live",
        valuation_ccy="usd",
        epoch_public_id=samples._EPOCH,
        anchor_point_time=samples._T0,
    )
    assert await repository.has_active_portfolio_pnl_samples(
        unnormalized_scope, samples._M1, samples._M5
    )


async def test_has_active_portfolio_pnl_samples_reuses_scope_contract_on_backend(
    backend_repository: SQLAlchemyRepository,
) -> None:
    """Execute the EXISTS occupancy contract identically on SQLite and opt-in PostgreSQL.

    Given: An anchored scope on SQLite or isolated PostgreSQL initially empty, then
        populated with a foreign-epoch active row and later closed via SCD2 update.
    When: has_active_portfolio_pnl_samples runs before insert, while active, and after close.
    Then: It returns False while empty, True while the foreign row is active, and False once closed.
    """
    scope = samples._scope()
    assert not await backend_repository.has_active_portfolio_pnl_samples(
        scope, samples._M1, samples._M5
    )

    foreign = samples._mutate(
        samples._complete_sample(samples._M3),
        epoch_public_id=samples._OTHER_EPOCH,
        calc_version="5B.1",
    )
    orm_row = _sample_point(backend_repository, foreign)
    async with backend_repository.session() as session:
        session.add(orm_row)
        await session.commit()
    assert await backend_repository.has_active_portfolio_pnl_samples(
        scope, samples._M1, samples._M5
    )

    async with backend_repository.session() as session:
        stored = await session.get(PortfolioPnlPoint, orm_row.id)
        assert stored is not None
        stored.known_to = samples._M4
        await session.commit()
    assert not await backend_repository.has_active_portfolio_pnl_samples(
        scope, samples._M1, samples._M5
    )
