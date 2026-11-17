"""The continuation migration adds control only and preserves financial evidence."""

from importlib import import_module
from pathlib import Path

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.data.models import PortfolioPnlPoint
from tests.data import test_pnl_timeline_sample_repository as samples


def test_0069_round_trip_preserves_every_preexisting_financial_column(tmp_path: Path) -> None:
    """Upgrade/downgrade alter only the new mutable continuation control table.

    Args:
        tmp_path: Owned disposable database directory.
    """
    engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    migration = import_module("snapper.data.migrations.versions.0069_pnl_correction_frontiers")
    PortfolioPnlPoint.__table__.create(engine)
    with Session(engine) as session:
        session.add(samples._anchor_orm())
        session.commit()
    with engine.begin() as connection:
        before = connection.execute(select(PortfolioPnlPoint.__table__)).all()
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
        names = inspect(connection).get_pk_constraint("portfolio_pnl_correction_frontiers")
        assert set(names["constrained_columns"]) == {
            "wallet_public_id",
            "mode",
            "valuation_ccy",
            "epoch_public_id",
            "calc_version",
        }
        assert connection.execute(select(PortfolioPnlPoint.__table__)).all() == before
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()
        assert not inspect(connection).has_table("portfolio_pnl_correction_frontiers")
        assert connection.execute(select(PortfolioPnlPoint.__table__)).all() == before
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
        assert inspect(connection).has_table("portfolio_pnl_correction_frontiers")
        assert connection.execute(select(PortfolioPnlPoint.__table__)).all() == before
    engine.dispose()
