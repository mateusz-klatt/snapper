"""Schema coverage for the paired group history lookup index."""

from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import PairedExecutionGroup

_TABLE = "paired_execution_groups"
_HISTORY_INDEX = "ix_peg_public_id_history"
_CURRENT_INDEX = "ix_peg_public_id"


def _assert_paired_group_indexes(engine: sa.Engine) -> None:
    """Check that history lookups and current-row uniqueness have separate indexes."""
    indexes = {index["name"]: index for index in sa.inspect(engine).get_indexes(_TABLE)}
    history = indexes[_HISTORY_INDEX]
    assert history["column_names"] == ["public_id"]
    assert not history["unique"]
    assert not history.get("dialect_options")
    current = indexes[_CURRENT_INDEX]
    assert current["unique"]
    assert "sqlite_where" in current["dialect_options"]


def test_0075_history_index_matches_model_and_survives_round_trip(
    migrated_db_path: Path,
) -> None:
    """Verify the unfiltered lookup index and the current-row constraint.

    Given: An isolated SQLite database at Alembic head and the ORM table definition.
    When: The paired group history index is reflected, downgraded and upgraded.
    Then: The ORM and migration declare the same full index and the original partial
        unique index survives the migration round trip.
    """
    db_url = f"sqlite:///{migrated_db_path}"
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", db_url)
    engine = sa.create_engine(db_url)
    try:
        _assert_paired_group_indexes(engine)
        declared = {index.name: index for index in PairedExecutionGroup.__table__.indexes}
        history = declared[_HISTORY_INDEX]
        assert list(history.columns.keys()) == ["public_id"]
        assert not history.unique
        assert history.dialect_options["sqlite"]["where"] is None
        assert history.dialect_options["postgresql"]["where"] is None
        command.downgrade(config, "0074")
        names = {index["name"] for index in sa.inspect(engine).get_indexes(_TABLE)}
        assert _HISTORY_INDEX not in names
        assert _CURRENT_INDEX in names
        command.upgrade(config, "0075")
        _assert_paired_group_indexes(engine)
    finally:
        engine.dispose()
