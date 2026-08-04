"""Migration tests for the venue-account observation attempt-stream index."""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_INDEX = "ix_venue_account_observations_attempt_stream"
_TABLE = "venue_account_observations"
_COLUMNS = ("wallet_public_id", "exchange", "mode", "timestamp", "id")


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config for one isolated SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _attempt_stream_index(engine: sa.Engine) -> tuple[tuple[str, ...], bool] | None:
    """Return the exact migrated index shape, or ``None`` when absent."""
    indexes = sa.inspect(engine).get_indexes(_TABLE)
    for index in indexes:
        if index["name"] == _INDEX:
            return tuple(index["column_names"]), bool(index["unique"])
    return None


@pytest.fixture
def migrated_attempt_stream_db(
    migrated_db_path: Path,
) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a private database migrated through the current head."""
    db_url = f"sqlite:///{migrated_db_path}"
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, _make_alembic_config(db_url)
    finally:
        engine.dispose()


def test_0050_creates_exact_attempt_stream_index(
    migrated_attempt_stream_db: tuple[sa.Engine, Config],
) -> None:
    """The head schema carries the exact query-prefix index once."""
    engine, _ = migrated_attempt_stream_db
    assert _attempt_stream_index(engine) == (_COLUMNS, False)


def test_0050_downgrade_and_upgrade_round_trip_the_index(
    migrated_attempt_stream_db: tuple[sa.Engine, Config],
) -> None:
    """Downgrade removes only the new index and upgrade restores its shape."""
    engine, config = migrated_attempt_stream_db
    command.downgrade(config, "0049")
    assert _attempt_stream_index(engine) is None
    command.upgrade(config, "0050")
    assert _attempt_stream_index(engine) == (_COLUMNS, False)
