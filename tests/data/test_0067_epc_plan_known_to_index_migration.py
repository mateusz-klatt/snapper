"""Migration tests for the execution plan checkpoint open-version index."""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.repository import SQLAlchemyRepository

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_INDEX = "ix_epc_plan_known_to"
_TABLE = "execution_plan_checkpoints"
_COLUMNS = ("plan_public_id", "known_to")
_FIRST_CHECKPOINT_AT = datetime(2026, 11, 9, 20, 0, 0, tzinfo=UTC)


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config for one isolated SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _open_version_index(engine: sa.Engine) -> tuple[tuple[str, ...], bool] | None:
    """Return the exact migrated index shape, or ``None`` when absent."""
    indexes = sa.inspect(engine).get_indexes(_TABLE)
    for index in indexes:
        if index["name"] == _INDEX:
            return tuple(index["column_names"]), bool(index["unique"])
    return None


@pytest.fixture
def migrated_checkpoint_db(
    migrated_db_path: Path,
) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a private database migrated through the current head."""
    db_url = f"sqlite:///{migrated_db_path}"
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, _make_alembic_config(db_url)
    finally:
        engine.dispose()


def test_0067_creates_exact_open_version_index(
    migrated_checkpoint_db: tuple[sa.Engine, Config],
) -> None:
    """The head schema carries the plan and knowledge-end index once, non-unique."""
    engine, _ = migrated_checkpoint_db
    assert _open_version_index(engine) == (_COLUMNS, False)


def test_0067_model_declares_the_migrated_index() -> None:
    """The ORM table declares the same index, so both schema sources agree."""
    declared = {
        index.name: tuple(column.name for column in index.columns)
        for index in ExecutionPlanCheckpoint.__table__.indexes
    }
    assert declared[_INDEX] == _COLUMNS


def test_0067_downgrade_and_upgrade_round_trip_the_index(
    migrated_checkpoint_db: tuple[sa.Engine, Config],
) -> None:
    """Downgrade removes only the new index and upgrade restores its shape."""
    engine, config = migrated_checkpoint_db
    command.downgrade(config, "0066")
    assert _open_version_index(engine) is None
    assert "ix_epc_plan_public_id" in {
        index["name"] for index in sa.inspect(engine).get_indexes(_TABLE)
    }
    command.upgrade(config, "0067")
    assert _open_version_index(engine) == (_COLUMNS, False)


async def test_0067_readers_keep_one_open_version_per_plan(migrated_db_path: Path) -> None:
    """Readers return the open version after several closed ones on the migrated schema.

    Given: Two plans on the head-migrated schema, one with three checkpoint
        versions written in order and one with a single version.
    When: The single and bulk latest-checkpoint readers run.
    Then: Each plan yields its newest state, and exactly one stored version per
        plan still carries the open knowledge end.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{migrated_db_path}")
    busy_plan = str(uuid4())
    quiet_plan = str(uuid4())
    try:
        for version in (1, 2, 3):
            written_at = _FIRST_CHECKPOINT_AT + timedelta(seconds=version)
            await repository.insert_execution_plan_checkpoint(
                plan_public_id=busy_plan,
                state={"version": version},
                last_venue_event_id=version,
                checkpoint_at=written_at,
                session_id="epc-index",
                sequence_id=version,
                bus_time=written_at,
            )
        await repository.insert_execution_plan_checkpoint(
            plan_public_id=quiet_plan,
            state={"version": 10},
            last_venue_event_id=10,
            checkpoint_at=_FIRST_CHECKPOINT_AT,
            session_id="epc-index",
            sequence_id=10,
            bus_time=_FIRST_CHECKPOINT_AT,
        )

        latest = await repository.get_latest_plan_checkpoint(busy_plan)
        by_plan = await repository.get_latest_checkpoints_for_plans([busy_plan, quiet_plan])
        async with repository.session() as session:
            rows = (
                await session.execute(
                    sa.select(
                        ExecutionPlanCheckpoint.plan_public_id,
                        ExecutionPlanCheckpoint.known_to,
                    )
                )
            ).all()
    finally:
        await repository.engine.dispose()

    assert latest is not None
    assert latest["state"] == {"version": 3}
    assert {plan: row["state"] for plan, row in by_plan.items()} == {
        busy_plan: {"version": 3},
        quiet_plan: {"version": 10},
    }
    open_versions = [plan for plan, known_to in rows if known_to == KNOWN_TO_MAX]
    assert len(rows) == 4
    assert sorted(open_versions) == sorted([busy_plan, quiet_plan])
