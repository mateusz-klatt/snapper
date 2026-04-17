"""Tests for migration 0009_trailing_stop_unique_index.

Verifies the partial unique index ``uq_ep_active_trailing_stop_per_cycle``
rejects a second non-terminal ``trailing_stop`` plan on the same
``position_cycle_public_id`` (mirroring the existing
``uq_ep_active_bracket_per_cycle`` behaviour from migration 0001),
is removed by the downgrade, and reappears after an upgrade round-trip.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
KNOWN_TO_MAX_LITERAL = "9999-12-31 23:59:59.000000"
_CYCLE_ID = str(uuid7())


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_execution_plan(
    engine: sa.Engine,
    *,
    plan_type: str,
    status: str,
    position_cycle_public_id: str | None,
    known_to: str = KNOWN_TO_MAX_LITERAL,
) -> str:
    """Insert one execution_plans row with the supplied lifecycle state.

    Uses literal SQL to side-step ORM invariants so the test exercises
    the partial-index predicate directly.
    """
    public_id = str(uuid7())
    now = datetime.now(UTC).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO execution_plans (public_id, plan_type, created_via, "
                "instrument_public_id, exchange, mode, shard_key, wallet_public_id, "
                "total_quantity, filled_quantity, side, position_cycle_public_id, "
                "params, status, created_at, session_id, sequence_id, timestamp, known_to) "
                "VALUES (:public_id, :plan_type, 'api', :inst, 'kraken', 'live', "
                ":shard, :wallet, 1.0, 0.0, 'buy', :cycle, '{}', :status, :ts, "
                ":session, 1, :ts, :known_to)"
            ),
            {
                "public_id": public_id,
                "plan_type": plan_type,
                "inst": str(uuid7()),
                "shard": "kraken.BTC-USD.live",
                "wallet": str(uuid7()),
                "cycle": position_cycle_public_id,
                "status": status,
                "ts": now,
                "session": str(uuid7()),
                "known_to": known_to,
            },
        )
    return public_id


def _index_exists(engine: sa.Engine) -> bool:
    """Return True iff the trailing-stop partial index is present on SQLite."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='uq_ep_active_trailing_stop_per_cycle'"
            )
        ).all()
    return len(rows) == 1


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "trailing_stop_index.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestTrailingStopUniqueIndexMigration:
    """Upgrade / downgrade / round-trip behaviours for the partial index."""

    def test_upgrade_creates_partial_index(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Given a freshly-migrated DB, the partial index is present.

        Given: `alembic upgrade head` on a fresh SQLite DB,
        When: querying sqlite_master for the index name,
        Then: exactly one row is returned for
            ``uq_ep_active_trailing_stop_per_cycle``.
        """
        engine, _ = migrated_db
        assert _index_exists(engine)

    def test_second_active_trailing_stop_on_same_cycle_is_rejected(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Given an active trailing_stop on cycle C, a second one on C is rejected.

        Given: a ``trailing_stop`` plan with ``status='armed'`` and
            ``known_to=KNOWN_TO_MAX`` on cycle C is inserted,
        When: a second ``trailing_stop`` row for the same
            ``position_cycle_public_id`` is attempted with the same
            terminal-excluded status + known_to,
        Then: the second INSERT raises ``IntegrityError`` because the
            partial unique index enforces at most one active
            trailing-stop per cycle.
        """
        engine, _ = migrated_db
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )
        with pytest.raises(sa.exc.IntegrityError):
            _insert_execution_plan(
                engine,
                plan_type="trailing_stop",
                status="armed",
                position_cycle_public_id=_CYCLE_ID,
            )

    def test_terminal_trailing_stop_does_not_block_new_one(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Given a terminal trailing_stop on cycle C, a new active one on C is allowed.

        Given: a ``trailing_stop`` with ``status='completed'`` on cycle C
            (outside the NOT IN (completed/cancelled/failed/expired)
            predicate),
        When: a new ``status='armed'`` trailing_stop on the same cycle
            is inserted,
        Then: the insert succeeds — completed rows do not occupy the
            partial-index slot.
        """
        engine, _ = migrated_db
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="completed",
            position_cycle_public_id=_CYCLE_ID,
        )
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )

    def test_bracket_on_same_cycle_coexists_with_trailing_stop(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Given an armed bracket on cycle C, an armed trailing_stop on C is allowed.

        Given: both partial unique indexes (bracket + trailing_stop)
            predicate on ``plan_type``,
        When: an active ``bracket`` and an active ``trailing_stop`` on
            the same cycle are inserted,
        Then: both inserts succeed — the indexes cover disjoint
            plan_type subsets.
        """
        engine, _ = migrated_db
        _insert_execution_plan(
            engine,
            plan_type="bracket",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )

    def test_downgrade_drops_index_and_allows_duplicates(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Given `alembic downgrade 0008`, the index is gone and duplicates insert.

        Given: a post-downgrade DB at revision 0008,
        When: two active trailing_stops on the same cycle are inserted,
        Then: both inserts succeed (no partial index to reject the
            second) — this is the behaviour expected of any DB that
            came through the 0001 init without this migration applied.
        """
        engine, cfg = migrated_db
        command.downgrade(cfg, "0008")
        assert not _index_exists(engine)
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )

    def test_round_trip_restores_enforcement(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Given upgrade -> downgrade -> upgrade, the index is reinstated.

        Given: a DB that has been through upgrade, then `downgrade 0008`,
            then `upgrade head`,
        When: two active trailing_stops on the same cycle are inserted,
        Then: the second raises ``IntegrityError`` — the index was
            re-created by the second upgrade.
        """
        engine, cfg = migrated_db
        command.downgrade(cfg, "0008")
        command.upgrade(cfg, "head")
        assert _index_exists(engine)
        _insert_execution_plan(
            engine,
            plan_type="trailing_stop",
            status="armed",
            position_cycle_public_id=_CYCLE_ID,
        )
        with pytest.raises(sa.exc.IntegrityError):
            _insert_execution_plan(
                engine,
                plan_type="trailing_stop",
                status="armed",
                position_cycle_public_id=_CYCLE_ID,
            )
