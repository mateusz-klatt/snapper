"""Tests for the 0015 ai_delegates operational-row backfill migration.

Delegates minted before the service fix have AI_DELEGATE user rows but
no ``ai_delegates`` operational row, so WS auth resolves
``delegate_public_id`` to None and AI-review admission can never see
them live. The migration inserts the missing row per delegate user
(SCD2 user versions dedup via DISTINCT) and leaves existing rows
untouched.
"""

import re
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TS = "2026-07-03 12:00:00.000000"
_ACTIVE = "9999-12-31 23:59:59.000000"
_UUID7_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")

_USER_MISSING = "019f0000-0000-7000-8000-000000000001"
_USER_SEEDED = "019f0000-0000-7000-8000-000000000002"
_USER_HUMAN = "019f0000-0000-7000-8000-000000000003"
_SEEDED_DELEGATE_ROW = "019f0000-0000-7000-8000-00000000000d"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: Database URL used by Alembic.

    Returns:
        Configured Alembic ``Config``.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_user(
    connection: sa.Connection,
    *,
    public_id: str,
    username: str,
    role: str,
    known_to: str = _ACTIVE,
) -> None:
    """Insert a minimal users row at the given SCD2 horizon."""
    connection.execute(
        sa.text(
            "INSERT INTO users (public_id, username, email, password_hash, role, "
            "is_active, created_at, session_id, sequence_id, timestamp, known_to) "
            "VALUES (:public_id, :username, NULL, 'x', :role, 1, :ts, :public_id, 1, "
            ":ts, :known_to)"
        ),
        {
            "public_id": public_id,
            "username": username,
            "role": role,
            "ts": _TS,
            "known_to": known_to,
        },
    )


@pytest.fixture
def db_at_0014(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded to 0014 with pre-fix delegates.

    Seeds: one delegate user WITHOUT an operational row (two SCD2
    versions to prove DISTINCT dedup), one delegate user WITH an
    existing row, and one human user that must never get a row.

    Args:
        tmp_path: Per-test temporary directory.

    Yields:
        Tuple of SQLAlchemy engine and Alembic config.
    """
    db_path = tmp_path / "ai_delegate_backfill.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0014")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _insert_user(
            connection,
            public_id=_USER_MISSING,
            username="ai-missing-row",
            role="ai_delegate",
            known_to=_TS,
        )
        _insert_user(
            connection,
            public_id=_USER_MISSING,
            username="ai-missing-row",
            role="ai_delegate",
        )
        _insert_user(
            connection,
            public_id=_USER_SEEDED,
            username="ai-seeded-row",
            role="ai_delegate",
        )
        _insert_user(
            connection,
            public_id=_USER_HUMAN,
            username="human-operator",
            role="operator",
        )
        connection.execute(
            sa.text(
                "INSERT INTO ai_delegates (public_id, user_public_id, last_seen_at, "
                "active_reviews_count, created_at, updated_at) "
                "VALUES (:row_id, :user_public_id, :ts, 1, :ts, :ts)"
            ),
            {"row_id": _SEEDED_DELEGATE_ROW, "user_public_id": _USER_SEEDED, "ts": _TS},
        )
    yield engine, cfg
    engine.dispose()


def test_backfill_inserts_missing_operational_rows(
    db_at_0014: tuple[sa.Engine, Config],
) -> None:
    """Verify 0015 creates exactly one fresh row per row-less delegate.

    Given: A delegate user without an operational row (two SCD2 versions),
    When: Upgrading to 0015,
    Then: Exactly one ai_delegates row exists for it, with a UUID7
        public_id, NULL last_seen_at, and a zeroed in-flight counter.
    """
    engine, cfg = db_at_0014
    command.upgrade(cfg, "0015")
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT public_id, last_seen_at, active_reviews_count FROM ai_delegates "
                "WHERE user_public_id = :user"
            ),
            {"user": _USER_MISSING},
        ).fetchall()
    assert len(rows) == 1
    public_id, last_seen_at, active_reviews_count = rows[0]
    assert _UUID7_RE.match(public_id)
    assert last_seen_at is None
    assert active_reviews_count == 0


def test_backfill_leaves_existing_rows_and_humans_untouched(
    db_at_0014: tuple[sa.Engine, Config],
) -> None:
    """Verify 0015 never duplicates rows or touches non-delegate users.

    Given: A delegate user with a pre-existing operational row and a
        human user,
    When: Upgrading to 0015,
    Then: The seeded row keeps its identity and counter, and the human
        user gains no row.
    """
    engine, cfg = db_at_0014
    command.upgrade(cfg, "0015")
    with engine.connect() as connection:
        seeded = connection.execute(
            sa.text(
                "SELECT public_id, active_reviews_count FROM ai_delegates "
                "WHERE user_public_id = :user"
            ),
            {"user": _USER_SEEDED},
        ).fetchall()
        human = connection.execute(
            sa.text("SELECT COUNT(*) FROM ai_delegates WHERE user_public_id = :user"),
            {"user": _USER_HUMAN},
        ).scalar_one()
    assert seeded == [(_SEEDED_DELEGATE_ROW, 1)]
    assert human == 0
