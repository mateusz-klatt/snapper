"""Tests for Alembic migration 0073: awaiting_target trade command status."""

import importlib
import shutil
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import TradeCommand
from tests.data.conftest import make_alembic_config

_MIGRATION_0073 = importlib.import_module(
    "snapper.data.migrations.versions.0073_trade_command_awaiting_target"
)


def _norm_ws(value: str | None) -> str:
    """Normalize whitespace in SQL text comparisons."""
    if value is None:
        return ""
    return " ".join(value.split())


def _schema_fingerprint(
    conn: sa.Connection,
) -> tuple[
    list[tuple[str, str, bool]],
    list[str],
    list[tuple[str | None, list[str], bool, str]],
    list[tuple[str | None, str]],
]:
    """Capture columns, primary key, indexes, and checks for trade_commands."""
    insp = sa.inspect(conn)
    columns = sorted(
        (
            str(col["name"]),
            str(col["type"]),
            bool(col["nullable"]),
        )
        for col in insp.get_columns("trade_commands")
    )
    pk_cols = list(insp.get_pk_constraint("trade_commands").get("constrained_columns") or [])
    indexes = sorted(
        (
            idx.get("name"),
            list(idx.get("column_names") or []),
            bool(idx.get("unique")),
            _norm_ws(
                str(idx["dialect_options"].get("sqlite_where", ""))
                if isinstance(idx.get("dialect_options"), dict)
                else ""
            ),
        )
        for idx in insp.get_indexes("trade_commands")
    )
    checks = sorted(
        (chk.get("name"), _norm_ws(str(chk.get("sqltext") or "")))
        for chk in insp.get_check_constraints("trade_commands")
    )
    return (columns, pk_cols, indexes, checks)


def _model_table_fingerprint(
    tmp_path: Path,
) -> tuple[
    list[tuple[str, str, bool]],
    list[str],
    list[tuple[str | None, list[str], bool, str]],
    list[tuple[str | None, str]],
]:
    """Capture the expected trade_commands fingerprint from ORM metadata."""
    table = TradeCommand.__table__
    orm_db = tmp_path / "orm_model_0073.sqlite3"
    orm_engine = create_engine(f"sqlite:///{orm_db}", poolclass=NullPool)
    try:
        table.create(orm_engine)
        with orm_engine.connect() as conn:
            return _schema_fingerprint(conn)
    finally:
        orm_engine.dispose()


def _insert_trade_command(
    conn: sa.Connection,
    *,
    public_id: str,
    client_order_id: str,
    status: str,
) -> None:
    """Insert a minimal trade_commands row using explicit SQL."""
    now_str = "2026-04-10 12:00:00+00:00"
    conn.execute(
        sa.text(
            "INSERT INTO trade_commands ("
            "public_id, session_id, sequence_id, timestamp, known_to, "
            "command_type, shard_key, wallet_public_id, exchange, instrument, "
            "mode, strategy_id, client_order_id, venue_client_id, side, "
            "order_type, quantity, reduce_only, post_only, status, attempt_count, "
            "created_at, correlation_id, source_surface, origin"
            ") VALUES ("
            ":public_id, 'sess-1', 1, :now, :known_to, "
            "'cancel', 'kraken.BTC-USD.live', 'wallet-1', 'kraken', 'BTC-USD', "
            "'live', 'strat-1', :cid, :cid, 'buy', "
            "'limit', 1.0, 0, 0, :status, 0, "
            ":now, :corr, 'rest', 'live'"
            ")"
        ),
        {
            "public_id": public_id,
            "now": now_str,
            "known_to": KNOWN_TO_MAX.isoformat(sep=" "),
            "cid": client_order_id,
            "status": status,
            "corr": f"corr-{public_id}",
        },
    )


def _current_revision(db_path: Path) -> str | None:
    """Read Alembic revision from a SQLite database."""
    engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    try:
        with engine.connect() as conn:
            return str(
                conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar_one()
            )
    finally:
        engine.dispose()


def test_0073_round_trip_and_schema_parity(
    migrated_db_path: Path,
    tmp_path: Path,
) -> None:
    """Empty 0072 <-> 0073 round-trip preserves exact trade_commands schema parity.

    Given: The migrated head database and an isolated copy moved to 0072,
    When: Migration 0073 upgrades, downgrades, and re-upgrades on SQLite,
    Then: Head matches ORM TradeCommand metadata and empty downgrade succeeds.
    """
    db_path = tmp_path / "roundtrip_0073.sqlite3"
    shutil.copyfile(migrated_db_path, db_path)
    cfg = make_alembic_config(f"sqlite:///{db_path}")

    engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    try:
        with engine.connect() as conn:
            head_fp = _schema_fingerprint(conn)
    finally:
        engine.dispose()
    model_fp = _model_table_fingerprint(tmp_path)
    assert head_fp[:3] == model_fp[:3]
    head_checks = dict(head_fp[3])
    model_checks = dict(model_fp[3])
    assert head_checks["ck_trade_commands_status"] == model_checks["ck_trade_commands_status"]
    assert "'awaiting_target'" in (head_checks["ck_trade_commands_status"] or "")

    command.downgrade(cfg, "0072")
    assert _current_revision(db_path) == "0072"
    engine_0072 = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    try:
        with engine_0072.connect() as conn:
            pre_fp = _schema_fingerprint(conn)
    finally:
        engine_0072.dispose()
    assert pre_fp[:3] == head_fp[:3]
    pre_checks = dict(pre_fp[3])
    assert pre_checks.keys() == head_checks.keys()
    for name, expr in head_checks.items():
        if name != "ck_trade_commands_status":
            assert pre_checks[name] == expr
    assert "'awaiting_target'" not in (pre_checks["ck_trade_commands_status"] or "")

    command.upgrade(cfg, "0073")
    assert _current_revision(db_path) == "0073"
    engine_after = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    try:
        with engine_after.connect() as conn:
            reupgraded_fp = _schema_fingerprint(conn)
    finally:
        engine_after.dispose()
    assert reupgraded_fp == head_fp


def test_0073_check_constraint_enforces_status_and_blocks_populated_downgrade(
    tmp_path: Path,
) -> None:
    """Status CHECK allows awaiting_target only at 0073 and downgrade refuses live rows.

    Given: A SQLite database upgraded from 0072 to 0073,
    When: Valid, awaiting_target, and unknown statuses are inserted and a
        populated awaiting_target row blocks downgrade until cleared,
    Then: Unknown statuses always fail IntegrityError, populated downgrade raises
        RuntimeError without altering revision, and clearing allows clean downgrade.
    """
    db_path = tmp_path / "guarded_0073.sqlite3"
    cfg = make_alembic_config(f"sqlite:///{db_path}")
    command.upgrade(cfg, "0072")

    engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    try:
        with engine.begin() as conn, pytest.raises(IntegrityError):
            _insert_trade_command(
                conn,
                public_id="cmd-pre-awaiting",
                client_order_id="cid-pre-awaiting",
                status="awaiting_target",
            )

        command.upgrade(cfg, "0073")
        with engine.begin() as conn:
            _insert_trade_command(
                conn,
                public_id="cmd-created",
                client_order_id="cid-created",
                status="created",
            )
            _insert_trade_command(
                conn,
                public_id="cmd-awaiting",
                client_order_id="cid-awaiting",
                status="awaiting_target",
            )
        with engine.begin() as conn, pytest.raises(IntegrityError):
            _insert_trade_command(
                conn,
                public_id="cmd-bogus",
                client_order_id="cid-bogus",
                status="bogus_status",
            )

        with pytest.raises(RuntimeError, match="awaiting_target"):
            command.downgrade(cfg, "0072")
        assert _current_revision(db_path) == "0073"

        with engine.begin() as conn:
            status_val = conn.execute(
                sa.text("SELECT status FROM trade_commands WHERE public_id = 'cmd-awaiting'")
            ).scalar_one()
            assert status_val == "awaiting_target"
            conn.execute(
                sa.text(
                    "UPDATE trade_commands SET status = 'cancelled' "
                    "WHERE public_id = 'cmd-awaiting'"
                )
            )

        command.downgrade(cfg, "0072")
        assert _current_revision(db_path) == "0072"

        with engine.begin() as conn, pytest.raises(IntegrityError):
            _insert_trade_command(
                conn,
                public_id="cmd-post-downgrade",
                client_order_id="cid-post-downgrade",
                status="awaiting_target",
            )

        command.upgrade(cfg, "0073")
        with engine.begin() as conn:
            _insert_trade_command(
                conn,
                public_id="cmd-reupgraded",
                client_order_id="cid-reupgraded",
                status="awaiting_target",
            )
    finally:
        engine.dispose()


def test_0073_postgresql_ddl_and_offline_sql(capsys: pytest.CaptureFixture[str]) -> None:
    """PostgreSQL online/offline paths use autocommit NOT VALID + VALIDATE and guard rows.

    Given: Alembic PostgreSQL offline SQL mode and mocked PostgreSQL online binds,
    When: 0073 upgrade and downgrade execute on both offline and mocked PG binds,
    Then: Offline SQL emits DROP + NOT VALID + VALIDATE, online PG downgrades
        when count is zero, and online PG raises RuntimeError when rows exist.
    """
    pg_cfg = make_alembic_config("postgresql+psycopg://user:pass@localhost/snapper")
    command.upgrade(pg_cfg, "0072:0073", sql=True)
    upgrade_sql = capsys.readouterr().out
    assert (
        "ALTER TABLE trade_commands DROP CONSTRAINT IF EXISTS ck_trade_commands_status, "
        "ADD CONSTRAINT ck_trade_commands_status CHECK"
    ) in upgrade_sql
    assert "'awaiting_target'" in upgrade_sql
    assert "NOT VALID" in upgrade_sql
    assert "VALIDATE CONSTRAINT ck_trade_commands_status" in upgrade_sql

    command.downgrade(pg_cfg, "0073:0072", sql=True)
    downgrade_sql = capsys.readouterr().out
    assert (
        "ALTER TABLE trade_commands DROP CONSTRAINT IF EXISTS ck_trade_commands_status, "
        "ADD CONSTRAINT ck_trade_commands_status CHECK"
    ) in downgrade_sql
    assert "'awaiting_target'" not in downgrade_sql
    assert "NOT VALID" in downgrade_sql
    assert "VALIDATE CONSTRAINT ck_trade_commands_status" in downgrade_sql

    executed: list[str] = []
    bind_zero = MagicMock()
    bind_zero.dialect.name = "postgresql"
    bind_zero.execute.return_value.scalar_one.return_value = 0
    ctx_zero = MagicMock()
    ctx_zero.as_sql = False

    with (
        patch.object(_MIGRATION_0073.op, "get_bind", return_value=bind_zero),
        patch.object(_MIGRATION_0073.op, "get_context", return_value=ctx_zero),
        patch.object(
            _MIGRATION_0073.op,
            "execute",
            side_effect=lambda sql: executed.append(str(sql)),
        ),
    ):
        _MIGRATION_0073.downgrade()

    ctx_zero.autocommit_block.assert_called_once()
    assert len(executed) == 2
    assert "DROP CONSTRAINT IF EXISTS ck_trade_commands_status" in executed[0]
    assert "ADD CONSTRAINT ck_trade_commands_status CHECK" in executed[0]
    assert "NOT VALID" in executed[0]
    assert "VALIDATE CONSTRAINT ck_trade_commands_status" in executed[1]

    bind_nonzero = MagicMock()
    bind_nonzero.dialect.name = "postgresql"
    bind_nonzero.execute.return_value.scalar_one.return_value = 2
    with (
        patch.object(_MIGRATION_0073.op, "get_bind", return_value=bind_nonzero),
        patch.object(_MIGRATION_0073.op, "get_context", return_value=ctx_zero),
        pytest.raises(RuntimeError, match="2 trade_commands row"),
    ):
        _MIGRATION_0073.downgrade()
