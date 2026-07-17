"""Tests for the dual-dialect execution scope + counter migration 0029.

Refusal atomicity is part of the contract under test: SQLite/Alembic has
no transactional DDL, so every fail-closed validation must run BEFORE the
first DDL statement — each refusal test therefore asserts EXACT schema
equality (columns, indexes, CHECKs of both touched tables) around the
aborted run, and the remediate-and-retry test proves a clean second
attempt succeeds without colliding with half-applied schema.

Quiescence is the other half of the contract, and it is enforced rather
than assumed: ``executions`` is live-written and the deploy shape stops
no executor, so the write-fence tests drive a GENUINELY concurrent
second connection — a real engine running the deployed revision-0028
INSERT shape — into the migration's read-then-write window through an
engine-level cursor hook. Those tests assert the intruder is REFUSED
(``database is locked``), never merely that it is absent from the final
rows: an absent-row assertion would pass vacuously if the writer had
silently no-op'd, certifying nothing.

Every fence verdict is wall-clock FREE. The contending connections all
run with busy_timeout=0, so SQLite never sleeps on contention and each
outcome is a pure function of whether the write reservation is held at
that instant, not of whether a timed wait happened to expire first. A
fence proof whose verdict could drift under a loaded CPU would be a
probability wearing a theorem's clothes — the one shape of certification
this suite exists to refuse.
"""

import contextlib
import importlib
from collections.abc import Iterator
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event
from sqlalchemy.engine import Engine

from snapper.data.models import Execution

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-17 08:00:00.000000"
_WALLET_A = "wallet-a"
_WALLET_B = "wallet-b"
_UUID_WALLET = "0000face-0000-7000-8000-000000000001"
_MIGRATION_MODULE = "snapper.data.migrations.versions.0029_execution_scope_counter"
_SNAPSHOT_TABLES = ("executions", "portfolio_spot_reconciliation_anchors")
_NO_BUSY_WAIT = 0
_ANCHOR_PROBE_SQL = "SELECT COUNT(*) FROM portfolio_spot_reconciliation_anchors"
_BACKFILL_SQL = "UPDATE executions SET exchange"


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _seed_instrument(connection: sa.Connection, public_id: str, exchange: str) -> None:
    """Insert one active migration-level instrument version."""
    connection.execute(
        sa.text(
            "INSERT INTO instruments (public_id, symbol_public_id, exchange, "
            "session_id, sequence_id, timestamp, known_to) VALUES "
            "(:public_id, :public_id, :exchange, 'session-1', 1, :timestamp, :known_to)"
        ),
        {"public_id": public_id, "exchange": exchange, "timestamp": _TS, "known_to": _ACTIVE},
    )


def _seed_order(
    connection: sa.Connection,
    public_id: str,
    instrument_public_id: str,
    mode: str,
    wallet_public_id: str,
) -> None:
    """Insert one active migration-level order version carrying the scope."""
    connection.execute(
        sa.text(
            "INSERT INTO orders (public_id, instrument_public_id, mode, "
            "wallet_public_id, side, order_type, size, status, created_at, "
            "session_id, sequence_id, timestamp, known_to) VALUES "
            "(:public_id, :instrument_public_id, :mode, :wallet_public_id, "
            "'buy', 'limit', 1.0, 'filled', :timestamp, 'session-1', 1, "
            ":timestamp, :known_to)"
        ),
        {
            "public_id": public_id,
            "instrument_public_id": instrument_public_id,
            "mode": mode,
            "wallet_public_id": wallet_public_id,
            "timestamp": _TS,
            "known_to": _ACTIVE,
        },
    )


def _seed_execution(
    connection: sa.Connection,
    public_id: str,
    order_public_id: str,
    wallet_public_id: str,
) -> None:
    """Insert one legacy migration-level execution without scope columns."""
    connection.execute(
        sa.text(
            "INSERT INTO executions (public_id, order_public_id, wallet_public_id, "
            "side, status, price, size, fee, fee_asset, session_id, sequence_id, "
            "timestamp, known_to) VALUES (:public_id, :order_public_id, "
            ":wallet_public_id, 'buy', 'filled', 1.25, 2.0, 0.1, 'PLN', "
            "'session-1', 1, :timestamp, :known_to)"
        ),
        {
            "public_id": public_id,
            "order_public_id": order_public_id,
            "wallet_public_id": wallet_public_id,
            "timestamp": _TS,
            "known_to": _ACTIVE,
        },
    )


def _seed_two_scope_lineage(engine: sa.Engine) -> None:
    """Seed active lineage for three scopes with interleaved execution ids.

    Scope A (wallet-a, walutomat, live) receives ids 1, 3, 6; scope B
    (wallet-a, walutomat, paper) ids 2, 5; scope C (wallet-b, kraken,
    live) id 4 — so per-scope numbering must skip foreign ids.
    """
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_instrument(connection, "inst-k", "kraken")
        _seed_order(connection, "order-live-w", "inst-w", "live", _WALLET_A)
        _seed_order(connection, "order-paper-w", "inst-w", "paper", _WALLET_A)
        _seed_order(connection, "order-live-k", "inst-k", "live", _WALLET_B)
        _seed_execution(connection, "exec-1", "order-live-w", _WALLET_A)
        _seed_execution(connection, "exec-2", "order-paper-w", _WALLET_A)
        _seed_execution(connection, "exec-3", "order-live-w", _WALLET_A)
        _seed_execution(connection, "exec-4", "order-live-k", _WALLET_B)
        _seed_execution(connection, "exec-5", "order-paper-w", _WALLET_A)
        _seed_execution(connection, "exec-6", "order-live-w", _WALLET_A)


def _insert_anchor(engine: sa.Engine, public_id: str, kind: str) -> None:
    """Insert one migration-level spot anchor with the given watermark kind."""
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_spot_reconciliation_anchors "
                "(wallet_public_id, exchange, mode, venue_account_state_public_id, "
                "balance_observation_id, source_watermark_kind, source_watermark, "
                "balances_json, first_request_started_at, first_request_completed_at, "
                "second_request_started_at, second_request_completed_at, "
                "boundary_status, inventory_status, margin_status, provenance, "
                "public_id, session_id, sequence_id, timestamp, known_to) VALUES "
                "('wallet-a', 'walutomat', 'live', 'state-1', 1, :kind, 3, "
                '\'{"BTC":"1"}\', :timestamp, :timestamp, :timestamp, :timestamp, '
                "'double_read_equal', 'certified_full', 'cash', 'test', "
                ":public_id, 'session-1', 1, :timestamp, :known_to)"
            ),
            {"kind": kind, "public_id": public_id, "timestamp": _TS, "known_to": _ACTIVE},
        )


@contextlib.contextmanager
def _old_writer_injected_at(
    db_url: str,
    trigger_prefix: str,
    public_id: str,
    order_public_id: str,
    wallet_public_id: str,
) -> Iterator[list[str]]:
    """Drive a real revision-0028 writer into the migration's window.

    Installs an engine-level ``before_cursor_execute`` hook that fires
    once, when the migration is about to run ``trigger_prefix``, and from
    a SEPARATE engine (a genuinely different connection, not a mock)
    attempts the deployed 0028-shape execution INSERT: no scope columns,
    and the wallet identity persisted VERBATIM — the canonicalization
    only shipped with the post-migration writer.

    The intruder runs with busy_timeout=0 so the proof is a THEOREM, not
    a race: SQLite then never sleeps on contention, so the outcome is a
    pure function of whether the write reservation is held at that
    instant — refused IMMEDIATELY when it is, committed IMMEDIATELY when
    it is not. A non-zero timeout would instead assert that a wait
    EXPIRED, making the verdict depend on wall-clock scheduling under a
    loaded CPU; a fence proof that can drift is exactly the shape of
    certification this suite must refuse to emit. Determinism costs no
    discriminating power here: the hook is invoked synchronously on the
    migration's OWN thread, which is parked inside it still holding the
    reservation, so the lock provably cannot be released while the
    intruder probes — the wait could only ever expire, never succeed.
    Yields a one-element list receiving ``COMMITTED`` or ``BLOCKED: ...``
    so a test can prove the writer was REFUSED rather than merely absent.
    """
    outcome: list[str] = []

    def inject(
        conn: sa.Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        """Fire the intruder once, immediately before the trigger statement."""
        if outcome or not " ".join(statement.split()).startswith(trigger_prefix):
            return
        outcome.append("PENDING")
        writer = sa.create_engine(db_url, connect_args={"timeout": _NO_BUSY_WAIT})
        try:
            with writer.begin() as writer_connection:
                _seed_execution(writer_connection, public_id, order_public_id, wallet_public_id)
            outcome[0] = "COMMITTED"
        except sa.exc.OperationalError as exc:
            outcome[0] = f"BLOCKED: {exc}"
        finally:
            writer.dispose()

    event.listen(Engine, "before_cursor_execute", inject)
    try:
        yield outcome
    finally:
        event.remove(Engine, "before_cursor_execute", inject)


def _second_connection_can_write(db_url: str, public_id: str) -> bool:
    """Return whether an independent connection can still take the write lock.

    Probes for a LEAKED reservation: a migration that failed to release
    its fence would wedge the database against the restarted executor, so
    every fence test asserts this is true once the migration has returned.
    busy_timeout=0 keeps this verdict wall-clock free for the same reason
    the intruder is: the migration has already RETURNED, so a correctly
    released reservation is observably gone and the write succeeds with
    no waiting, while a leaked one is reported instantly instead of after
    a sleep that a loaded CPU could stretch past the suite's timeout.
    """
    prober = sa.create_engine(db_url, connect_args={"timeout": _NO_BUSY_WAIT})
    try:
        with prober.begin() as connection:
            _seed_instrument(connection, public_id, "walutomat")
        return True
    except sa.exc.OperationalError:
        return False
    finally:
        prober.dispose()


def _alembic_version(engine: sa.Engine) -> str:
    """Return the single stamped Alembic revision."""
    with engine.connect() as connection:
        return str(connection.execute(sa.text("SELECT version_num FROM alembic_version")).scalar())


def _schema_snapshot(engine: sa.Engine) -> dict[str, dict[str, object]]:
    """Capture the exact shape of both migration-touched tables.

    Columns (type + nullability), indexes (columns + uniqueness), and
    CHECK constraints (name + text) of ``executions`` and the spot anchor
    table — the complete surface migration 0029 mutates. Refusal tests
    compare snapshots for EXACT equality so a validation that ran after
    even one DDL statement fails loudly.
    """
    inspector = sa.inspect(engine)
    snapshot: dict[str, dict[str, object]] = {}
    for table in _SNAPSHOT_TABLES:
        snapshot[table] = {
            "columns": {
                column["name"]: (str(column["type"]), bool(column["nullable"]))
                for column in inspector.get_columns(table)
            },
            "indexes": {
                index["name"]: (tuple(index["column_names"]), bool(index["unique"]))
                for index in inspector.get_indexes(table)
            },
            "checks": {
                constraint["name"]: constraint["sqltext"]
                for constraint in inspector.get_check_constraints(table)
            },
        }
    return snapshot


def test_0029_sqlite_backfills_scopes_tightens_and_swaps_anchor_kind(
    tmp_path: Path,
) -> None:
    """SQLite upgrade derives scopes, numbers per scope by id, and tightens.

    Given: Legacy executions across three scopes with interleaved ids and
        sound active lineage at revision 0028.
    When: Migration 0029 upgrades, then downgrades, then re-upgrades.
    Then: Each scope carries contiguous 1..N counters in id order with the
        lineage-derived exchange/mode, the new CHECKs and the TOTAL unique
        index enforce the scope plane, the anchor watermark kind flips to
        ``scope_sequence``, and the downgrade removes the plane cleanly.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-counter.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    _seed_two_scope_lineage(engine)

    command.upgrade(config, "0029")
    inspector = sa.inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns("executions")}
    assert columns["exchange"]["nullable"] is False
    assert columns["mode"]["nullable"] is False
    assert columns["scope_sequence"]["nullable"] is False
    indexes = {index["name"]: index for index in inspector.get_indexes("executions")}
    assert indexes["uq_executions_scope_sequence"]["unique"] == 1
    assert indexes["uq_executions_scope_sequence"]["column_names"] == [
        "wallet_public_id",
        "exchange",
        "mode",
        "scope_sequence",
    ]
    assert "sqlite_where" not in indexes["uq_executions_scope_sequence"].get("dialect_options", {})
    constraints = {
        constraint["name"] for constraint in inspector.get_check_constraints("executions")
    }
    assert {
        "ck_executions_exchange_lower",
        "ck_executions_mode",
        "ck_executions_scope_sequence",
    } <= constraints
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT public_id, wallet_public_id, exchange, mode, scope_sequence "
                "FROM executions ORDER BY id"
            )
        ).all()
    assert [tuple(row) for row in rows] == [
        ("exec-1", _WALLET_A, "walutomat", "live", 1),
        ("exec-2", _WALLET_A, "walutomat", "paper", 1),
        ("exec-3", _WALLET_A, "walutomat", "live", 2),
        ("exec-4", _WALLET_B, "kraken", "live", 1),
        ("exec-5", _WALLET_A, "walutomat", "paper", 2),
        ("exec-6", _WALLET_A, "walutomat", "live", 3),
    ]

    with pytest.raises(sa.exc.IntegrityError), engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO executions (public_id, order_public_id, "
                "wallet_public_id, exchange, mode, scope_sequence, side, status, "
                "price, size, fee, fee_asset, session_id, sequence_id, timestamp, "
                "known_to) VALUES ('exec-dup', 'order-live-w', :wallet, "
                "'walutomat', 'live', 1, 'buy', 'filled', 1.25, 2.0, 0.1, 'PLN', "
                "'session-1', 1, :timestamp, :known_to)"
            ),
            {"wallet": _WALLET_A, "timestamp": _TS, "known_to": _ACTIVE},
        )
    for bad_scope in (
        "'WALUTOMAT', 'live', 9",
        "' ', 'live', 9",
        "'walutomat', 'simulated', 9",
        "'walutomat', 'live', 0",
    ):
        with pytest.raises(sa.exc.IntegrityError), engine.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO executions (public_id, order_public_id, "
                    "wallet_public_id, exchange, mode, scope_sequence, side, "
                    "status, price, size, fee, fee_asset, session_id, "
                    "sequence_id, timestamp, known_to) VALUES ('exec-bad', "
                    f"'order-live-w', :wallet, {bad_scope}, 'buy', 'filled', "
                    "1.25, 2.0, 0.1, 'PLN', 'session-1', 1, :timestamp, :known_to)"
                ),
                {"wallet": _WALLET_A, "timestamp": _TS, "known_to": _ACTIVE},
            )

    with pytest.raises(sa.exc.IntegrityError):
        _insert_anchor(engine, "anchor-old-kind", "execution_id")
    _insert_anchor(engine, "anchor-new-kind", "scope_sequence")
    with engine.begin() as connection:
        connection.execute(sa.text("DELETE FROM portfolio_spot_reconciliation_anchors"))

    command.downgrade(config, "0028")
    downgraded = {column["name"] for column in sa.inspect(engine).get_columns("executions")}
    assert {"exchange", "mode", "scope_sequence"}.isdisjoint(downgraded)
    assert "uq_executions_scope_sequence" not in {
        index["name"] for index in sa.inspect(engine).get_indexes("executions")
    }
    with engine.connect() as connection:
        count = connection.execute(sa.text("SELECT COUNT(*) FROM executions")).scalar()
    assert count == 6
    _insert_anchor(engine, "anchor-legacy-kind", "execution_id")
    with engine.begin() as connection:
        connection.execute(sa.text("DELETE FROM portfolio_spot_reconciliation_anchors"))

    command.upgrade(config, "0029")
    assert _alembic_version(engine) == "0029"
    engine.dispose()


def test_0029_sqlite_aborts_on_dangling_order_lineage_then_retries_clean(
    tmp_path: Path,
) -> None:
    """A dangling-order refusal leaves 0028 untouched and a retry succeeds.

    Guessing a scope for an unresolvable row would fabricate certification
    input, so the migration must fail closed with actionable evidence —
    and because SQLite has no transactional DDL, the refusal must happen
    BEFORE any DDL so the remediated retry does not collide with
    half-applied columns or indexes.

    Given: One execution referencing a missing order at revision 0028.
    When: Migration 0029 upgrades, the operator seeds the missing active
        lineage, and the upgrade re-runs.
    Then: The first run raises naming the dangling-order violation with
        the sample public id, the stamped revision stays 0028, the schema
        is EXACTLY unchanged; the retry reaches 0029 and backfills the
        remediated row.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-dangling.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_execution(connection, "exec-dangling", "order-missing", _WALLET_A)
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="dangling order lineage") as abort:
        command.upgrade(config, "0029")
    assert "exec-dangling" in str(abort.value)
    assert _alembic_version(engine) == "0028"
    assert _schema_snapshot(engine) == before

    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_order(connection, "order-missing", "inst-w", "live", _WALLET_A)
    command.upgrade(config, "0029")
    assert _alembic_version(engine) == "0029"
    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT exchange, mode, scope_sequence FROM executions "
                "WHERE public_id = 'exec-dangling'"
            )
        ).one()
    assert tuple(row) == ("walutomat", "live", 1)
    engine.dispose()


def test_0029_sqlite_aborts_on_crossed_wallet_lineage(tmp_path: Path) -> None:
    """A legacy execution crossing into another wallet's order ABORTS.

    Given: An active order owned by wallet B and one execution labelled
        wallet A referencing it, at revision 0028.
    When: Migration 0029 upgrades.
    Then: The upgrade raises naming the crossed-wallet violation with the
        sample public id, and the stamped revision stays 0028.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-crossed.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_order(connection, "order-live-w", "inst-w", "live", _WALLET_B)
        _seed_execution(connection, "exec-crossed", "order-live-w", _WALLET_A)
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="crossed wallet lineage") as abort:
        command.upgrade(config, "0029")
    assert "exec-crossed" in str(abort.value)
    assert _alembic_version(engine) == "0028"
    assert _schema_snapshot(engine) == before
    engine.dispose()


def test_0029_sqlite_aborts_on_dangling_instrument_lineage(tmp_path: Path) -> None:
    """A legacy execution whose order lacks an active instrument ABORTS.

    Given: An active order referencing a missing instrument and one
        execution through it, at revision 0028.
    When: Migration 0029 upgrades.
    Then: The upgrade raises naming the dangling-instrument violation and
        the stamped revision stays 0028.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-orphan.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_order(connection, "order-orphan", "inst-missing", "live", _WALLET_A)
        _seed_execution(connection, "exec-orphan", "order-orphan", _WALLET_A)
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="dangling instrument lineage") as abort:
        command.upgrade(config, "0029")
    assert "exec-orphan" in str(abort.value)
    assert _alembic_version(engine) == "0028"
    assert _schema_snapshot(engine) == before
    engine.dispose()


def test_0029_sqlite_aborts_on_populated_anchor_table(tmp_path: Path) -> None:
    """Stored anchors block the watermark-kind swap fail-closed.

    No anchor writer has shipped, so a populated table means stored
    watermarks in the retired unit — the migration must refuse rather than
    silently rewrite their semantics.

    Given: One anchor row with the legacy kind at revision 0028.
    When: Migration 0029 upgrades.
    Then: The upgrade raises naming the anchor obstruction and the stamped
        revision stays 0028.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-anchored.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    _insert_anchor(engine, "anchor-preexisting", "execution_id")
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="anchor row"):
        command.upgrade(config, "0029")
    assert _alembic_version(engine) == "0028"
    assert _schema_snapshot(engine) == before
    engine.dispose()


def test_0029_sqlite_aborts_on_blank_instrument_exchange(tmp_path: Path) -> None:
    """A blank backfill-source exchange refuses the upgrade before any DDL.

    The legacy instruments CHECK enforces lowercase only, so a BLANK
    exchange is legal there while the new executions scope CHECK refuses
    it — backfilling it would fail only AFTER the DDL had run, leaving a
    partially upgraded schema.

    Given: An active blank-exchange instrument with an order and an
        execution through it, at revision 0028.
    When: Migration 0029 upgrades.
    Then: The upgrade raises naming the scope-domain violation with the
        sample public id, the stamped revision stays 0028, and the schema
        is EXACTLY unchanged.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-blank.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-blank", " ")
        _seed_order(connection, "order-blank", "inst-blank", "live", _WALLET_A)
        _seed_execution(connection, "exec-blank", "order-blank", _WALLET_A)
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="outside the executions scope") as abort:
        command.upgrade(config, "0029")
    assert "exec-blank" in str(abort.value)
    assert _alembic_version(engine) == "0028"
    assert _schema_snapshot(engine) == before
    engine.dispose()


def test_0029_sqlite_normalizes_alias_wallet_spellings_into_one_scope(
    tmp_path: Path,
) -> None:
    """Alias UUID wallet spellings merge into one canonical counter scope.

    SQLite stores wallet identities verbatim, so a raw-text backfill
    partition would number ``...ABC...`` and ``...abc...`` as separate
    scopes — duplicating logical sequences and hiding the alias rows from
    the canonical watermark capture. The migration must canonicalize
    before numbering, and the crossed-wallet check must compare
    canonically instead of refusing the alias.

    Given: An active order stored under the UPPERCASE alias spelling and
        two legacy executions under the alias and the canonical spelling,
        at revision 0028.
    When: Migration 0029 upgrades.
    Then: Both rows persist under the canonical spelling as ONE scope
        with contiguous counters 1..2, and no crossed-wallet refusal
        fires.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-alias.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_order(connection, "order-alias", "inst-w", "live", _UUID_WALLET.upper())
        _seed_execution(connection, "exec-alias", "order-alias", _UUID_WALLET.upper())
        _seed_execution(connection, "exec-canonical", "order-alias", _UUID_WALLET)
    command.upgrade(config, "0029")
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT public_id, wallet_public_id, exchange, mode, scope_sequence "
                "FROM executions ORDER BY id"
            )
        ).all()
    assert [tuple(row) for row in rows] == [
        ("exec-alias", _UUID_WALLET, "walutomat", "live", 1),
        ("exec-canonical", _UUID_WALLET, "walutomat", "live", 2),
    ]
    engine.dispose()


def test_0029_sqlite_downgrade_refuses_while_scope_sequence_anchor_exists(
    tmp_path: Path,
) -> None:
    """A stored scope_sequence anchor blocks the downgrade before any DDL.

    The downgrade restores the ``execution_id`` watermark unit, which
    cannot represent a ``scope_sequence`` anchor — and SQLite has no
    transactional DDL, so a late refusal would leave the executions table
    already rebuilt while the revision still reads 0029.

    Given: An upgraded database holding one scope_sequence anchor.
    When: Migration 0029 downgrades.
    Then: The downgrade raises naming the obstruction, the stamped
        revision stays 0029, the schema is EXACTLY unchanged; after the
        anchor is removed the downgrade succeeds.
    """
    db_url = f"sqlite:///{tmp_path / 'scope-downgrade.db'}"
    config = _config(db_url)
    command.upgrade(config, "0029")
    engine = sa.create_engine(db_url)
    _insert_anchor(engine, "anchor-live", "scope_sequence")
    before = _schema_snapshot(engine)
    with pytest.raises(RuntimeError, match="downgrade aborted"):
        command.downgrade(config, "0028")
    assert _alembic_version(engine) == "0029"
    assert _schema_snapshot(engine) == before

    with engine.begin() as connection:
        connection.execute(sa.text("DELETE FROM portfolio_spot_reconciliation_anchors"))
    command.downgrade(config, "0028")
    assert _alembic_version(engine) == "0028"
    downgraded = {column["name"] for column in sa.inspect(engine).get_columns("executions")}
    assert {"exchange", "mode", "scope_sequence"}.isdisjoint(downgraded)
    engine.dispose()


def test_0029_sqlite_write_fence_refuses_an_old_writer_at_the_backfill_window(
    tmp_path: Path,
) -> None:
    """A live 0028 writer cannot slip an alias-wallet fill past normalization.

    THE corruption this fence exists to prevent, and it is silent: the
    deployed 0028 writer stores the wallet identity VERBATIM, so a fill
    committed after ``_normalize_wallet_aliases`` keeps its alias
    spelling; the backfill partitions by RAW stored text, hands it
    ``scope_sequence = 1`` beside the canonical row, and the TOTAL unique
    index accepts the two as distinct textual scopes. The migration then
    SUCCEEDS with a published fill invisible to the canonical watermark
    capture — corrupt certification input, no error anywhere.

    Given: A canonical legacy execution with sound active lineage at
        revision 0028, and a genuinely concurrent second connection that
        commits an UPPERCASE-alias 0028-shape fill for the same logical
        wallet the instant the backfill is about to run.
    When: Migration 0029 upgrades.
    Then: The intruder is REFUSED with ``database is locked`` (proved
        explicitly, not inferred from its absence), the upgrade still
        reaches 0029, no two rows share a canonical scope counter, the
        canonical watermark equals the TRUE committed fill count, and the
        reservation is released so the restarted executor can write.
    """
    db_url = f"sqlite:///{tmp_path / 'fence-backfill.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_order(connection, "order-1", "inst-w", "live", _UUID_WALLET)
        _seed_execution(connection, "exec-canonical", "order-1", _UUID_WALLET)

    with _old_writer_injected_at(
        db_url, _BACKFILL_SQL, "exec-old-writer", "order-1", _UUID_WALLET.upper()
    ) as outcome:
        command.upgrade(config, "0029")

    assert outcome[0].startswith("BLOCKED"), f"old writer was not fenced out: {outcome[0]}"
    assert "database is locked" in outcome[0]
    assert _alembic_version(engine) == "0029"
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT public_id, wallet_public_id, exchange, mode, scope_sequence "
                "FROM executions ORDER BY id"
            )
        ).all()
        watermark = connection.execute(
            sa.text(
                "SELECT MAX(scope_sequence) FROM executions WHERE wallet_public_id = :wallet "
                "AND exchange = 'walutomat' AND mode = 'live'"
            ),
            {"wallet": _UUID_WALLET},
        ).scalar()
    assert [tuple(row) for row in rows] == [
        ("exec-canonical", _UUID_WALLET, "walutomat", "live", 1)
    ]
    scopes = [(str(row[1]).lower(), row[2], row[3], row[4]) for row in rows]
    assert len(set(scopes)) == len(scopes), f"duplicate canonical scope counter: {scopes}"
    assert watermark == len(rows), "canonical watermark hides a committed fill"
    assert _second_connection_can_write(db_url, "inst-after-success")
    engine.dispose()


def test_0029_sqlite_write_fence_refuses_an_old_writer_before_the_first_validation(
    tmp_path: Path,
) -> None:
    """The fence covers VALIDATION, not merely the normalization onward.

    This is the test that separates a real fix from a half-fix: the
    intruder is injected at the anchor-emptiness probe, which runs AFTER
    every lineage check has passed but BEFORE
    ``_normalize_wallet_aliases``. A fence taken only at the first WRITE
    would leave this window open, the unresolvable row would commit, the
    backfill would leave its scope NULL, and the NOT NULL tightening
    would fail AFTER the DDL had run — wedging the module's documented
    remediate-and-retry contract against half-applied columns. A fence
    taken before the first validation READ blocks it.

    Given: A sound legacy execution at revision 0028 and a concurrent
        second connection committing a DANGLING-lineage 0028-shape fill
        (one the lineage check would have refused) after that check has
        already passed.
    When: Migration 0029 upgrades.
    Then: The intruder is REFUSED with ``database is locked``, the
        upgrade reaches 0029 with a fully applied schema rather than a
        half-applied wedge, and the ledger holds only the sound row.
    """
    db_url = f"sqlite:///{tmp_path / 'fence-validation.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_instrument(connection, "inst-w", "walutomat")
        _seed_order(connection, "order-1", "inst-w", "live", _WALLET_A)
        _seed_execution(connection, "exec-sound", "order-1", _WALLET_A)

    with _old_writer_injected_at(
        db_url, _ANCHOR_PROBE_SQL, "exec-late-dangling", "order-missing", _WALLET_A
    ) as outcome:
        command.upgrade(config, "0029")

    assert outcome[0].startswith("BLOCKED"), f"old writer was not fenced out: {outcome[0]}"
    assert "database is locked" in outcome[0]
    assert _alembic_version(engine) == "0029"
    columns = {column["name"] for column in sa.inspect(engine).get_columns("executions")}
    assert {"exchange", "mode", "scope_sequence"} <= columns
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text("SELECT public_id, exchange, mode, scope_sequence FROM executions ORDER BY id")
        ).all()
    assert [tuple(row) for row in rows] == [("exec-sound", "walutomat", "live", 1)]
    assert _second_connection_can_write(db_url, "inst-after-success")
    engine.dispose()


def test_0029_sqlite_refuses_when_a_live_writer_already_holds_the_reservation(
    tmp_path: Path,
) -> None:
    """An executor already writing makes the migration refuse, not proceed.

    Refusing beats making safe: if the reservation cannot be taken, some
    other connection is mid-write and the operator has not actually
    quiesced the deployment. Because the fence is the migration's FIRST
    statement, that refusal lands before any validation or DDL, so
    revision 0028 is left EXACTLY intact and the retry is clean.

    The migration connection carries busy_timeout=0 so its refusal is a
    theorem rather than an expired wait: with the holder's reservation
    provably in place, ``BEGIN IMMEDIATE`` is refused IMMEDIATELY, and
    the retry — run once that reservation is provably gone — takes the
    fence with no waiting. The verdict therefore never depends on a
    sleep that a loaded CPU could stretch past the suite's timeout.

    Given: A sound revision-0028 database that would otherwise migrate
        cleanly, and a second connection holding SQLite's write
        reservation.
    When: Migration 0029 upgrades, and then re-runs once that writer has
        committed.
    Then: The first run raises ``database is locked``, the stamped
        revision stays 0028 and the schema is EXACTLY unchanged; the
        retry reaches 0029 and backfills every row.
    """
    db_path = tmp_path / "fence-held.db"
    db_url = f"sqlite:///{db_path}"
    config = _config(f"{db_url}?timeout={_NO_BUSY_WAIT}")
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    _seed_two_scope_lineage(engine)
    before = _schema_snapshot(engine)

    holder = sa.create_engine(db_url)
    with holder.connect() as holding_connection:
        holding_connection.execute(sa.text("BEGIN IMMEDIATE"))
        with pytest.raises(sa.exc.OperationalError, match="database is locked"):
            command.upgrade(config, "0029")
        assert _alembic_version(engine) == "0028"
        assert _schema_snapshot(engine) == before
        holding_connection.rollback()
    holder.dispose()

    command.upgrade(config, "0029")
    assert _alembic_version(engine) == "0029"
    with engine.connect() as connection:
        unresolved = connection.execute(
            sa.text("SELECT COUNT(*) FROM executions WHERE scope_sequence IS NULL")
        ).scalar()
    assert unresolved == 0
    engine.dispose()


def test_0029_sqlite_write_fence_is_released_after_a_refusal(tmp_path: Path) -> None:
    """A refused migration must not wedge the database against the executor.

    The fence is taken before the validations, so a fail-closed refusal
    now aborts while HOLDING it. If that reservation leaked, the restarted
    executor could never write again and a fail-closed refusal would
    escalate into an outage — strictly worse than the bug being fixed.
    Alembic's per-migration transaction must roll it back.

    Given: A revision-0028 database carrying a dangling-lineage row.
    When: Migration 0029 upgrades and refuses.
    Then: The refusal names the violation, and an INDEPENDENT connection
        can immediately take the write lock — proving the reservation was
        released on the abort path.
    """
    db_url = f"sqlite:///{tmp_path / 'fence-release.db'}"
    config = _config(db_url)
    command.upgrade(config, "0028")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        _seed_execution(connection, "exec-dangling", "order-missing", _WALLET_A)

    with pytest.raises(RuntimeError, match="dangling order lineage"):
        command.upgrade(config, "0029")

    assert _alembic_version(engine) == "0028"
    assert _second_connection_can_write(db_url, "inst-after-refusal")
    engine.dispose()


def test_0029_postgresql_takes_an_access_exclusive_write_fence() -> None:
    """PostgreSQL fences BOTH mutated tables with ACCESS EXCLUSIVE, up front.

    The native ``uuid`` type defeats the alias-split variant and
    transactional DDL turns an UNRESOLVABLE late row into a clean atomic
    abort, but neither catches a RESOLVABLE crossed-wallet row (stored
    under wallet B, referencing wallet A's valid active order): the
    lineage ``SELECT`` takes only ``ACCESS SHARE`` and the first DDL takes
    ``ACCESS EXCLUSIVE`` too late, so absent a fence such a row commits in
    the read-then-DDL window and is certified into wallet B's watermark.
    The fence therefore takes ``LOCK TABLE executions,
    portfolio_spot_reconciliation_anchors IN ACCESS EXCLUSIVE MODE`` as
    the FIRST statement — the same locks the DDL takes, up front,
    conflicting with the ``ROW EXCLUSIVE`` an ``INSERT`` holds. The anchor
    table is fenced for the same reason: both directions read its
    emptiness and then rebuild its watermark-kind CHECK, so a concurrent
    anchor writer must not slip between that read and the swap. Emitting
    SQLite's ``BEGIN IMMEDIATE`` there would be a syntax error, so the
    dispatch must stay dialect-aware.

    Given: The fence bound to an ONLINE PostgreSQL context.
    When: ``_acquire_migration_write_fence`` runs.
    Then: It emits exactly the combined ACCESS EXCLUSIVE table lock over
        both mutated tables and never SQLite's ``BEGIN IMMEDIATE``.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)
    postgres_op = MagicMock()
    postgres_op.get_context.return_value.as_sql = False
    postgres_op.get_bind.return_value.dialect.name = "postgresql"
    with patch.object(migration, "op", postgres_op):
        migration._acquire_migration_write_fence()
    statements = [
        " ".join(str(call.args[0]).split())
        for call in postgres_op.get_bind.return_value.execute.call_args_list
    ]
    assert statements == [
        "LOCK TABLE executions, portfolio_spot_reconciliation_anchors IN ACCESS EXCLUSIVE MODE"
    ]


def test_0029_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL offline DDL and ORM metadata expose the same scope plane."""
    migration = importlib.import_module(_MIGRATION_MODULE)
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with (
        patch.object(migration, "op", operations),
        patch.object(migration, "_acquire_migration_write_fence"),
        patch.object(migration, "_abort_on_broken_lineage"),
        patch.object(migration, "_abort_on_invalid_target_domain"),
        patch.object(migration, "_assert_anchor_table_empty"),
        patch.object(migration, "_normalize_wallet_aliases"),
    ):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0029"
    assert migration.down_revision == "0028"
    assert "ADD COLUMN exchange VARCHAR(32)" in ddl
    assert "ADD COLUMN mode VARCHAR(8)" in ddl
    assert "ADD COLUMN scope_sequence BIGINT" in ddl
    assert "ROW_NUMBER() OVER" in ddl
    assert "known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "ALTER COLUMN exchange SET NOT NULL" in ddl
    assert "ALTER COLUMN mode SET NOT NULL" in ddl
    assert "ALTER COLUMN scope_sequence SET NOT NULL" in ddl
    assert "ck_executions_exchange_lower" in ddl
    assert "ck_executions_mode" in ddl
    assert "ck_executions_scope_sequence" in ddl
    assert (
        "CREATE UNIQUE INDEX uq_executions_scope_sequence ON executions "
        "(wallet_public_id, exchange, mode, scope_sequence)" in ddl
    )
    assert "DROP CONSTRAINT ck_portfolio_spot_anchor_watermark" in ddl
    assert "source_watermark_kind = 'scope_sequence' AND source_watermark >= 0" in ddl
    assert "source_watermark_kind = 'execution_id' AND source_watermark >= 0" in ddl
    assert "DROP INDEX uq_executions_scope_sequence" in ddl
    assert "DROP COLUMN scope_sequence" in ddl
    assert "DROP COLUMN mode" in ddl
    assert "DROP COLUMN exchange" in ddl

    table = Execution.__table__
    assert isinstance(table.c.exchange.type, sa.String)
    assert isinstance(table.c.mode.type, sa.String)
    assert isinstance(table.c.scope_sequence.type, sa.BigInteger)
    assert table.c.exchange.nullable is False
    assert table.c.mode.nullable is False
    assert table.c.scope_sequence.nullable is False
    scope_index = next(
        index for index in table.indexes if index.name == "uq_executions_scope_sequence"
    )
    assert scope_index.unique is True
    assert "sqlite_where" not in scope_index.dialect_options["sqlite"]
    assert [column.name for column in scope_index.columns] == [
        "wallet_public_id",
        "exchange",
        "mode",
        "scope_sequence",
    ]
    assert {constraint.name for constraint in table.constraints if constraint.name is not None} >= {
        "ck_executions_exchange_lower",
        "ck_executions_mode",
        "ck_executions_scope_sequence",
    }


def test_0029_offline_sql_rendering_is_refused_with_zero_output() -> None:
    """Offline ``--sql`` rendering fails closed BEFORE emitting any DDL.

    The lineage pre-checks and the anchor-emptiness assert must read data;
    rendering DDL without running them would ship fabricated-scope risk —
    and rendering even PARTIAL DDL before refusing would hand the operator
    a half-schema script. Both directions must refuse with empty output.

    Given: The migration bound to an offline (as_sql) PostgreSQL context.
    When: ``upgrade`` and ``downgrade`` run without the data-read helpers
        patched out.
    Then: Each raises naming the online-connection requirement and the
        rendered SQL output is completely empty.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with patch.object(migration, "op", operations):
        with pytest.raises(RuntimeError, match="requires an online connection"):
            migration.upgrade()
        with pytest.raises(RuntimeError, match="requires an online connection"):
            migration.downgrade()
    assert output.getvalue() == ""
