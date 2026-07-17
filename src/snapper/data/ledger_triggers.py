"""Canonical DDL for the ``executions`` append-only immutability triggers.

The ``executions`` ledger is append-only at runtime: the fenced ingest
persists a fill with a single ``INSERT`` and no code path may UPDATE or
DELETE a committed row. The Python-layer ``_guard_immutable_ledger``
refuses the six generic ORM mutation primitives early and legibly, but a
Python target-parameter guard cannot reach raw ``text()`` SQL, a
``quoted_name(quote=False)`` identifier injection, or a ``psql`` /
``sqlite3`` console sharing the connection. The physical trigger installed
here is the theorem that closes that hole: on both dialects UPDATE and
DELETE (and, on PostgreSQL, TRUNCATE) are physically rejected, while
INSERT is left untouched so the ``max + 1`` fenced ingest keeps working
and gaps/duplicates stay caught by ``uq_executions_scope_sequence`` and
the counted-range proof.

Single source of truth: :func:`install_execution_immutability_triggers`
is invoked BOTH by the ``after_create`` DDL event on
``Execution.__table__`` (covering every ``create_all``-built database,
including the test and in-memory fixtures Alembic never touches) AND by
migration 0030 (covering the already-existing production table Alembic
built). Because both callers run the same function emitting the same
module-scope statement constants, the installed DDL is byte-identical by
construction, not by convention. The install is idempotent on both
dialects (SQLite ``IF NOT EXISTS``; PostgreSQL ``CREATE OR REPLACE
FUNCTION`` plus ``DROP TRIGGER IF EXISTS`` then ``CREATE TRIGGER``), so
re-running it against a database that already carries the triggers is a
safe no-op.

Doctrine note for future migrations: a SQLite
``batch_alter_table(recreate="always")`` rebuild of ``executions``
silently DROPS these triggers (they are not carried onto the rebuilt
table). Any future migration that recreates the SQLite ``executions``
table MUST re-install them as its final step by calling
:func:`install_execution_immutability_triggers` on its bind. This is the
reason the DDL lives in a shared module rather than being inlined in a
single migration.

The PostgreSQL function ``executions_reject_mutation()`` persists after a
``Base.metadata.drop_all`` (which drops the table and its triggers but not
the standalone function); this is harmless on throwaway databases and
migration 0030's downgrade drops it explicitly.
"""

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

_SQLITE_REJECT_UPDATE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS executions_reject_update "
    "BEFORE UPDATE ON executions BEGIN "
    "SELECT RAISE(ABORT, 'executions is append-only: UPDATE is physically "
    "forbidden; corrections enter as new appended events, never in-place "
    "revisions of a sealed scope_sequence'); END"
)
_SQLITE_REJECT_DELETE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS executions_reject_delete "
    "BEFORE DELETE ON executions BEGIN "
    "SELECT RAISE(ABORT, 'executions is append-only: DELETE is physically "
    "forbidden; removing a row would free a burnt scope_sequence and regress "
    "the sealed watermark'); END"
)
_SQLITE_DROP_UPDATE: TextClause = text("DROP TRIGGER IF EXISTS executions_reject_update")
_SQLITE_DROP_DELETE: TextClause = text("DROP TRIGGER IF EXISTS executions_reject_delete")

_PG_FUNCTION: TextClause = text(
    "CREATE OR REPLACE FUNCTION executions_reject_mutation() "
    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
    "RAISE EXCEPTION 'executions is append-only: % is physically forbidden; "
    "the sealed scope_sequence prefix is immutable and corrections enter as "
    "new appended events', TG_OP USING ERRCODE = 'raise_exception', "
    "TABLE = 'executions'; RETURN NULL; END; $$"
)
_PG_DROP_ROW_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS executions_reject_row_mutation ON executions"
)
_PG_CREATE_ROW_TRIGGER: TextClause = text(
    "CREATE TRIGGER executions_reject_row_mutation "
    "BEFORE UPDATE OR DELETE ON executions "
    "FOR EACH ROW EXECUTE FUNCTION executions_reject_mutation()"
)
_PG_ENABLE_ALWAYS_ROW_TRIGGER: TextClause = text(
    "ALTER TABLE executions ENABLE ALWAYS TRIGGER executions_reject_row_mutation"
)
_PG_DROP_TRUNCATE_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS executions_reject_truncate ON executions"
)
_PG_CREATE_TRUNCATE_TRIGGER: TextClause = text(
    "CREATE TRIGGER executions_reject_truncate "
    "BEFORE TRUNCATE ON executions "
    "FOR EACH STATEMENT EXECUTE FUNCTION executions_reject_mutation()"
)
_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER: TextClause = text(
    "ALTER TABLE executions ENABLE ALWAYS TRIGGER executions_reject_truncate"
)
_PG_DROP_FUNCTION: TextClause = text("DROP FUNCTION IF EXISTS executions_reject_mutation()")


def install_execution_immutability_triggers(connection: Connection) -> None:
    """Emit the dialect's ``executions`` append-only triggers on ``connection``.

    Idempotent on both dialects and the single source of truth so the
    ``after_create`` event (``create_all``-built databases) and migration
    0030 (the existing production table) install byte-identical DDL.
    SQLite installs one ``BEFORE UPDATE`` and one ``BEFORE DELETE`` trigger
    (there is no combined form); PostgreSQL installs the shared plpgsql
    function, a ``BEFORE UPDATE OR DELETE`` row trigger, and a ``BEFORE
    TRUNCATE`` statement trigger (a row-level truncate trigger is rejected
    by PostgreSQL, and a ``BEFORE DELETE`` row trigger does not fire on
    TRUNCATE, so the second trigger is required for physical immutability).

    Each PostgreSQL trigger is then promoted to ``ENABLE ALWAYS`` via
    ``ALTER TABLE ... ENABLE ALWAYS TRIGGER``. A plain (origin) trigger
    does NOT fire when the session sets ``session_replication_role =
    replica`` — a role the app's own connection can set — which would
    otherwise let a non-privileged app connection DELETE/UPDATE/TRUNCATE
    the ledger with the triggers silently disabled. ``ENABLE ALWAYS``
    makes the trigger fire under the replica role too, closing that
    session-wide bypass. SQLite has no session-role concept, so it needs
    no analogue.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_SQLITE_REJECT_UPDATE)
        connection.execute(_SQLITE_REJECT_DELETE)
        return
    connection.execute(_PG_FUNCTION)
    connection.execute(_PG_DROP_ROW_TRIGGER)
    connection.execute(_PG_CREATE_ROW_TRIGGER)
    connection.execute(_PG_ENABLE_ALWAYS_ROW_TRIGGER)
    connection.execute(_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_PG_CREATE_TRUNCATE_TRIGGER)
    connection.execute(_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER)


def drop_execution_immutability_triggers(connection: Connection) -> None:
    """Remove the ``executions`` append-only triggers from ``connection``.

    Idempotent on both dialects (every statement is ``IF EXISTS``). SQLite
    drops the two per-operation triggers; PostgreSQL drops both triggers
    and then the shared function, which no ``after_create`` path removes.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_SQLITE_DROP_UPDATE)
        connection.execute(_SQLITE_DROP_DELETE)
        return
    connection.execute(_PG_DROP_ROW_TRIGGER)
    connection.execute(_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_PG_DROP_FUNCTION)
