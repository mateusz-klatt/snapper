"""Canonical DDL for the append-only execution-ledger immutability triggers.

Three tables share this module because they share one doctrine: ``executions``
(the ledger of committed fills), ``execution_annulments`` (the manifest of
uniquely targeted operator corrections to that ledger), and
``execution_annulment_visibility`` (the observations proving when each
correction became durable). Each gets its own installer emitting its own
bespoke refusal messages; none is a generic "immutable table" helper, because
the harm each refusal prevents is different and the message is the operator's
first diagnostic.

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
migration 0030's downgrade drops it explicitly. The same holds for
``execution_annulments_reject_mutation()`` and migration 0037, and for
``execution_annulment_visibility_reject_mutation()`` and migration 0039.

The annulment and visibility installers are exact mirrors of the executions one: same
``after_create``-plus-migration dual install through a single function, same
SQLite ``BEFORE UPDATE`` / ``BEFORE DELETE`` pair, same PostgreSQL function plus
row trigger plus statement TRUNCATE trigger, and the same ``ENABLE ALWAYS``
promotion so ``session_replication_role = replica`` cannot disable it. The
SQLite ``REPLACE``-bypass vector is closed for all three tables by the connect-time
``PRAGMA recursive_triggers=ON`` in
:data:`snapper.data.repository._SQLITE_CONNECT_PRAGMAS`, which makes the delete
that ``INSERT OR REPLACE`` performs fire the ``BEFORE DELETE`` trigger.
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

_ANNULMENT_SQLITE_REJECT_UPDATE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS execution_annulments_reject_update "
    "BEFORE UPDATE ON execution_annulments BEGIN "
    "SELECT RAISE(ABORT, 'execution_annulments is append-only: UPDATE is "
    "physically forbidden; a correction manifest that could be re-pointed, "
    "re-reasoned, or silently un-annulled would not be evidence'); END"
)
_ANNULMENT_SQLITE_REJECT_DELETE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS execution_annulments_reject_delete "
    "BEFORE DELETE ON execution_annulments BEGIN "
    "SELECT RAISE(ABORT, 'execution_annulments is append-only: DELETE is "
    "physically forbidden; removing a correction would restore a repudiated "
    "execution to effective accounting history with no trace'); END"
)
_ANNULMENT_SQLITE_DROP_UPDATE: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulments_reject_update"
)
_ANNULMENT_SQLITE_DROP_DELETE: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulments_reject_delete"
)

_ANNULMENT_PG_FUNCTION: TextClause = text(
    "CREATE OR REPLACE FUNCTION execution_annulments_reject_mutation() "
    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
    "RAISE EXCEPTION 'execution_annulments is append-only: % is physically "
    "forbidden; the correction manifest is evidence and a revisable manifest "
    "proves nothing', TG_OP USING ERRCODE = 'raise_exception', "
    "TABLE = 'execution_annulments'; RETURN NULL; END; $$"
)
_ANNULMENT_PG_DROP_ROW_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulments_reject_row_mutation ON execution_annulments"
)
_ANNULMENT_PG_CREATE_ROW_TRIGGER: TextClause = text(
    "CREATE TRIGGER execution_annulments_reject_row_mutation "
    "BEFORE UPDATE OR DELETE ON execution_annulments "
    "FOR EACH ROW EXECUTE FUNCTION execution_annulments_reject_mutation()"
)
_ANNULMENT_PG_ENABLE_ALWAYS_ROW_TRIGGER: TextClause = text(
    "ALTER TABLE execution_annulments "
    "ENABLE ALWAYS TRIGGER execution_annulments_reject_row_mutation"
)
_ANNULMENT_PG_DROP_TRUNCATE_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulments_reject_truncate ON execution_annulments"
)
_ANNULMENT_PG_CREATE_TRUNCATE_TRIGGER: TextClause = text(
    "CREATE TRIGGER execution_annulments_reject_truncate "
    "BEFORE TRUNCATE ON execution_annulments "
    "FOR EACH STATEMENT EXECUTE FUNCTION execution_annulments_reject_mutation()"
)
_ANNULMENT_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER: TextClause = text(
    "ALTER TABLE execution_annulments ENABLE ALWAYS TRIGGER execution_annulments_reject_truncate"
)
_ANNULMENT_PG_DROP_FUNCTION: TextClause = text(
    "DROP FUNCTION IF EXISTS execution_annulments_reject_mutation()"
)

_VISIBILITY_SQLITE_REJECT_UPDATE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS execution_annulment_visibility_reject_update "
    "BEFORE UPDATE ON execution_annulment_visibility BEGIN "
    "SELECT RAISE(ABORT, 'execution_annulment_visibility is append-only: UPDATE "
    "is physically forbidden; an observation whose instant could be moved would "
    "let a correction claim it was knowable before it was durable'); END"
)
_VISIBILITY_SQLITE_REJECT_DELETE: TextClause = text(
    "CREATE TRIGGER IF NOT EXISTS execution_annulment_visibility_reject_delete "
    "BEFORE DELETE ON execution_annulment_visibility BEGIN "
    "SELECT RAISE(ABORT, 'execution_annulment_visibility is append-only: DELETE "
    "is physically forbidden; removing an observation would retract a "
    "correction from history that has already been reported with it'); END"
)
_VISIBILITY_SQLITE_DROP_UPDATE: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_update"
)
_VISIBILITY_SQLITE_DROP_DELETE: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_delete"
)

_VISIBILITY_PG_FUNCTION: TextClause = text(
    "CREATE OR REPLACE FUNCTION execution_annulment_visibility_reject_mutation() "
    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
    "RAISE EXCEPTION 'execution_annulment_visibility is append-only: % is "
    "physically forbidden; a revisable observation proves nothing about when a "
    "correction became durable', TG_OP USING ERRCODE = 'raise_exception', "
    "TABLE = 'execution_annulment_visibility'; RETURN NULL; END; $$"
)
_VISIBILITY_PG_DROP_ROW_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_row_mutation "
    "ON execution_annulment_visibility"
)
_VISIBILITY_PG_CREATE_ROW_TRIGGER: TextClause = text(
    "CREATE TRIGGER execution_annulment_visibility_reject_row_mutation "
    "BEFORE UPDATE OR DELETE ON execution_annulment_visibility "
    "FOR EACH ROW EXECUTE FUNCTION execution_annulment_visibility_reject_mutation()"
)
_VISIBILITY_PG_ENABLE_ALWAYS_ROW_TRIGGER: TextClause = text(
    "ALTER TABLE execution_annulment_visibility "
    "ENABLE ALWAYS TRIGGER execution_annulment_visibility_reject_row_mutation"
)
_VISIBILITY_PG_DROP_TRUNCATE_TRIGGER: TextClause = text(
    "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_truncate "
    "ON execution_annulment_visibility"
)
_VISIBILITY_PG_CREATE_TRUNCATE_TRIGGER: TextClause = text(
    "CREATE TRIGGER execution_annulment_visibility_reject_truncate "
    "BEFORE TRUNCATE ON execution_annulment_visibility "
    "FOR EACH STATEMENT EXECUTE FUNCTION execution_annulment_visibility_reject_mutation()"
)
_VISIBILITY_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER: TextClause = text(
    "ALTER TABLE execution_annulment_visibility "
    "ENABLE ALWAYS TRIGGER execution_annulment_visibility_reject_truncate"
)
_VISIBILITY_PG_DROP_FUNCTION: TextClause = text(
    "DROP FUNCTION IF EXISTS execution_annulment_visibility_reject_mutation()"
)


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


def install_execution_annulment_immutability_triggers(connection: Connection) -> None:
    """Emit the dialect's ``execution_annulments`` append-only triggers.

    The exact mirror of :func:`install_execution_immutability_triggers` for the
    correction manifest, and the single source of truth shared by the
    ``after_create`` DDL event on ``ExecutionAnnulment.__table__`` (every
    ``create_all``-built database, including test fixtures Alembic never
    touches) and migration 0037 (the Alembic-built production table), so both
    installs are byte-identical by construction rather than by convention.

    The manifest needs the same physical refusal as the ledger it corrects, for
    the same reason: an operator correction that could later be UPDATEd to point
    at a different execution, re-reasoned, or DELETEd to silently restore a
    repudiated fill to effective accounting history is not evidence. Because the
    manifest also has no lifecycle at all — its rows are inserted once and never
    close — the refusal is total rather than a bitemporal close guard.

    Idempotent on both dialects (SQLite ``IF NOT EXISTS``; PostgreSQL ``CREATE OR
    REPLACE FUNCTION`` plus ``DROP TRIGGER IF EXISTS`` then ``CREATE TRIGGER``).
    SQLite installs one ``BEFORE UPDATE`` and one ``BEFORE DELETE`` trigger;
    PostgreSQL installs the shared plpgsql function, a ``BEFORE UPDATE OR
    DELETE`` row trigger, and a ``BEFORE TRUNCATE`` statement trigger (a
    ``BEFORE DELETE`` row trigger does not fire on TRUNCATE), each promoted to
    ``ENABLE ALWAYS`` so it still fires under ``session_replication_role =
    replica``.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_ANNULMENT_SQLITE_REJECT_UPDATE)
        connection.execute(_ANNULMENT_SQLITE_REJECT_DELETE)
        return
    connection.execute(_ANNULMENT_PG_FUNCTION)
    connection.execute(_ANNULMENT_PG_DROP_ROW_TRIGGER)
    connection.execute(_ANNULMENT_PG_CREATE_ROW_TRIGGER)
    connection.execute(_ANNULMENT_PG_ENABLE_ALWAYS_ROW_TRIGGER)
    connection.execute(_ANNULMENT_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_ANNULMENT_PG_CREATE_TRUNCATE_TRIGGER)
    connection.execute(_ANNULMENT_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER)


def drop_execution_annulment_immutability_triggers(connection: Connection) -> None:
    """Remove the ``execution_annulments`` append-only triggers.

    Idempotent on both dialects (every statement is ``IF EXISTS``). SQLite drops
    the two per-operation triggers; PostgreSQL drops both triggers and then the
    shared function, which no ``after_create`` path removes and which survives a
    ``Base.metadata.drop_all``.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_ANNULMENT_SQLITE_DROP_UPDATE)
        connection.execute(_ANNULMENT_SQLITE_DROP_DELETE)
        return
    connection.execute(_ANNULMENT_PG_DROP_ROW_TRIGGER)
    connection.execute(_ANNULMENT_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_ANNULMENT_PG_DROP_FUNCTION)


def install_execution_annulment_visibility_immutability_triggers(
    connection: Connection,
) -> None:
    """Emit the dialect's ``execution_annulment_visibility`` append-only triggers.

    The third member of the same family, installed from the same shared source
    the ``after_create`` DDL event on
    ``ExecutionAnnulmentVisibility.__table__`` and migration 0039 both call, so
    the ``create_all``-built and Alembic-built schemas are byte-identical by
    construction rather than by convention.

    This table needs the refusal MORE sharply than the two it serves, not less.
    An observation row is the proof that a correction was durable at a stated
    instant; a mutable ``observed_at`` would let that instant be moved earlier,
    which is precisely the claim the visibility ledger exists to make
    unforgeable — a correction folded into a historical answer it was not
    durable for. A deletable observation is the mirror hazard: it would retract
    a correction from history that has already been reported with it, silently
    changing numbers a reader has seen. Like the manifest, these rows have no
    lifecycle at all, so the refusal is total rather than a close guard.

    Idempotent on both dialects, with the same statement shapes the manifest
    installer uses.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_VISIBILITY_SQLITE_REJECT_UPDATE)
        connection.execute(_VISIBILITY_SQLITE_REJECT_DELETE)
        return
    connection.execute(_VISIBILITY_PG_FUNCTION)
    connection.execute(_VISIBILITY_PG_DROP_ROW_TRIGGER)
    connection.execute(_VISIBILITY_PG_CREATE_ROW_TRIGGER)
    connection.execute(_VISIBILITY_PG_ENABLE_ALWAYS_ROW_TRIGGER)
    connection.execute(_VISIBILITY_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_VISIBILITY_PG_CREATE_TRUNCATE_TRIGGER)
    connection.execute(_VISIBILITY_PG_ENABLE_ALWAYS_TRUNCATE_TRIGGER)


def drop_execution_annulment_visibility_immutability_triggers(
    connection: Connection,
) -> None:
    """Remove the ``execution_annulment_visibility`` append-only triggers.

    Idempotent on both dialects (every statement is ``IF EXISTS``). SQLite drops
    the two per-operation triggers; PostgreSQL drops both triggers and then the
    shared function, which no ``after_create`` path removes and which survives a
    ``Base.metadata.drop_all``.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
    """
    if connection.dialect.name == "sqlite":
        connection.execute(_VISIBILITY_SQLITE_DROP_UPDATE)
        connection.execute(_VISIBILITY_SQLITE_DROP_DELETE)
        return
    connection.execute(_VISIBILITY_PG_DROP_ROW_TRIGGER)
    connection.execute(_VISIBILITY_PG_DROP_TRUNCATE_TRIGGER)
    connection.execute(_VISIBILITY_PG_DROP_FUNCTION)
