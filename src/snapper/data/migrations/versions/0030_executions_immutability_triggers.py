"""Install the DB-level append-only immutability triggers on ``executions``.

The runtime ledger is append-only: the fenced ingest persists a fill with
a single ``INSERT`` and no code path may UPDATE or DELETE a committed row.
The Python-layer ``_guard_immutable_ledger`` refuses the generic ORM
mutation primitives early, but it cannot reach raw ``text()`` SQL, a
``quoted_name(quote=False)`` identifier injection, or a console sharing
the connection. This migration installs the physical trigger that closes
that hole on the ALREADY-EXISTING production table (created by 0001,
extended by 0025 and 0029): on both dialects UPDATE and DELETE (and, on
PostgreSQL, TRUNCATE) are physically rejected while INSERT is left
untouched, so the ``max + 1`` fenced ingest keeps working and
gaps/duplicates stay caught by ``uq_executions_scope_sequence`` and the
counted-range proof.

Test and in-memory databases are built by ``Base.metadata.create_all``,
never by Alembic, so a migration-only trigger would be absent from them;
the trigger is therefore ALSO installed by an ``after_create`` DDL event
on ``Execution.__table__`` (see :mod:`snapper.data.ledger_triggers`). Both
callers invoke the SAME
:func:`install_execution_immutability_triggers`, so the emitted DDL is
byte-identical by construction. The two paths are mutually exclusive per
database (production is migration-built only; test databases are
``create_all``-built only), and both are idempotent, so no double-install
can occur.

Standalone and atomically reversible: it installs no schema shape of its
own, only the triggers (and, on PostgreSQL, the shared rejection
function), so ``downgrade`` drops them with no data dependency. Ordering
is deliberate — 0029 -> 0030 means the trigger exists only AFTER 0029's
backfill ``UPDATE`` has completed (installing it earlier would reject that
backfill), and a downgrade runs 0030-down (drop trigger) BEFORE 0029-down
(which on SQLite recreates the table). The online guard mirrors 0029:
``--sql`` offline rendering is refused because the install helper needs a
live bind, even though trigger install touches no rows.
Revises 0029.
"""

from collections.abc import Sequence

from alembic import op

from snapper.data.ledger_triggers import drop_execution_immutability_triggers
from snapper.data.ledger_triggers import install_execution_immutability_triggers

revision: str = "0030"
down_revision: str | None = "0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online_bind() -> None:
    """Refuse offline ``--sql`` rendering before touching the schema.

    The install helper executes real trigger DDL against a live bind and
    branches on the connection's dialect; offline rendering has neither.
    Mirrors 0029's online guard so an offline invocation refuses with zero
    rendered output.
    """
    if op.get_context().as_sql:
        raise RuntimeError(
            "migration 0030 requires an online connection: it installs "
            "dialect-specific trigger DDL through a live bind"
        )


def upgrade() -> None:
    """Install the dual-dialect append-only triggers on ``executions``."""
    _require_online_bind()
    install_execution_immutability_triggers(op.get_bind())


def downgrade() -> None:
    """Drop the append-only triggers (and the PostgreSQL rejection function)."""
    _require_online_bind()
    drop_execution_immutability_triggers(op.get_bind())
