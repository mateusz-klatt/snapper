"""Detect the single-running-backtest conflict across SQL dialects.

This repo uses **asyncpg** (not psycopg) for PostgreSQL — see
``pyproject.toml asyncpg = "^0.31.0"``. ``asyncpg.exceptions.UniqueViolationError``
exposes the constraint name directly via ``.constraint_name`` and sqlstate
``23505``. psycopg's ``.diag.constraint_name`` access path is kept as
defence-in-depth so a future driver swap does not silently break detection.

SQLite via aiosqlite raises ``sqlite3.IntegrityError`` with message
``UNIQUE constraint failed: backtest_runs.status``. The index name is not
surfaced in the message; we accept the column-level match as proxy because
``backtest_runs.status`` has no other unique constraint in any migration
(0001/0002/0003/0004 audited).

The runner uses :func:`is_single_running_conflict` to distinguish a
"second runner racing into running" IntegrityError (which it converts
to a clean ``failed`` transition) from any unrelated unique violation
(which it re-raises so the caller sees the underlying bug).
"""

from sqlalchemy.exc import IntegrityError

SINGLE_RUNNING_INDEX = "uq_bt_single_running"
_SQLITE_MATCH = "UNIQUE constraint failed: backtest_runs.status"
_PG_UNIQUE_VIOLATION_SQLSTATE = "23505"


def is_single_running_conflict(exc: IntegrityError) -> bool:
    """Return True iff ``exc`` was raised by the uq_bt_single_running index.

    Uses duck-typing across exception shapes:

    - asyncpg native: ``orig.constraint_name == SINGLE_RUNNING_INDEX`` and/or
      ``orig.sqlstate == "23505"``.
    - psycopg legacy: ``orig.diag.constraint_name == SINGLE_RUNNING_INDEX``.
    - SQLite via aiosqlite: stringified ``orig`` contains the canonical
      column-level message; safe given the audit (no other UNIQUE
      constraint references backtest_runs.status).
    """
    orig = exc.orig
    if orig is None:
        return False
    constraint_name = getattr(orig, "constraint_name", None)
    if constraint_name == SINGLE_RUNNING_INDEX:
        return True
    sqlstate = getattr(orig, "sqlstate", None)
    if sqlstate == _PG_UNIQUE_VIOLATION_SQLSTATE and constraint_name == SINGLE_RUNNING_INDEX:
        return True
    diag = getattr(orig, "diag", None)
    if diag is not None and getattr(diag, "constraint_name", None) == SINGLE_RUNNING_INDEX:
        return True
    return _SQLITE_MATCH in str(orig)
