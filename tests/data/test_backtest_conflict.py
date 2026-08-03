"""Tests for is_single_running_conflict — dialect-aware unique-violation detection."""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from snapper.data.backtest_conflict import SINGLE_RUNNING_INDEX
from snapper.data.backtest_conflict import is_single_running_conflict

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


class _AsyncpgUniqueViolationError(Exception):
    """Stub mimicking asyncpg.exceptions.UniqueViolationError shape."""

    def __init__(self, *, constraint_name: str, sqlstate: str = "23505") -> None:
        super().__init__(f"unique violation on {constraint_name}")
        self.constraint_name = constraint_name
        self.sqlstate = sqlstate


class _PsycopgDiag:
    """Stub mimicking psycopg ``Diagnostic`` shape (constraint_name attr)."""

    def __init__(self, constraint_name: str | None) -> None:
        self.constraint_name = constraint_name


class _PsycopgUniqueViolationError(Exception):
    """Stub mimicking psycopg ``IntegrityError.orig`` exposing ``.diag``."""

    def __init__(self, *, constraint_name: str | None) -> None:
        super().__init__("psycopg unique violation")
        self.diag = _PsycopgDiag(constraint_name)


def _wrap(orig: Exception) -> IntegrityError:
    """Wrap an underlying driver exception in SA's IntegrityError."""
    return IntegrityError("stmt", {}, orig)


class TestExceptionShapes:
    """Direct shape detection — does not need a real DB."""

    def test_asyncpg_constraint_name_match_returns_true(self) -> None:
        """Asyncpg constraint_name match wins regardless of sqlstate."""
        exc = _wrap(_AsyncpgUniqueViolationError(constraint_name=SINGLE_RUNNING_INDEX))
        assert is_single_running_conflict(exc)

    def test_asyncpg_constraint_name_match_with_unrelated_sqlstate(self) -> None:
        """Constraint name alone is sufficient even with non-23505 sqlstate."""
        exc = _wrap(
            _AsyncpgUniqueViolationError(constraint_name=SINGLE_RUNNING_INDEX, sqlstate="99999")
        )
        assert is_single_running_conflict(exc)

    def test_asyncpg_other_constraint_returns_false(self) -> None:
        """Different constraint name → not our conflict."""
        exc = _wrap(_AsyncpgUniqueViolationError(constraint_name="some_other_unique"))
        assert not is_single_running_conflict(exc)

    def test_psycopg_diag_constraint_match_returns_true(self) -> None:
        """Psycopg legacy path (diag.constraint_name) is honoured."""
        exc = _wrap(_PsycopgUniqueViolationError(constraint_name=SINGLE_RUNNING_INDEX))
        assert is_single_running_conflict(exc)

    def test_psycopg_diag_other_constraint_returns_false(self) -> None:
        """Psycopg with a different constraint name → not our conflict."""
        exc = _wrap(_PsycopgUniqueViolationError(constraint_name="other_unique"))
        assert not is_single_running_conflict(exc)

    def test_sqlite_message_match_returns_true(self) -> None:
        """SQLite message-based fallback: 'UNIQUE constraint failed: backtest_runs.status'."""
        sqlite_exc = sa.exc.SQLAlchemyError("UNIQUE constraint failed: backtest_runs.status")
        exc = _wrap(sqlite_exc)
        assert is_single_running_conflict(exc)

    def test_sqlite_message_for_other_table_returns_false(self) -> None:
        """SQLite message referring to a different column is not our conflict."""
        sqlite_exc = sa.exc.SQLAlchemyError("UNIQUE constraint failed: orders.client_order_id")
        exc = _wrap(sqlite_exc)
        assert not is_single_running_conflict(exc)

    def test_orig_none_returns_false(self) -> None:
        """No underlying driver exception → not our conflict."""
        exc = IntegrityError("stmt", {}, Exception("opaque"))
        exc.orig = None
        assert not is_single_running_conflict(exc)


def _insert_running_row(engine: sa.Engine, public_id: str) -> None:
    """Insert a backtest_runs row with status=running at known_to=MAX."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO backtest_runs (public_id, session_id, sequence_id, "
                "timestamp, known_to, wallet_public_id, strategy_name, "
                "strategy_params, instrument_public_id, exchange, mode, timeframe, "
                "start_date, end_date, initial_cash, status, execution_mode, "
                "fill_model, slippage_bps, commission_bps) VALUES (:public_id, "
                "'sess', 1, '2026-01-01 00:00:00.000000', '9999-12-31 23:59:59.000000', "
                "'wallet', 'sma_cross', '{}', 'instr', 'kraken', 'paper', '1h', "
                "'2026-01-01 00:00:00.000000', '2026-01-01 00:00:00.000000', "
                "10000.0, 'running', 'direct_db', 'market', 0.0, 0.0)"
            ),
            {"public_id": public_id},
        )


@pytest.fixture
def migrated_db(migrated_db_path: Path) -> Iterator[sa.Engine]:
    """SQLite database upgraded through migration 0004 inclusive.

    The schema comes from the worker's session-scoped Alembic template,
    copied privately for this test.
    """
    engine = sa.create_engine(f"sqlite:///{migrated_db_path}", future=True)
    try:
        yield engine
    finally:
        engine.dispose()


class TestRealSqliteIntegrityError:
    """End-to-end: a real IntegrityError from migration 0004 is detected."""

    def test_real_sqlite_partial_index_violation_detected(self, migrated_db: sa.Engine) -> None:
        """Two running rows in real SQLite trigger detected conflict."""
        _insert_running_row(migrated_db, public_id="run-a")
        with pytest.raises(IntegrityError) as conflict:
            _insert_running_row(migrated_db, public_id="run-b")
        assert is_single_running_conflict(conflict.value)
