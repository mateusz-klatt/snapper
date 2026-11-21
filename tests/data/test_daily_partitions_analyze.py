"""Unit and opt-in PostgreSQL contracts for daily-partition planner statistics ANALYZE."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC
from datetime import date
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError

import snapper.data.daily_partitions as lifecycle

_ANCHOR = datetime(2026, 8, 2, tzinfo=UTC)
_YESTERDAY_CANDLES = "candles_d20260801"
_TODAY_DATE = date(2026, 8, 2)


class _SqlStateError(RuntimeError):
    """Minimal DBAPI payload carrying one explicit five-character SQLSTATE."""

    def __init__(self, sqlstate: str) -> None:
        """Bind a deterministic PostgreSQL SQLSTATE code.

        Args:
            sqlstate: Five-character SQLSTATE string.
        """
        super().__init__(f"simulated failure {sqlstate}")
        self.sqlstate = sqlstate


def _postgresql_double() -> tuple[MagicMock, list[str]]:
    """Build a PostgreSQL connection double that records executed SQL text.

    Returns:
        Connection double and the ordered list of executed SQL strings.
    """
    executed_sql: list[str] = []
    connection = MagicMock(spec=Connection)
    connection.in_transaction.return_value = False
    connection.dialect = SimpleNamespace(name="postgresql")
    connection.scalar.return_value = "public"

    @contextmanager
    def _begin() -> Generator[None]:
        """Simulate a clean SQLAlchemy transaction block."""
        yield

    def _record_execute(statement: object, *_args: object, **_kwargs: object) -> MagicMock:
        """Capture SQL text emitted through ``sqlalchemy.text``."""
        executed_sql.append(str(statement))
        return MagicMock()

    connection.begin.side_effect = _begin
    connection.execute.side_effect = _record_execute
    return connection, executed_sql


def _configured_postgresql_url() -> str | None:
    """Return the opt-in live PostgreSQL URL or ``None`` for default SQLite runs.

    Returns:
        Configured PostgreSQL URL string, or ``None`` when unset or non-PG.
    """
    database_url = os.environ.get("PNL_TEST_POSTGRES_URL")
    if database_url is None:
        return None
    try:
        if make_url(database_url).get_backend_name() == "postgresql":
            return database_url
    except ArgumentError:
        return None
    return None


def _sync_postgresql_url(database_url: str) -> str:
    """Normalize an async PostgreSQL URL to the synchronous psycopg2 driver.

    Args:
        database_url: Configured PostgreSQL SQLAlchemy URL.

    Returns:
        Equivalent URL using ``postgresql+psycopg2://``.
    """
    if database_url.startswith("postgresql+asyncpg://"):
        return database_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    return database_url


def test_sqlite_dialect_is_a_clean_noop_for_rollover_and_stale_analyze() -> None:
    """SQLite dev/test engines must skip PostgreSQL ANALYZE cleanly with zero SQL.

    Given: A real in-memory SQLite SQLAlchemy connection.
    When: Both ``analyze_rollover_targets`` and ``analyze_stale_relations`` run
        in dry-run and apply modes.
    Then: Each call returns ``skipped_dialect='sqlite'`` with empty targets and
        statements, and executes zero database work.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    try:
        with engine.connect() as connection:
            for dry_run in (True, False):
                rollover = lifecycle.analyze_rollover_targets(
                    connection,
                    "candles",
                    _ANCHOR,
                    dry_run=dry_run,
                )
                stale = lifecycle.analyze_stale_relations(
                    connection,
                    limit=50,
                    dry_run=dry_run,
                )
                assert rollover == lifecycle.AnalyzeResult(
                    targets=(),
                    statements=(),
                    executed=(),
                    skipped=(),
                    skipped_dialect="sqlite",
                )
                assert stale == lifecycle.AnalyzeResult(
                    targets=(),
                    statements=(),
                    executed=(),
                    skipped=(),
                    skipped_dialect="sqlite",
                )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("relation", "expected"),
    [
        ("candles_d20260801", date(2026, 8, 1)),
        ("venue_account_observations", None),
        ("candles_default", None),
        ("candles_d20269999", None),
    ],
)
def test_parse_daily_suffix_day_handles_valid_missing_and_invalid_dates(
    relation: str,
    expected: date | None,
) -> None:
    """Daily suffix date parsing must ignore unrelated or calendar-invalid names.

    Args:
        relation: Candidate catalog relation name.
        expected: Parsed UTC date or ``None`` when not a valid daily suffix.

    Given: Valid daily, ordinary, default, and invalid-calendar relation names.
    When: ``_parse_daily_suffix_day`` extracts any trailing ``_dYYYYMMDD`` date.
    Then: Only valid calendar dates return a ``date`` object.
    """
    assert lifecycle._parse_daily_suffix_day(relation) == expected


def test_analyze_rollover_targets_plans_and_executes_only_parent_and_yesterday_leaf() -> None:
    """Partition rollover ANALYZE must run non-recursive ONLY on the parent after the leaf.

    Given: A partitioned ``candles`` table with yesterday's closed leaf attached.
    When: ``analyze_rollover_targets`` runs in dry-run then apply mode.
    Then: Dry-run plans ``candles_d20260801`` then ``ONLY "public"."candles"`` without
        opening a write transaction, and apply runs each in its own short transaction
        with bounded timeouts (60s leaf, 300s parent).
    """
    connection, executed_sql = _postgresql_double()
    connection.scalar.side_effect = ["public", "p", "public", "p"]
    attached = (
        lifecycle.PartitionRef(
            name=_YESTERDAY_CANDLES,
            bound="FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')",
        ),
    )

    with patch.object(lifecycle, "_direct_partitions", return_value=attached):
        planned = lifecycle.analyze_rollover_targets(
            cast(Connection, connection),
            "candles",
            _ANCHOR,
            dry_run=True,
        )
        applied = lifecycle.analyze_rollover_targets(
            cast(Connection, connection),
            "candles",
            _ANCHOR,
            dry_run=False,
        )

    expected_statements = (
        'ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"',
        'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',
    )
    assert planned == lifecycle.AnalyzeResult(
        targets=("candles_d20260801", "candles"),
        statements=expected_statements,
        executed=(),
        skipped=(),
    )
    assert applied == lifecycle.AnalyzeResult(
        targets=("candles_d20260801", "candles"),
        statements=expected_statements,
        executed=("candles_d20260801", "candles"),
        skipped=(),
    )
    assert connection.begin.call_count == 2
    assert executed_sql == [
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL lock_timeout = '2s'",
        "SET LOCAL statement_timeout = '60s'",
        'ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"',
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL lock_timeout = '2s'",
        "SET LOCAL statement_timeout = '300s'",
        'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',
    ]


def test_analyze_rollover_targets_handles_absent_yesterday_ordinary_and_invalid_states() -> None:
    """Rollover ANALYZE must tolerate absent leaves, ordinary tables, and refuse missing ones.

    Given: Partitioned ``ticks`` with no yesterday leaf, ordinary ``trades``,
        and missing ``ticks``.
    When: ``analyze_rollover_targets`` plans or rejects each relation.
    Then: Absent yesterday still targets ``ONLY`` parent, ordinary omits ``ONLY``
        using 60s timeout, and missing/default-anchor resolution behaves cleanly.
    """
    connection, executed_sql = _postgresql_double()
    connection.scalar.side_effect = ["public", "p", "public", "r", "public", None]

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=()),
        patch.object(lifecycle, "current_utc_anchor", return_value=_ANCHOR) as default_anchor,
    ):
        parent_only = lifecycle.analyze_rollover_targets(
            cast(Connection, connection),
            "ticks",
            dry_run=True,
        )
    default_anchor.assert_called_once_with()
    assert parent_only.targets == ("ticks",)
    assert parent_only.statements == ('ANALYZE (SKIP_LOCKED) ONLY "public"."ticks"',)

    ordinary = lifecycle.analyze_rollover_targets(
        cast(Connection, connection),
        "trades",
        _ANCHOR,
        dry_run=False,
    )
    assert ordinary.targets == ("trades",)
    assert ordinary.statements == ('ANALYZE (SKIP_LOCKED) "public"."trades"',)
    assert ordinary.executed == ("trades",)
    assert "SET LOCAL statement_timeout = '60s'" in executed_sql

    with pytest.raises(lifecycle.DailyPartitionError, match="cannot be analyzed"):
        lifecycle.analyze_rollover_targets(
            cast(Connection, connection),
            "ticks",
            _ANCHOR,
            dry_run=True,
        )


def test_analyze_stale_relations_filters_future_leaves_and_orders_by_priority() -> None:
    """Stale sweep must ignore future daily leaves without relation locks and sort deterministically.

    Given: Stale catalog rows across parents, ordinary tables, past/today daily
        leaves, non-date partition children, and future daily leaves (> today UTC).
    When: ``analyze_stale_relations`` plans with an explicit limit.
    Then: Discovery runs the lock-free ``pg_stat_user_tables`` NULL query, drops
        future daily leaves, ranks parents -> ordinary -> newest daily leaves ->
        remaining partitions, rolls back before returning, and enforces ``limit``.
    """
    connection, _executed_sql = _postgresql_double()
    catalog_rows = [
        ("candles_d20260805", "r", True),
        ("ticks_d20260731", "r", True),
        ("venue_account_observations", "r", False),
        ("candles_default", "r", True),
        ("candles", "p", False),
        ("candles_d20260802", "r", True),
        ("candles_d20260801", "r", True),
        ("portfolio_reconciliation_observations", "r", False),
        ("ticks", "p", False),
    ]
    execute_result = MagicMock()
    execute_result.all.return_value = catalog_rows
    connection.execute.side_effect = [execute_result]

    with patch.object(lifecycle, "current_utc_anchor", return_value=_ANCHOR):
        result = lifecycle.analyze_stale_relations(
            cast(Connection, connection),
            limit=7,
            dry_run=True,
        )

    sql_issued = str(connection.execute.call_args.args[0])
    assert "pg_stat_user_tables" in sql_issued
    assert "s.last_analyze IS NULL" in sql_issued
    assert "s.last_autoanalyze IS NULL" in sql_issued
    assert result.targets == (
        "candles",
        "ticks",
        "portfolio_reconciliation_observations",
        "venue_account_observations",
        "candles_d20260802",
        "candles_d20260801",
        "ticks_d20260731",
    )
    assert result.statements == (
        'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',
        'ANALYZE (SKIP_LOCKED) ONLY "public"."ticks"',
        'ANALYZE (SKIP_LOCKED) "public"."portfolio_reconciliation_observations"',
        'ANALYZE (SKIP_LOCKED) "public"."venue_account_observations"',
        'ANALYZE (SKIP_LOCKED) "public"."candles_d20260802"',
        'ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"',
        'ANALYZE (SKIP_LOCKED) "public"."ticks_d20260731"',
    )
    connection.rollback.assert_called_once_with()

    full_result = lifecycle._select_stale_targets(
        catalog_rows,
        today=_TODAY_DATE,
        limit=20,
    )
    assert full_result[-1] == lifecycle._AnalyzeTarget("candles_default", is_parent=False)


def test_analyze_stale_relations_executes_empty_noop_and_validates_inputs() -> None:
    """Stale sweep validation and empty result set must reject invalid limits and succeed empty.

    Given: Invalid ``limit=0``, active transaction, non-public schema, and a clean
        empty catalog match under ``dry_run=False``.
    When: Calling ``analyze_stale_relations``.
    Then: Validation failures raise cleanly and empty matches return empty tuples.
    """
    connection, _executed_sql = _postgresql_double()

    with pytest.raises(ValueError, match="limit must be >= 1"):
        lifecycle.analyze_stale_relations(cast(Connection, connection), limit=0)

    connection.in_transaction.return_value = True
    with pytest.raises(lifecycle.DailyPartitionError, match="no active transaction"):
        lifecycle.analyze_stale_relations(cast(Connection, connection))

    connection.in_transaction.return_value = False
    connection.scalar.return_value = "analytics"
    with pytest.raises(lifecycle.DailyPartitionError, match="expected 'public'"):
        lifecycle.analyze_stale_relations(cast(Connection, connection))

    connection.scalar.return_value = "public"
    empty_result = MagicMock()
    empty_result.all.return_value = []
    connection.execute.side_effect = [empty_result]

    outcome = lifecycle.analyze_stale_relations(
        cast(Connection, connection),
        today=_TODAY_DATE,
        dry_run=False,
    )
    assert outcome == lifecycle.AnalyzeResult(
        targets=(),
        statements=(),
        executed=(),
        skipped=(),
    )
    assert connection.begin.call_count == 0


def test_run_analyze_targets_skips_timeouts_warns_and_raises_other_db_errors() -> None:
    """Timeout SQLSTATEs 55P03/57014 must skip and continue while other errors abort.

    The module logger is replaced by a spy instead of using ``caplog`` because
    other tests in the same xdist worker can reconfigure logging propagation,
    which left ``caplog.text`` empty under ``make check-all``.

    Given: Three targets where target 1 hits ``55P03``, target 2 hits ``57014``,
        and target 3 succeeds; plus a separate case hitting ``42P01``.
    When: ``_run_analyze_targets`` executes with ``dry_run=False``.
    Then: The timeout targets land in ``skipped`` with one warning each naming
        their SQLSTATE, target 3 commits in ``executed``, and ``42P01`` raises
        ``DailyPartitionError``.
    """
    connection, _executed_sql = _postgresql_double()
    targets = (
        lifecycle._AnalyzeTarget("candles_d20260801", is_parent=False),
        lifecycle._AnalyzeTarget("venue_account_observations", is_parent=False),
        lifecycle._AnalyzeTarget("candles", is_parent=True),
    )

    def _timeout_on_first_two(statement: object, *_args: object, **_kwargs: object) -> MagicMock:
        """Fail first two ANALYZE statements with lock/statement timeout codes."""
        sql_text = str(statement)
        if '"candles_d20260801"' in sql_text:
            raise DBAPIError(sql_text, None, _SqlStateError("55P03"))
        if '"venue_account_observations"' in sql_text:
            raise DBAPIError(sql_text, None, _SqlStateError("57014"))
        return MagicMock()

    connection.execute.side_effect = _timeout_on_first_two
    with patch.object(lifecycle, "logger") as logger_spy:
        outcome = lifecycle._run_analyze_targets(
            cast(Connection, connection),
            targets,
            dry_run=False,
        )

    assert outcome.executed == ("candles",)
    assert outcome.skipped == ("candles_d20260801", "venue_account_observations")
    warned = [call.args for call in logger_spy.warning.call_args_list]
    assert [(args[1], args[2]) for args in warned] == [
        ("candles_d20260801", "55P03"),
        ("venue_account_observations", "57014"),
    ]

    def _unexpected_error(statement: object, *_args: object, **_kwargs: object) -> MagicMock:
        """Raise an undefined-table error on the ANALYZE statement itself."""
        sql_text = str(statement)
        if sql_text.startswith("ANALYZE"):
            raise DBAPIError(sql_text, None, _SqlStateError("42P01"))
        return MagicMock()

    connection.execute.side_effect = _unexpected_error
    with pytest.raises(lifecycle.DailyPartitionError, match="ANALYZE failed"):
        lifecycle._run_analyze_targets(
            cast(Connection, connection),
            (lifecycle._AnalyzeTarget("candles", is_parent=True),),
            dry_run=False,
        )


@pytest.mark.skipif(
    _configured_postgresql_url() is None,
    reason="live PostgreSQL ANALYZE proof requires PNL_TEST_POSTGRES_URL",
)
def test_postgresql_live_stale_analyze_populates_stats_and_becomes_noop() -> None:
    """Live PG18 ANALYZE must populate parent, past leaf, and ordinary stats and skip future leaves.

    Given: Uniquely named throwaway partitioned table (with one past leaf containing
        rows and one empty future leaf) plus one ordinary table with reset counters.
    When: ``analyze_stale_relations`` executes twice with ``dry_run=False``.
    Then: The first run sets ``last_analyze`` on the parent, past leaf, and ordinary
        table while leaving the future leaf unanalyzed, and the second run treats all
        three objects as already fresh.
    """
    database_url = _configured_postgresql_url()
    assert database_url is not None
    engine = create_engine(_sync_postgresql_url(database_url), future=True)
    suffix = uuid4().hex[:12]
    parent_name = f"tmp_analyze_parent_{suffix}"
    past_leaf_name = f"{parent_name}_d20261119"
    future_leaf_name = f"{parent_name}_d20990101"
    ordinary_name = f"tmp_analyze_ordinary_{suffix}"

    try:
        with engine.connect() as setup_conn:
            with setup_conn.begin():
                setup_conn.execute(
                    text(
                        f"CREATE TABLE public.{parent_name} "
                        "(id BIGINT GENERATED ALWAYS AS IDENTITY, open_at TIMESTAMPTZ NOT NULL) "
                        "PARTITION BY RANGE (open_at)"
                    )
                )
                setup_conn.execute(
                    text(
                        f"CREATE TABLE public.{past_leaf_name} PARTITION OF public.{parent_name} "
                        "FOR VALUES FROM ('2026-11-19 00:00:00+00') "
                        "TO ('2026-11-20 00:00:00+00')"
                    )
                )
                setup_conn.execute(
                    text(
                        f"CREATE TABLE public.{future_leaf_name} PARTITION OF public.{parent_name} "
                        "FOR VALUES FROM ('2099-01-01 00:00:00+00') "
                        "TO ('2099-01-02 00:00:00+00')"
                    )
                )
                setup_conn.execute(
                    text(
                        f"CREATE TABLE public.{ordinary_name} "
                        "(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val INT NOT NULL)"
                    )
                )
                setup_conn.execute(
                    text(
                        f"INSERT INTO public.{parent_name} (open_at) VALUES "
                        "('2026-11-19 01:00:00+00'), "
                        "('2026-11-19 02:00:00+00'), "
                        "('2026-11-19 03:00:00+00')"
                    )
                )
                setup_conn.execute(
                    text(f"INSERT INTO public.{ordinary_name} (val) VALUES (1), (2), (3)")
                )
                for relation in (parent_name, past_leaf_name, future_leaf_name, ordinary_name):
                    setup_conn.execute(
                        text("SELECT pg_stat_reset_single_table_counters(CAST(:rel AS regclass))"),
                        {"rel": f"public.{relation}"},
                    )

            first = lifecycle.analyze_stale_relations(setup_conn, limit=200, dry_run=False)
            assert parent_name in first.executed
            assert ordinary_name in first.executed
            assert past_leaf_name in first.executed
            assert future_leaf_name not in first.targets

            rows = setup_conn.execute(
                text(
                    "SELECT relname, last_analyze IS NOT NULL "
                    "FROM pg_stat_user_tables "
                    "WHERE schemaname = 'public' AND relname IN :names"
                ),
                {"names": (parent_name, past_leaf_name, future_leaf_name, ordinary_name)},
            ).all()
            setup_conn.rollback()
            analyzed_map = {str(row[0]): bool(row[1]) for row in rows}
            assert analyzed_map[parent_name] is True
            assert analyzed_map[past_leaf_name] is True
            assert analyzed_map[ordinary_name] is True
            assert analyzed_map[future_leaf_name] is False

            second = lifecycle.analyze_stale_relations(setup_conn, limit=200, dry_run=False)
            for analyzed_rel in (parent_name, past_leaf_name, ordinary_name):
                assert analyzed_rel not in second.targets
                assert analyzed_rel not in second.executed
    finally:
        with engine.connect() as cleanup_conn, cleanup_conn.begin():
            cleanup_conn.execute(text(f"DROP TABLE IF EXISTS public.{parent_name} CASCADE"))
            cleanup_conn.execute(text(f"DROP TABLE IF EXISTS public.{ordinary_name} CASCADE"))
        engine.dispose()
