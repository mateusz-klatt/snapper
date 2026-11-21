"""CLI surface contracts for ``snapper daily-partitions analyze`` and post-ensure ANALYZE."""

from datetime import UTC
from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from typer.testing import CliRunner

import snapper.cli.daily_partitions as daily_partitions_cli
from snapper.cli.daily_partitions import daily_partitions_app
from snapper.data.daily_partitions import AnalyzeResult
from snapper.data.daily_partitions import DailyPartitionError
from snapper.data.daily_partitions import LifecycleAction
from snapper.data.daily_partitions import LifecycleResult

_ANCHOR = "2026-08-02T00:00:00+00:00"
_ANCHOR_VALUE = datetime(2026, 8, 2, tzinfo=UTC)


def _engine_double() -> tuple[MagicMock, MagicMock]:
    """Build mocked SQLAlchemy engine and connection context for CLI tests.

    Returns:
        Engine and connection doubles wired via ``engine.connect()``.
    """
    engine = MagicMock(spec=Engine)
    connection = MagicMock(spec=Connection)
    engine.connect.return_value.__enter__.return_value = connection
    return engine, connection


def test_render_analyze_result_formats_sqlite_noop_and_pg_summary() -> None:
    """Renderer must distinguish non-PostgreSQL skips from planned/executed/skipped relations.

    Given: One SQLite no-op ``AnalyzeResult`` and one PostgreSQL ``AnalyzeResult``
        containing targets, statements, committed executions, and skipped names.
    When: ``_render_analyze_result`` formats each result.
    Then: SQLite emits one skip notice while PostgreSQL renders all four sections.
    """
    sqlite_noop = AnalyzeResult(
        targets=(),
        statements=(),
        executed=(),
        skipped=(),
        skipped_dialect="sqlite",
    )
    pg_outcome = AnalyzeResult(
        targets=("candles_d20260801", "candles"),
        statements=(
            'ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"',
            'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',
        ),
        executed=("candles_d20260801",),
        skipped=("candles",),
    )
    with patch("snapper.cli.daily_partitions.typer.echo") as echo:
        daily_partitions_cli._render_analyze_result(sqlite_noop)
        daily_partitions_cli._render_analyze_result(pg_outcome)

    assert echo.call_args_list == [
        call("skipped: non-postgresql dialect (sqlite)"),
        call("targets: 2"),
        call("  target: candles_d20260801"),
        call("  target: candles"),
        call("statements: 2"),
        call('ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"'),
        call('ANALYZE (SKIP_LOCKED) ONLY "public"."candles"'),
        call("executed: 1"),
        call("  executed: candles_d20260801"),
        call("skipped: 1"),
        call("  skipped: candles"),
    ]


def test_ensure_apply_runs_post_rollover_analyze_and_warns_nonfatally_on_failure() -> None:
    """Successful ``ensure --apply`` runs best-effort post-rollover ANALYZE without aborting.

    Given: ``ensure_future_leaves`` returning ``ENSURED`` under ``--apply``, first
        with successful ``analyze_rollover_targets`` and then failing with
        ``DailyPartitionError``.
    When: The operator invokes ``daily-partitions ensure candles --anchor ... --apply``.
    Then: The first run prints the ANALYZE summary and exits 0; the second emits a
        warning to ``stderr``, still exits 0, and disposes the engine.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    ensured = LifecycleResult(
        table="candles",
        action=LifecycleAction.ENSURED,
        statements=("CREATE TABLE candles_d20260815 PARTITION OF candles",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    analyzed = AnalyzeResult(
        targets=("candles_d20260801", "candles"),
        statements=(
            'ANALYZE (SKIP_LOCKED) "public"."candles_d20260801"',
            'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',
        ),
        executed=("candles_d20260801", "candles"),
        skipped=(),
    )

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch("snapper.cli.daily_partitions.ensure_future_leaves", return_value=ensured),
        patch(
            "snapper.cli.daily_partitions.analyze_rollover_targets",
            return_value=analyzed,
        ) as rollover_analyze,
    ):
        ok_result = runner.invoke(
            daily_partitions_app,
            ["ensure", "candles", "--anchor", _ANCHOR, "--apply"],
        )

    assert ok_result.exit_code == 0, ok_result.stderr
    assert "action: ensured" in ok_result.stdout
    assert "executed: 2" in ok_result.stdout
    rollover_analyze.assert_called_once_with(
        connection,
        "candles",
        _ANCHOR_VALUE,
        dry_run=False,
    )

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch("snapper.cli.daily_partitions.ensure_future_leaves", return_value=ensured),
        patch(
            "snapper.cli.daily_partitions.analyze_rollover_targets",
            side_effect=DBAPIError("ANALYZE", None, RuntimeError("lock held")),
        ),
    ):
        warn_result = runner.invoke(
            daily_partitions_app,
            ["ensure", "candles", "--anchor", _ANCHOR, "--apply"],
        )

    assert warn_result.exit_code == 0
    assert "warning: post-rollover ANALYZE failed" in warn_result.stderr
    assert engine.dispose.call_count == 2


def test_ensure_noop_and_dry_run_do_not_invoke_post_rollover_analyze() -> None:
    """``ensure`` NOOP outcomes and dry-runs must never trigger post-rollover ANALYZE.

    Given: ``ensure_future_leaves`` returning ``NOOP`` on ``--apply`` and ``PLANNED``
        on default dry-run.
    When: Invoking ``daily-partitions ensure``.
    Then: ``analyze_rollover_targets`` is not called in either case.
    """
    runner = CliRunner()
    engine, _connection = _engine_double()
    noop = LifecycleResult(
        table="ticks",
        action=LifecycleAction.NOOP,
        statements=(),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    planned = LifecycleResult(
        table="ticks",
        action=LifecycleAction.PLANNED,
        statements=("CREATE TABLE ticks_d20260802 PARTITION OF ticks",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch("snapper.cli.daily_partitions.ensure_future_leaves", return_value=noop),
        patch("snapper.cli.daily_partitions.analyze_rollover_targets") as rollover_analyze,
    ):
        res_noop = runner.invoke(
            daily_partitions_app,
            ["ensure", "ticks", "--anchor", _ANCHOR, "--apply"],
        )

    assert res_noop.exit_code == 0, res_noop.stderr
    rollover_analyze.assert_not_called()

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch("snapper.cli.daily_partitions.ensure_future_leaves", return_value=planned),
        patch("snapper.cli.daily_partitions.analyze_rollover_targets") as rollover_analyze,
    ):
        res_dry = runner.invoke(
            daily_partitions_app,
            ["ensure", "ticks", "--anchor", _ANCHOR],
        )

    assert res_dry.exit_code == 0, res_dry.stderr
    rollover_analyze.assert_not_called()


def test_analyze_cli_defaults_to_dry_run_stale_sweep_with_limit_200() -> None:
    """Bare ``analyze`` must default to dry-run stale sweep capped at 200 relations.

    Given: Default arguments to ``snapper daily-partitions analyze``.
    When: The command runs without ``table`` or ``--apply``.
    Then: Only ``analyze_stale_relations(conn, limit=200, dry_run=True)`` runs.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    planned = AnalyzeResult(
        targets=("candles",),
        statements=('ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',),
        executed=(),
        skipped=(),
    )

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch("snapper.cli.daily_partitions.analyze_rollover_targets") as rollover_analyze,
        patch(
            "snapper.cli.daily_partitions.analyze_stale_relations",
            return_value=planned,
        ) as stale_analyze,
    ):
        result = runner.invoke(daily_partitions_app, ["analyze"])

    assert result.exit_code == 0, result.stderr
    assert 'ANALYZE (SKIP_LOCKED) ONLY "public"."candles"' in result.stdout
    rollover_analyze.assert_not_called()
    stale_analyze.assert_called_once_with(connection, limit=200, dry_run=True)
    engine.dispose.assert_called_once_with()


def test_analyze_cli_table_and_combined_stale_apply_forward_exact_options() -> None:
    """Explicit table, anchor, ``--stale``, ``--limit``, and ``--apply`` pass through cleanly.

    Given: Explicit ``candles``, ``--anchor``, ``--stale``, ``--limit 50``, and ``--apply``.
    When: The operator invokes ``daily-partitions analyze``.
    Then: Both ``analyze_rollover_targets`` and ``analyze_stale_relations`` execute
        with ``dry_run=False`` in order and the engine is disposed.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    outcome = AnalyzeResult(
        targets=("candles",),
        statements=('ANALYZE (SKIP_LOCKED) ONLY "public"."candles"',),
        executed=("candles",),
        skipped=(),
    )

    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.analyze_rollover_targets",
            return_value=outcome,
        ) as rollover_analyze,
        patch(
            "snapper.cli.daily_partitions.analyze_stale_relations",
            return_value=outcome,
        ) as stale_analyze,
    ):
        result = runner.invoke(
            daily_partitions_app,
            [
                "analyze",
                "candles",
                "--anchor",
                _ANCHOR,
                "--stale",
                "--limit",
                "50",
                "--apply",
            ],
        )

    assert result.exit_code == 0, result.stderr
    rollover_analyze.assert_called_once_with(
        connection,
        "candles",
        _ANCHOR_VALUE,
        dry_run=False,
    )
    stale_analyze.assert_called_once_with(connection, limit=50, dry_run=False)
    engine.dispose.assert_called_once_with()


def test_analyze_cli_validates_inputs_before_engine_and_disposes_on_error() -> None:
    """Invalid table, empty ``--no-stale``, or non-positive ``--limit`` fail before DB access.

    Given: Disallowed table, ``--no-stale`` without table, bad limit, and runtime error.
    When: Calling ``daily-partitions analyze``.
    Then: Input errors exit 1 without constructing an engine; runtime errors exit 1
        after disposing the constructed engine.
    """
    runner = CliRunner()
    with patch("snapper.cli.daily_partitions._engine") as engine_factory:
        bad_table = runner.invoke(daily_partitions_app, ["analyze", "orders"])
        no_work = runner.invoke(daily_partitions_app, ["analyze", "--no-stale"])
        bad_limit = runner.invoke(daily_partitions_app, ["analyze", "--limit", "0"])

    assert bad_table.exit_code == 1
    assert "market-data table must be one of" in bad_table.stderr
    assert no_work.exit_code == 1
    assert "specify a market-data table" in no_work.stderr
    assert bad_limit.exit_code == 1
    assert "--limit must be >= 1" in bad_limit.stderr
    engine_factory.assert_not_called()

    engine, _connection = _engine_double()
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.analyze_stale_relations",
            side_effect=DailyPartitionError("analyze failed"),
        ),
    ):
        fatal_res = runner.invoke(daily_partitions_app, ["analyze", "--apply"])

    assert fatal_res.exit_code == 1
    assert "analyze failed" in fatal_res.stderr
    engine.dispose.assert_called_once_with()


def test_analyze_cli_runs_end_to_end_as_sqlite_noop() -> None:
    """Running ``daily-partitions analyze`` or post-ensure ANALYZE against SQLite exits zero.

    Given: An isolated in-memory SQLite engine returned by ``_engine``.
    When: Invoking ``daily-partitions analyze --apply``, ``analyze candles --apply``,
        and ``ensure candles --apply`` returning ``ENSURED``.
    Then: The analyze commands exit 0 with ``skipped: non-postgresql dialect (sqlite)``
        and ``ensure`` completes silently without emitting PostgreSQL DDL.
    """
    runner = CliRunner()
    sqlite_engine = create_engine("sqlite:///:memory:", future=True)
    with patch("snapper.cli.daily_partitions._engine", return_value=sqlite_engine):
        stale_res = runner.invoke(daily_partitions_app, ["analyze", "--apply"])
    sqlite_engine2 = create_engine("sqlite:///:memory:", future=True)
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=sqlite_engine2),
        patch("snapper.cli.daily_partitions.current_utc_anchor", return_value=_ANCHOR_VALUE),
    ):
        table_res = runner.invoke(daily_partitions_app, ["analyze", "candles", "--apply"])
    sqlite_engine3 = create_engine("sqlite:///:memory:", future=True)
    ensured = LifecycleResult(
        table="candles",
        action=LifecycleAction.ENSURED,
        statements=("CREATE TABLE candles_d20260815 PARTITION OF candles",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=sqlite_engine3),
        patch("snapper.cli.daily_partitions.ensure_future_leaves", return_value=ensured),
    ):
        ensure_sqlite = runner.invoke(
            daily_partitions_app,
            ["ensure", "candles", "--anchor", _ANCHOR, "--apply"],
        )

    assert stale_res.exit_code == 0, stale_res.stderr
    assert "skipped: non-postgresql dialect (sqlite)" in stale_res.stdout
    assert table_res.exit_code == 0, table_res.stderr
    assert "skipped: non-postgresql dialect (sqlite)" in table_res.stdout
    assert ensure_sqlite.exit_code == 0, ensure_sqlite.stderr
    assert "non-postgresql dialect" not in ensure_sqlite.stdout
