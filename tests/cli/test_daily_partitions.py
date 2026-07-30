"""Operator-surface contracts for ``snapper daily-partitions``.

These focused tests prove the host CLI keeps mutation opt-in, validates the
static table and anchor before connecting, and normalizes the heavy-adoption
load ceiling by CPU count. Database topology behavior remains covered by the
runtime units and the independent PostgreSQL convergence suite.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

import snapper.cli.daily_partitions as daily_partitions_cli
from snapper.cli.app import app
from snapper.cli.daily_partitions import _require_safe_adoption_load
from snapper.cli.daily_partitions import daily_partitions_app
from snapper.data.daily_partitions import DailyPartitionError
from snapper.data.daily_partitions import LifecycleAction
from snapper.data.daily_partitions import LifecycleResult
from snapper.data.daily_partitions import PartitionInspection
from snapper.data.daily_partitions import PartitionRef
from snapper.data.daily_partitions import RelationState

_ANCHOR = "2026-08-01T00:00:00+00:00"
_ANCHOR_VALUE = datetime(2026, 8, 1, tzinfo=UTC)
_PARTITION_BOUND = "FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')"


def _engine_double() -> tuple[MagicMock, MagicMock]:
    """Build one engine and clean connection context for CLI tests.

    Returns:
        Engine and connection doubles wired through ``engine.connect()``.
    """
    engine = MagicMock(spec=Engine)
    connection = MagicMock(spec=Connection)
    engine.connect.return_value.__enter__.return_value = connection
    return engine, connection


def _planned_result() -> LifecycleResult:
    """Build one compact dry-run adoption result.

    Returns:
        Typed lifecycle plan suitable for CLI rendering.
    """
    return LifecycleResult(
        table="trades",
        action=LifecycleAction.PLANNED,
        statements=("ALTER TABLE trades PLAN",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )


def _partitioned_inspection() -> PartitionInspection:
    """Build one healthy adopted topology for CLI rendering tests.

    Returns:
        Typed catalog report with one visible daily partition.
    """
    return PartitionInspection(
        table="trades",
        state=RelationState.PARTITIONED,
        partition_key="RANGE (executed_at)",
        partitions=(
            PartitionRef(
                name="trades_d20260801",
                bound=_PARTITION_BOUND,
            ),
        ),
        default_attached=True,
        default_has_rows=False,
        future_leaf_count=14,
        future_leaf_alarm=False,
    )


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (
            "postgresql+asyncpg://snapper:secret@db/snapper",
            "postgresql+psycopg2://snapper:secret@db/snapper",
        ),
        (
            "sqlite+aiosqlite:///./data/dev.db",
            "sqlite:///./data/dev.db",
        ),
        (
            "postgresql+psycopg2://snapper:secret@db/snapper",
            "postgresql+psycopg2://snapper:secret@db/snapper",
        ),
    ],
)
def test_sync_database_url_converts_only_supported_async_drivers(
    configured: str,
    expected: str,
) -> None:
    """Driver conversion must preserve every non-driver URL component.

    Args:
        configured: Configured async or already-synchronous database URL.
        expected: Exact URL accepted by the lifecycle engine.

    Given: Each supported async driver and one already-synchronous URL.
    When: The host boundary converts the configuration for synchronous DDL.
    Then: Only the driver token changes, and an unrelated URL is unchanged.
    """
    assert daily_partitions_cli._sync_database_url(configured) == expected


def test_engine_uses_bootstrap_settings_and_the_synchronous_driver() -> None:
    """The host engine must never pass an async driver to synchronous DDL.

    Given: Bootstrap settings containing the application's async PostgreSQL URL.
    When: The short-lived lifecycle engine is constructed.
    Then: SQLAlchemy receives the converted URL and explicit future semantics.
    """
    engine = MagicMock(spec=Engine)
    settings = SimpleNamespace(db_url="postgresql+asyncpg://snapper:secret@db/snapper")
    with (
        patch(
            "snapper.cli.daily_partitions.get_bootstrap_settings",
            return_value=settings,
        ) as get_settings,
        patch(
            "snapper.cli.daily_partitions.create_engine",
            return_value=engine,
        ) as create,
    ):
        result = daily_partitions_cli._engine()

    assert result is engine
    get_settings.assert_called_once_with()
    create.assert_called_once_with(
        "postgresql+psycopg2://snapper:secret@db/snapper",
        future=True,
    )


def test_dispose_engine_handles_both_early_refusal_and_constructed_engine() -> None:
    """Cleanup must tolerate validation refusal and release a real engine.

    Given: One absent engine and one constructed short-lived engine.
    When: Common command cleanup handles both states.
    Then: Absence is a no-op and the constructed engine is disposed once.
    """
    engine = MagicMock(spec=Engine)

    daily_partitions_cli._dispose_engine(None)
    daily_partitions_cli._dispose_engine(engine)

    engine.dispose.assert_called_once_with()


def test_effective_anchor_uses_today_only_when_the_operator_omits_it() -> None:
    """An explicit anchor must not be replaced by the wall clock.

    Given: One omitted anchor and one canonical operator-supplied anchor.
    When: The CLI resolves both lifecycle boundaries.
    Then: Only omission calls the current-day provider and the explicit value
        is parsed exactly once.
    """
    current = datetime(2026, 8, 2, tzinfo=UTC)
    with (
        patch(
            "snapper.cli.daily_partitions.current_utc_anchor",
            return_value=current,
        ) as current_anchor,
        patch(
            "snapper.cli.daily_partitions.parse_anchor",
            return_value=_ANCHOR_VALUE,
        ) as parse,
    ):
        omitted = daily_partitions_cli._effective_anchor(None)
        explicit = daily_partitions_cli._effective_anchor(_ANCHOR)

    assert omitted == current
    assert explicit == _ANCHOR_VALUE
    current_anchor.assert_called_once_with()
    parse.assert_called_once_with(_ANCHOR)


def test_host_load_reads_the_linux_one_minute_value_without_fallback() -> None:
    """Linux load discovery must select the first ``/proc/loadavg`` field.

    Given: A readable procfs load file and a distinct portable fallback value.
    When: The adoption safety guard reads host load.
    Then: It parses the one-minute procfs field and never calls ``getloadavg``.
    """
    load_path = MagicMock()
    load_path.exists.return_value = True
    load_path.read_text.return_value = "4.25 3.00 2.00 1/100 123"
    with (
        patch("snapper.cli.daily_partitions.Path", return_value=load_path) as path,
        patch.object(
            daily_partitions_cli.os,
            "getloadavg",
            create=True,
        ) as getloadavg,
    ):
        load = daily_partitions_cli._host_load()

    assert load == 4.25
    path.assert_called_once_with("/proc/loadavg")
    load_path.read_text.assert_called_once_with(encoding="utf-8")
    getloadavg.assert_not_called()


def test_host_load_falls_back_when_procfs_is_unavailable() -> None:
    """Non-Linux hosts must retain the portable one-minute load source.

    Given: No procfs load file and a three-window operating-system load tuple.
    When: The adoption safety guard reads host load.
    Then: It returns the first tuple value without attempting to read the path.
    """
    load_path = MagicMock()
    load_path.exists.return_value = False
    with (
        patch("snapper.cli.daily_partitions.Path", return_value=load_path),
        patch.object(
            daily_partitions_cli.os,
            "getloadavg",
            return_value=(2.5, 2.0, 1.5),
            create=True,
        ) as getloadavg,
    ):
        load = daily_partitions_cli._host_load()

    assert load == 2.5
    load_path.read_text.assert_not_called()
    getloadavg.assert_called_once_with()


def test_host_load_refuses_when_the_host_exposes_no_load_source() -> None:
    """Unsupported hosts must fail closed instead of bypassing load safety.

    Given: No procfs load file and no portable operating-system load reader.
    When: The adoption safety guard tries to determine one-minute host load.
    Then: It raises a controlled lifecycle refusal naming both absent sources.
    """
    load_path = MagicMock()
    load_path.exists.return_value = False
    with (
        patch("snapper.cli.daily_partitions.Path", return_value=load_path),
        patch.object(
            daily_partitions_cli.os,
            "getloadavg",
            None,
            create=True,
        ),
        pytest.raises(
            DailyPartitionError,
            match="/proc/loadavg and os\\.getloadavg are unavailable",
        ),
    ):
        daily_partitions_cli._host_load()

    load_path.read_text.assert_not_called()


def test_render_inspection_keeps_each_anomaly_visibly_distinct() -> None:
    """The operator report must not render unsafe states as healthy.

    Given: An ordinary relation with no key, too few future leaves, no DEFAULT,
        and a nonempty anomaly buffer.
    When: The CLI renders its complete inspection.
    Then: Every unsafe state is loud and the empty child list adds no fake row.
    """
    report = PartitionInspection(
        table="ticks",
        state=RelationState.ORDINARY,
        partition_key=None,
        partitions=(),
        default_attached=False,
        default_has_rows=True,
        future_leaf_count=0,
        future_leaf_alarm=True,
    )
    with patch("snapper.cli.daily_partitions.typer.echo") as echo:
        daily_partitions_cli._render_inspection(report)

    assert echo.call_args_list == [
        call("table: ticks"),
        call("state: ordinary"),
        call("partition key: -"),
        call("future daily leaves: 0"),
        call("future leaf alarm: YES"),
        call("DEFAULT attached: NO"),
        call("DEFAULT has rows: YES"),
        call("direct partitions: 0"),
    ]


def test_render_result_keeps_an_alarm_visible_when_no_ddl_is_planned() -> None:
    """A no-statement result must not hide a depleted future-leaf window.

    Given: A lifecycle no-op that still carries the future-leaf alarm.
    When: The CLI renders the result.
    Then: It prints the alarm and zero count without inventing a statement.
    """
    result = LifecycleResult(
        table="ticks",
        action=LifecycleAction.NOOP,
        statements=(),
        future_leaf_count=0,
        future_leaf_alarm=True,
    )
    with patch("snapper.cli.daily_partitions.typer.echo") as echo:
        daily_partitions_cli._render_result(result)

    assert echo.call_args_list == [
        call("table: ticks"),
        call("action: noop"),
        call("future daily leaves: 0"),
        call("future leaf alarm: YES"),
        call("statements: 0"),
    ]


def test_adopt_cli_defaults_to_dry_run_and_uses_a_clean_connection() -> None:
    """Omitting ``--apply`` must keep the host command read-only.

    Given: A valid table and explicit UTC anchor with a configured host engine.
    When: The operator invokes ``daily-partitions adopt`` without ``--apply``.
    Then: The public path receives ``dry_run=True`` through ``engine.connect()``
        and the rendered outcome says the operation was only planned.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    with (
        patch(
            "snapper.cli.daily_partitions._engine",
            return_value=engine,
        ),
        patch(
            "snapper.cli.daily_partitions.adopt",
            return_value=_planned_result(),
        ) as adopt,
    ):
        result = runner.invoke(
            daily_partitions_app,
            ["adopt", "trades", "--anchor", _ANCHOR],
        )

    assert result.exit_code == 0, result.stderr
    assert "action: planned" in result.stdout
    engine.connect.assert_called_once_with()
    adopt.assert_called_once()
    assert adopt.call_args.args[0] is connection
    assert adopt.call_args.kwargs["dry_run"] is True
    engine.dispose.assert_called_once_with()


def test_daily_partitions_group_is_registered_on_the_host_cli() -> None:
    """The lifecycle surface must be reachable from the installed Snapper CLI.

    Given: The application-level Typer registry.
    When: Registered command groups are inspected without invoking a callback.
    Then: Exactly one group exposes the ``daily-partitions`` host command.
    """
    names = [group.name for group in app.registered_groups]

    assert names.count("daily-partitions") == 1


def test_adopt_cli_apply_flips_the_public_dry_run_flag_to_false() -> None:
    """Explicit ``--apply`` must cross the final CLI mutation boundary.

    Given: Safe normalized host load, a valid table and anchor, and a mocked
        host engine that opens no configured database.
    When: The operator explicitly supplies ``--apply``.
    Then: The public adoption function receives ``dry_run=False`` exactly once.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    applied = LifecycleResult(
        table="trades",
        action=LifecycleAction.ADOPTED,
        statements=("ALTER TABLE trades APPLY",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    with (
        patch("snapper.cli.daily_partitions.os.cpu_count", return_value=4),
        patch("snapper.cli.daily_partitions._host_load", return_value=1.0),
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.adopt",
            return_value=applied,
        ) as adopt,
    ):
        result = runner.invoke(
            daily_partitions_app,
            ["adopt", "trades", "--anchor", _ANCHOR, "--apply"],
        )

    assert result.exit_code == 0, result.stderr
    adopt.assert_called_once()
    assert adopt.call_args.args[0] is connection
    assert adopt.call_args.kwargs["dry_run"] is False


def test_adopt_cli_apply_is_refused_above_the_per_cpu_load_ceiling() -> None:
    """A busy router host must refuse before opening the database.

    Given: Four CPUs, one-minute load just above the normalized 6.0 ceiling,
        and an explicit ``--apply`` request.
    When: The host adoption command performs its safety preflight.
    Then: It exits one with the measured ceiling and never creates an engine.
    """
    runner = CliRunner()
    with (
        patch("snapper.cli.daily_partitions.os.cpu_count", return_value=4),
        patch("snapper.cli.daily_partitions._host_load", return_value=6.01),
        patch("snapper.cli.daily_partitions._engine") as engine,
    ):
        result = runner.invoke(
            daily_partitions_app,
            ["adopt", "trades", "--anchor", _ANCHOR, "--apply"],
        )

    assert result.exit_code == 1
    assert "4-CPU adoption ceiling 6.00" in result.stderr
    engine.assert_not_called()


def test_adoption_load_ceiling_scales_to_the_detected_cpu_count() -> None:
    """Load safe on four CPUs must not be treated as an absolute 1.5 breach.

    Given: Four CPUs and total one-minute load 5.99.
    When: The heavy-adoption guard normalizes 1.5 by CPU count.
    Then: It returns successfully because total load remains below 6.0.
    """
    with (
        patch("snapper.cli.daily_partitions.os.cpu_count", return_value=4),
        patch("snapper.cli.daily_partitions._host_load", return_value=5.99),
    ):
        _require_safe_adoption_load()


def test_adoption_load_ceiling_uses_one_cpu_when_detection_is_unavailable() -> None:
    """Missing CPU discovery must retain a conservative nonzero ceiling.

    Given: CPU discovery returns no value and total one-minute load is 1.51.
    When: The heavy-adoption guard computes its fallback.
    Then: It refuses at the one-CPU 1.5 ceiling instead of disabling safety.
    """
    with (
        patch("snapper.cli.daily_partitions.os.cpu_count", return_value=None),
        patch("snapper.cli.daily_partitions._host_load", return_value=1.51),
        pytest.raises(DailyPartitionError, match="1-CPU adoption ceiling 1.50"),
    ):
        _require_safe_adoption_load()


def test_adopt_cli_rejects_table_and_anchor_before_connecting() -> None:
    """Unsafe identifiers and wall-clock anchors must fail before DB access.

    Given: An out-of-scope relation and a date-only partition anchor.
    When: The host adoption command validates its arguments.
    Then: It exits one and does not instantiate a database engine.
    """
    runner = CliRunner()
    with patch("snapper.cli.daily_partitions._engine") as engine:
        result = runner.invoke(
            daily_partitions_app,
            ["adopt", "orders", "--anchor", "2026-08-01"],
        )

    assert result.exit_code == 1
    assert "market-data table must be one of" in result.stderr
    engine.assert_not_called()


def test_inspect_cli_renders_the_exact_catalog_report_and_disposes() -> None:
    """Read-only inspection must expose topology and release its engine.

    Given: A valid explicit anchor and a healthy adopted catalog report.
    When: The operator invokes ``daily-partitions inspect``.
    Then: The lifecycle API receives the clean connection and exact boundary,
        every partition is rendered, and the short-lived engine is disposed.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    report = _partitioned_inspection()
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.inspect",
            return_value=report,
        ) as inspect,
    ):
        result = runner.invoke(
            daily_partitions_app,
            ["inspect", "trades", "--anchor", _ANCHOR],
        )

    assert result.exit_code == 0, result.stderr
    assert "state: partitioned" in result.stdout
    assert "partition key: RANGE (executed_at)" in result.stdout
    assert "future leaf alarm: no" in result.stdout
    assert "DEFAULT attached: yes" in result.stdout
    assert "DEFAULT has rows: no" in result.stdout
    assert f"trades_d20260801: {_PARTITION_BOUND}" in result.stdout
    inspect.assert_called_once_with(connection, "trades", _ANCHOR_VALUE)
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize(
    ("operation_name", "arguments", "expected_args", "expected_kwargs"),
    [
        (
            "inspect",
            ["inspect", "trades", "--anchor", _ANCHOR],
            ("trades", _ANCHOR_VALUE),
            {},
        ),
        (
            "adopt",
            ["adopt", "trades", "--anchor", _ANCHOR],
            ("trades", _ANCHOR_VALUE),
            {"dry_run": True},
        ),
        (
            "ensure_future_leaves",
            ["ensure", "trades", "--anchor", _ANCHOR],
            ("trades", _ANCHOR_VALUE),
            {"dry_run": True},
        ),
        (
            "detach",
            [
                "detach",
                "trades",
                "--day",
                "2026-06-01",
                "--anchor",
                _ANCHOR,
            ],
            ("trades", date(2026, 6, 1), _ANCHOR_VALUE),
            {"dry_run": True},
        ),
    ],
)
def test_lifecycle_commands_dispose_after_a_downstream_refusal(
    operation_name: str,
    arguments: list[str],
    expected_args: tuple[object, ...],
    expected_kwargs: dict[str, bool],
) -> None:
    """Every command must release its engine after a lifecycle refusal.

    Args:
        operation_name: Patched lifecycle function invoked by the command.
        arguments: Complete valid command vector.
        expected_args: Positional lifecycle arguments after the connection.
        expected_kwargs: Mutation-intent keyword arguments.

    Given: Each valid command with a downstream controlled lifecycle error.
    When: The host CLI converts the refusal to exit code one.
    Then: It passes the exact typed inputs and disposes the constructed engine.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            f"snapper.cli.daily_partitions.{operation_name}",
            side_effect=DailyPartitionError(f"{operation_name} refused"),
        ) as operation,
    ):
        result = runner.invoke(daily_partitions_app, arguments)

    assert result.exit_code == 1
    assert result.stderr == f"{operation_name} refused\n"
    assert result.stdout == ""
    operation.assert_called_once_with(
        connection,
        *expected_args,
        **expected_kwargs,
    )
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize(
    "arguments",
    [
        ["inspect", "orders"],
        ["ensure", "orders"],
        ["detach", "orders", "--day", "2026-06-01"],
    ],
)
def test_lifecycle_commands_reject_unsafe_tables_before_engine_creation(
    arguments: list[str],
) -> None:
    """Every lifecycle entry point must validate identifiers before DB access.

    Args:
        arguments: Complete command vector with an unsafe relation name.

    Given: An out-of-scope table for inspect, ensure, or detach.
    When: The host command validates its first positional argument.
    Then: It exits one through the common fatal boundary without constructing
        or disposing an engine that never existed.
    """
    runner = CliRunner()
    with patch("snapper.cli.daily_partitions._engine") as engine:
        result = runner.invoke(daily_partitions_app, arguments)

    assert result.exit_code == 1
    assert "market-data table must be one of" in result.stderr
    engine.assert_not_called()


def test_ensure_cli_apply_uses_the_current_anchor_and_disposes() -> None:
    """Future-leaf creation must remain explicit and date-deterministic.

    Given: No anchor override, an explicit ``--apply``, and a healthy result.
    When: The operator invokes ``daily-partitions ensure``.
    Then: The current UTC boundary and ``dry_run=False`` reach the lifecycle
        API exactly once and the engine is disposed after rendering.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    result_value = LifecycleResult(
        table="ticks",
        action=LifecycleAction.ENSURED,
        statements=("CREATE TABLE ticks_d20260801 PARTITION OF ticks",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    with (
        patch(
            "snapper.cli.daily_partitions.current_utc_anchor",
            return_value=_ANCHOR_VALUE,
        ) as current_anchor,
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.ensure_future_leaves",
            return_value=result_value,
        ) as ensure,
    ):
        result = runner.invoke(
            daily_partitions_app,
            ["ensure", "ticks", "--apply"],
        )

    assert result.exit_code == 0, result.stderr
    assert "action: ensured" in result.stdout
    ensure.assert_called_once_with(
        connection,
        "ticks",
        _ANCHOR_VALUE,
        dry_run=False,
    )
    current_anchor.assert_called_once_with()
    engine.dispose.assert_called_once_with()


def test_detach_cli_apply_parses_the_day_and_preserves_the_anchor() -> None:
    """Plain DETACH must receive the exact event day and mutation intent.

    Given: A canonical expired day, explicit current boundary, and ``--apply``.
    When: The operator invokes ``daily-partitions detach``.
    Then: The lifecycle API receives typed date values and ``dry_run=False``,
        renders only DETACH, and releases the host engine.
    """
    runner = CliRunner()
    engine, connection = _engine_double()
    result_value = LifecycleResult(
        table="candles",
        action=LifecycleAction.DETACHED,
        statements=("ALTER TABLE candles DETACH PARTITION candles_d20260601",),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )
    with (
        patch("snapper.cli.daily_partitions._engine", return_value=engine),
        patch(
            "snapper.cli.daily_partitions.detach",
            return_value=result_value,
        ) as detach,
    ):
        result = runner.invoke(
            daily_partitions_app,
            [
                "detach",
                "candles",
                "--day",
                "2026-06-01",
                "--anchor",
                _ANCHOR,
                "--apply",
            ],
        )

    assert result.exit_code == 0, result.stderr
    assert "action: detached" in result.stdout
    assert "ALTER TABLE candles DETACH PARTITION candles_d20260601" in result.stdout
    detach.assert_called_once_with(
        connection,
        "candles",
        date(2026, 6, 1),
        _ANCHOR_VALUE,
        dry_run=False,
    )
    engine.dispose.assert_called_once_with()
