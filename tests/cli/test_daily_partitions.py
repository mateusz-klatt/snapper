"""Operator-surface contracts for ``snapper daily-partitions``.

These focused tests prove the host CLI keeps mutation opt-in, validates the
static table and anchor before connecting, and normalizes the heavy-adoption
load ceiling by CPU count. Database topology behavior remains covered by the
runtime units and the independent PostgreSQL convergence suite.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.cli.daily_partitions import _require_safe_adoption_load
from snapper.cli.daily_partitions import daily_partitions_app
from snapper.data.daily_partitions import DailyPartitionError
from snapper.data.daily_partitions import LifecycleAction
from snapper.data.daily_partitions import LifecycleResult

_ANCHOR = "2026-08-01T00:00:00+00:00"


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
