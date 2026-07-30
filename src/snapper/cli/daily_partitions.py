"""Host-run operator CLI for daily market-data partition lifecycle.

Every mutating command defaults to dry-run and requires ``--apply`` before it
executes DDL. The CLI reads the ordinary Snapper database setting only at the
host boundary; the lifecycle module itself accepts an explicit SQLAlchemy
connection so scratch and convergence tests never inherit ``DB_URL``.
"""

import os
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Final

import typer
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from snapper.config.settings import get_bootstrap_settings
from snapper.data.daily_partitions import DailyPartitionError
from snapper.data.daily_partitions import LifecycleResult
from snapper.data.daily_partitions import PartitionInspection
from snapper.data.daily_partitions import adopt
from snapper.data.daily_partitions import current_utc_anchor
from snapper.data.daily_partitions import detach
from snapper.data.daily_partitions import ensure_future_leaves
from snapper.data.daily_partitions import inspect
from snapper.data.daily_partitions import market_data_table
from snapper.data.daily_partitions import parse_anchor

_HEAVY_LOAD_PER_CPU: Final[float] = 1.5

daily_partitions_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Inspect and operate daily market-data partitions from the database host.",
)


def _sync_database_url(value: str) -> str:
    """Convert Snapper's supported async URL to its synchronous driver.

    Args:
        value: Configured SQLAlchemy database URL.

    Returns:
        Equivalent URL accepted by the synchronous lifecycle connection.
    """
    if value.startswith("postgresql+asyncpg://"):
        return value.replace(
            "postgresql+asyncpg://",
            "postgresql+psycopg2://",
            1,
        )
    if value.startswith("sqlite+aiosqlite://"):
        return value.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return value


def _engine() -> Engine:
    """Build the short-lived host lifecycle engine.

    Returns:
        Synchronous SQLAlchemy engine using configured database credentials.
    """
    settings = get_bootstrap_settings()
    return create_engine(_sync_database_url(settings.db_url), future=True)


def _effective_anchor(raw: str | None) -> datetime:
    """Resolve an optional strict anchor to current UTC midnight.

    Args:
        raw: Strict anchor string or no operator override.

    Returns:
        Explicit UTC midnight.
    """
    return current_utc_anchor() if raw is None else parse_anchor(raw)


def _host_load() -> float:
    """Read the host's one-minute load without starting external work.

    Returns:
        Current one-minute load average.
    """
    load_path = Path("/proc/loadavg")
    if load_path.exists():
        return float(load_path.read_text(encoding="utf-8").split(maxsplit=1)[0])
    return os.getloadavg()[0]


def _require_safe_adoption_load() -> None:
    """Refuse the potentially heavy U3 build while the router host is busy.

    Raises:
        DailyPartitionError: If one-minute host load exceeds the safe ceiling.
    """
    load = _host_load()
    processor_count = max(os.cpu_count() or 1, 1)
    ceiling = _HEAVY_LOAD_PER_CPU * processor_count
    if load > ceiling:
        raise DailyPartitionError(
            f"refused: one-minute host load {load:.2f} exceeds "
            f"the {processor_count}-CPU adoption ceiling {ceiling:.2f}"
        )


def _render_inspection(report: PartitionInspection) -> None:
    """Print one compact catalog inspection for an operator.

    Args:
        report: Typed read-only lifecycle report.
    """
    typer.echo(f"table: {report.table}")
    typer.echo(f"state: {report.state.value}")
    typer.echo(f"partition key: {report.partition_key or '-'}")
    typer.echo(f"future daily leaves: {report.future_leaf_count}")
    typer.echo(f"future leaf alarm: {'YES' if report.future_leaf_alarm else 'no'}")
    typer.echo(f"DEFAULT attached: {'yes' if report.default_attached else 'NO'}")
    typer.echo(f"DEFAULT has rows: {'YES' if report.default_has_rows else 'no'}")
    typer.echo(f"direct partitions: {len(report.partitions)}")
    for partition in report.partitions:
        typer.echo(f"  {partition.name}: {partition.bound}")


def _render_result(result: LifecycleResult) -> None:
    """Print one mutation plan or outcome with its exact ordered DDL.

    Args:
        result: Typed lifecycle operation result.
    """
    typer.echo(f"table: {result.table}")
    typer.echo(f"action: {result.action.value}")
    typer.echo(f"future daily leaves: {result.future_leaf_count}")
    typer.echo(f"future leaf alarm: {'YES' if result.future_leaf_alarm else 'no'}")
    typer.echo(f"statements: {len(result.statements)}")
    for statement in result.statements:
        typer.echo(statement)


def _fatal(error: Exception) -> None:
    """Convert an operator-facing refusal to a stable nonzero exit.

    Args:
        error: Validated input or lifecycle refusal.

    Raises:
        typer.Exit: Always, with refusal exit code one.
    """
    typer.echo(str(error), err=True)
    raise typer.Exit(code=1)


@daily_partitions_app.command(name="inspect")
def inspect_command(
    table: str = typer.Argument(..., help="ticks, candles, or trades"),
    anchor: str | None = typer.Option(
        None,
        "--anchor",
        help="UTC midnight as YYYY-MM-DDT00:00:00+00:00",
    ),
) -> None:
    """Inspect catalog topology, future coverage, and the DEFAULT buffer.

    Args:
        table: Statically allowlisted market-data table.
        anchor: Optional strict UTC boundary used for future coverage.
    """
    engine: Engine | None = None
    try:
        selected = market_data_table(table)
        effective_anchor = _effective_anchor(anchor)
        engine = _engine()
        with engine.connect() as connection:
            report = inspect(connection, selected, effective_anchor)
        _render_inspection(report)
    except (DailyPartitionError, ValueError, OSError) as error:
        _fatal(error)
    finally:
        if engine is not None:
            engine.dispose()


@daily_partitions_app.command(name="adopt")
def adopt_command(
    table: str = typer.Argument(..., help="ticks, candles, or trades"),
    anchor: str = typer.Option(
        ...,
        "--anchor",
        help="Required UTC midnight as YYYY-MM-DDT00:00:00+00:00",
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--apply",
        help="Print the plan by default; --apply explicitly executes DDL.",
    ),
) -> None:
    """Plan or apply the manual ordinary-table adoption path.

    Args:
        table: Statically allowlisted market-data table.
        anchor: Explicit deterministic legacy upper bound.
        dry_run: Safe default; ``--apply`` is required for DDL.
    """
    engine: Engine | None = None
    try:
        selected = market_data_table(table)
        effective_anchor = parse_anchor(anchor)
        if not dry_run:
            _require_safe_adoption_load()
        engine = _engine()
        with engine.connect() as connection:
            result = adopt(
                connection,
                selected,
                effective_anchor,
                dry_run=dry_run,
            )
        _render_result(result)
    except (DailyPartitionError, ValueError, OSError) as error:
        _fatal(error)
    finally:
        if engine is not None:
            engine.dispose()


@daily_partitions_app.command(name="ensure")
def ensure_command(
    table: str = typer.Argument(..., help="ticks, candles, or trades"),
    anchor: str | None = typer.Option(
        None,
        "--anchor",
        help="First required UTC day as YYYY-MM-DDT00:00:00+00:00",
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--apply",
        help="Print the plan by default; --apply explicitly executes DDL.",
    ),
) -> None:
    """Plan or create missing leaves through the fourteen-day target.

    Args:
        table: Statically allowlisted market-data table.
        anchor: Optional first required UTC day.
        dry_run: Safe default; ``--apply`` is required for DDL.
    """
    engine: Engine | None = None
    try:
        selected = market_data_table(table)
        effective_anchor = _effective_anchor(anchor)
        engine = _engine()
        with engine.connect() as connection:
            result = ensure_future_leaves(
                connection,
                selected,
                effective_anchor,
                dry_run=dry_run,
            )
        _render_result(result)
    except (DailyPartitionError, ValueError, OSError) as error:
        _fatal(error)
    finally:
        if engine is not None:
            engine.dispose()


@daily_partitions_app.command(name="detach")
def detach_command(
    table: str = typer.Argument(..., help="ticks, candles, or trades"),
    day: str = typer.Option(..., "--day", help="Expired UTC event day as YYYY-MM-DD"),
    anchor: str | None = typer.Option(
        None,
        "--anchor",
        help="Current UTC boundary as YYYY-MM-DDT00:00:00+00:00",
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--apply",
        help="Print the plan by default; --apply explicitly executes plain DETACH.",
    ),
) -> None:
    """Plan or detach one expired leaf without dropping its relation.

    Args:
        table: Statically allowlisted market-data table.
        day: Expired UTC event day as ``YYYY-MM-DD``.
        anchor: Optional current UTC retention boundary.
        dry_run: Safe default; ``--apply`` is required for DDL.
    """
    engine: Engine | None = None
    try:
        selected = market_data_table(table)
        effective_anchor = _effective_anchor(anchor)
        selected_day = date.fromisoformat(day)
        engine = _engine()
        with engine.connect() as connection:
            result = detach(
                connection,
                selected,
                selected_day,
                effective_anchor,
                dry_run=dry_run,
            )
        _render_result(result)
    except (DailyPartitionError, ValueError, OSError) as error:
        _fatal(error)
    finally:
        if engine is not None:
            engine.dispose()
