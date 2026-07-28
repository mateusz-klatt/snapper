"""Operator CLI for safely retiring trade-projection checkpoints."""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import NoReturn
from typing import cast

import typer
from sqlalchemy.engine import make_url

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import TradeProjectionCheckpointRow


def _fatal(message: str) -> NoReturn:
    """Print a refusal and terminate the command.

    Args:
        message: Refusal shown to the operator.

    Raises:
        typer.Exit: Always, with a non-zero status.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=1)


def _database_identity(db_url: str) -> str:
    """Render a credential-free database identity.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Dialect, host and database without user, password, port or query.
    """
    url = make_url(db_url)
    return (
        f"dialect={url.get_backend_name()} "
        f"host={url.host or '(local)'} database={url.database or '(none)'}"
    )


def _repository(db_url: str) -> SQLAlchemyRepository:
    """Resolve the configured repository.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Cached repository for the target database.
    """
    return cast(SQLAlchemyRepository, get_repository(db_url))


def _echo_candidates(candidates: list[TradeProjectionCheckpointRow]) -> None:
    """Print every economic field required for operator review.

    Args:
        candidates: Active checkpoint versions selected for retirement.
    """
    typer.echo(f"candidate checkpoints: {len(candidates)}")
    for row in candidates:
        typer.echo(
            f"  shard_key={row['shard_key']} position_qty={row['position_qty']} "
            f"realized_pnl={row['realized_pnl']} turnover={row['turnover']} "
            f"checkpoint_at={row['checkpoint_at'].isoformat()}"
        )


async def _run_retirement(db_url: str, shard_key: str | None, confirm: bool) -> None:
    """Preview candidates and optionally execute the asserted SCD2 close.

    Args:
        db_url: Configured database URL.
        shard_key: Exact shard selection, or None for all active checkpoints.
        confirm: Whether the separately supplied confirmation flag was present.
    """
    repository = _repository(db_url)
    try:
        candidates = await repository.get_trade_projection_checkpoint_retirement_candidates(
            shard_key, datetime.now(UTC)
        )
        _echo_candidates(candidates)
        if not candidates:
            typer.echo("nothing to do; closed 0 checkpoint rows")
            return
        if not confirm:
            typer.echo("DRY RUN: nothing was written; add --confirm to close these rows")
            return
        expected = [row["public_id"] for row in candidates]
        closed = await repository.retire_trade_projection_checkpoints(
            shard_key, expected, datetime.now(UTC)
        )
        if closed != len(candidates):
            _fatal(f"close count mismatch: previewed={len(candidates)} closed={closed}")
        typer.echo(f"closed {closed} checkpoint rows")
    except ValueError as error:
        _fatal(f"refused: {error}")
    finally:
        await dispose_repositories()


def retire_trade_projection_checkpoints(
    shard_key: Annotated[
        str | None,
        typer.Option("--shard-key", help="Select exactly one checkpoint shard key"),
    ] = None,
    all_checkpoints: Annotated[
        bool,
        typer.Option("--all", help="Select every currently active checkpoint"),
    ] = False,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Required separately to perform the SCD2 close"),
    ] = False,
) -> None:
    """Preview or SCD2-close selected trade-projection checkpoints.

    Exactly one selection must be explicit: ``--shard-key`` selects one
    checkpoint and ``--all`` selects every currently active checkpoint,
    regardless of checkpoint age. The default execution mode is a read-only
    preview. ``--confirm`` only acknowledges the already-separate selection;
    it never changes selection and never skips the preview.

    Args:
        shard_key: Exact shard key to select.
        all_checkpoints: Select all currently active checkpoints.
        confirm: Separately acknowledge closing the previewed versions.
    """
    if (shard_key is None) == (not all_checkpoints):
        _fatal("refused: supply exactly one of --shard-key or --all")
    db_url = get_bootstrap_settings().db_url
    typer.echo(f"target database: {_database_identity(db_url)}")
    asyncio.run(_run_retirement(db_url, shard_key, confirm))
