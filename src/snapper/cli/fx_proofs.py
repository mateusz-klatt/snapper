"""Operator CLI for reporting and pinning historical FX conversion proofs."""

import asyncio
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import typer

from snapper.application.portfolio.fx_proof_backfill import FX_PROOF_BACKFILL_CHECKPOINT
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillRequirement
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillResult
from snapper.application.portfolio.fx_proof_backfill import run_fx_proof_backfill
from snapper.config.settings import get_settings
from snapper.data.repository import get_repository

fx_proofs_app = typer.Typer(
    add_completion=False,
    help="Report or pin durable execution-minute FX conversion proofs.",
)


def _group_key(requirement: FxProofBackfillRequirement) -> tuple[str, str, str]:
    """Return the required wallet, pair, and scope-kind report grouping."""
    return (
        requirement.wallet_public_id,
        "-".join(requirement.evaluation.pair),
        requirement.evaluation.scope_kind,
    )


def _oldest(requirements: list[FxProofBackfillRequirement]) -> datetime:
    """Return the oldest exact minute within one nonempty report group."""
    return min(min(item.evaluation.required_minutes) for item in requirements)


def _render(result: FxProofBackfillResult, apply: bool) -> None:
    """Print grouped unpinned requirements, refusals, and seven counters."""
    unpinned = [item for item in result.requirements if not item.pinned]
    groups: dict[tuple[str, str, str], list[FxProofBackfillRequirement]] = defaultdict(list)
    for requirement in unpinned:
        groups[_group_key(requirement)].append(requirement)
    typer.echo("Unpinned requirements")
    typer.echo("wallet | pair | scope kind | count | oldest minute")
    for key, requirements in sorted(groups.items()):
        typer.echo(
            f"{key[0]} | {key[1]} | {key[2]} | {len(requirements)} | "
            f"{_oldest(requirements).isoformat()}"
        )
    typer.echo("Refusals")
    refused = [
        item
        for item in result.requirements
        if item.evaluation.selected_plane is None or not item.evaluation.required_minutes
    ]
    if not refused and not result.semantic_refusals:
        typer.echo("none")
    for refusal in result.semantic_refusals:
        typer.echo(refusal)
    for item in refused:
        minutes = ",".join(
            minute.isoformat() for minute in sorted(item.evaluation.required_minutes)
        )
        typer.echo(
            f"{item.wallet_public_id} | {'-'.join(item.evaluation.pair)} | "
            f"{item.evaluation.scope_kind} | {minutes} | fx_conversion_unproven"
        )
    metrics = result.metrics
    typer.echo("Summary")
    typer.echo("counter | value")
    for name in (
        "creation",
        "reuse",
        "conflict",
        "upgrade_required",
        "mismatch",
        "failure",
        "dropped",
    ):
        typer.echo(f"{name} | {getattr(metrics, name)}")
    typer.echo(f"mode | {'apply' if apply else 'report'}")
    typer.echo(f"processed | {result.processed}")


async def _run(apply: bool, checkpoint: Path) -> FxProofBackfillResult:
    """Open the configured repository and execute one operator run."""
    repository = get_repository(get_settings().db_url)
    return await run_fx_proof_backfill(repository, apply, checkpoint)


@fx_proofs_app.command(name="backfill")
def backfill_command(
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Persist proofs and refusal audits; the default only reports.",
    ),
    checkpoint: Path = typer.Option(
        FX_PROOF_BACKFILL_CHECKPOINT,
        "--checkpoint",
        help="Resumable apply cursor file.",
    ),
) -> None:
    """Report or pin the active durable P&L execution-minute requirement union.

    Args:
        apply: Persist proofs and refusal audits when true.
        checkpoint: Resumable batch cursor path used only by apply mode.
    """
    try:
        result = asyncio.run(_run(apply, checkpoint))
        _render(result, apply)
        if apply and result.semantic_refusals:
            raise typer.Exit(code=1)
    except (OSError, RuntimeError, ValueError) as error:
        typer.echo(f"refused: {error}", err=True)
        raise typer.Exit(code=1) from error
