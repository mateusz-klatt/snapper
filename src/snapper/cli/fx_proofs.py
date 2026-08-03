"""Operator CLI for reporting and pinning historical FX conversion proofs."""

import asyncio
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import typer

from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_evaluation_completeness
from snapper.application.portfolio.fx_proof_backfill import FX_PROOF_BACKFILL_CHECKPOINT
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillRequirement
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillResult
from snapper.application.portfolio.fx_proof_backfill import fx_proof_backfill_apply_lock
from snapper.application.portfolio.fx_proof_backfill import reset_fx_proof_backfill_checkpoint
from snapper.application.portfolio.fx_proof_backfill import run_fx_proof_backfill
from snapper.config.settings import get_settings
from snapper.data.repository import get_repository

fx_proofs_app = typer.Typer(
    add_completion=False,
    help="Report or pin durable execution-minute FX conversion proofs.",
)


def _group_key(requirement: FxProofBackfillRequirement) -> tuple[str, str, str]:
    """Return the wallet, pair, and scope-kind report grouping."""
    return (
        requirement.wallet_public_id,
        "-".join(requirement.evaluation.pair),
        requirement.evaluation.scope_kind,
    )


def _oldest(requirements: list[FxProofBackfillRequirement]) -> datetime:
    """Return the oldest exact minute within one nonempty report group."""
    return min(min(item.evaluation.required_minutes) for item in requirements)


def _render_requirement_state(result: FxProofBackfillResult) -> None:
    """Print state visible at each durable consumer's knowledge horizon."""
    groups: dict[tuple[str, str, str], list[FxProofBackfillRequirement]] = defaultdict(list)
    for requirement in result.requirements:
        groups[_group_key(requirement)].append(requirement)
    typer.echo("Requirement state")
    typer.echo("wallet | pair | scope kind | electorates | oldest minute | consumer-horizon state")
    for key, requirements in sorted(groups.items()):
        states = {
            "proof" if item.pinned else "refusal-audit" if item.refusal_audited else "missing"
            for item in requirements
        }
        typer.echo(
            f"{key[0]} | {key[1]} | {key[2]} | {len(requirements)} | "
            f"{_oldest(requirements).isoformat()} | {','.join(sorted(states))}"
        )


def _render_refusals(result: FxProofBackfillResult) -> None:
    """Print election refusals, semantic refusals, and persistence failures."""
    typer.echo("Refusals")
    printed = False
    for refusal in result.semantic_refusals:
        lost = ",".join(refusal.lost_requirements) or "unknown"
        typer.echo(
            f"{refusal.wallet_public_id} | {refusal.valuation_ccy} | "
            f"{refusal.calculation_version} | {refusal.instrument_public_id} | "
            f"{refusal.reason} | lost={lost}"
        )
        printed = True
    for item in result.requirements:
        completeness = fx_shadow_evaluation_completeness(item.evaluation)
        if completeness != "complete":
            minutes = ",".join(
                minute.isoformat() for minute in sorted(item.evaluation.required_minutes)
            )
            state = (
                "audited"
                if item.refusal_audited
                else (
                    "partial-proof"
                    if completeness == "partial" and item.pinned
                    else "missing-audit"
                )
            )
            typer.echo(
                f"{item.wallet_public_id} | {'-'.join(item.evaluation.pair)} | "
                f"{item.evaluation.scope_kind} | {minutes} | fx_conversion_unproven | "
                f"{completeness} | {state}"
            )
            printed = True
    if result.metrics.failure > 0 or result.aborted:
        typer.echo(
            f"persistence | aborted={str(result.aborted).lower()} | "
            f"failure={result.metrics.failure}"
        )
        printed = True
    if not printed:
        typer.echo("none")


def _render(result: FxProofBackfillResult, apply: bool) -> None:
    """Print post-run state, explicit refusals, counters, and classified writes."""
    _render_requirement_state(result)
    _render_refusals(result)
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
    typer.echo(f"proof creations | {result.proof_creations}")
    typer.echo(f"refusal audit creations | {result.refusal_audit_creations}")
    typer.echo(f"mode | {'apply' if apply else 'report'}")
    typer.echo(f"processed consumers | {result.processed_consumers}")
    typer.echo(f"unreached consumers | {result.unreached_consumers}")
    resolved = "yes" if result.fully_verified else "NO"
    typer.echo(f"fully resolved (proof or refusal audit) | {resolved}")


def _anchored_checkpoint(path: Path) -> Path:
    """Anchor relative operator paths to the repository rather than the CWD."""
    if path.is_absolute():
        return path
    return FX_PROOF_BACKFILL_CHECKPOINT.parents[1] / path


async def _run(apply: bool, checkpoint: Path) -> FxProofBackfillResult:
    """Open the configured repository and execute one operator run."""
    repository = get_repository(get_settings().db_url)
    return await run_fx_proof_backfill(repository, apply, checkpoint)


def _run_locked(apply: bool, checkpoint: Path, reset_checkpoint: bool) -> FxProofBackfillResult:
    """Serialize mutation and optional checkpoint reset under one host lock."""
    if not apply:
        if reset_checkpoint:
            raise ValueError("--reset-checkpoint requires --apply")
        return asyncio.run(_run(False, checkpoint))
    with fx_proof_backfill_apply_lock(checkpoint):
        if reset_checkpoint:
            reset_fx_proof_backfill_checkpoint(checkpoint)
        return asyncio.run(_run(True, checkpoint))


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
        help="Anchored resumable consumer cursor file.",
    ),
    reset_checkpoint: bool = typer.Option(
        False,
        "--reset-checkpoint",
        help="Discard the cursor hint under the apply lock before starting.",
    ),
) -> None:
    """Report or pin active durable P&L consumer-wide FX electorates.

    Args:
        apply: Persist proofs and refusal audits when true.
        checkpoint: Anchored resumable consumer cursor path.
        reset_checkpoint: Remove the cursor hint before a locked apply.
    """
    try:
        result = _run_locked(apply, _anchored_checkpoint(checkpoint), reset_checkpoint)
        _render(result, apply)
    except (OSError, ValueError) as error:
        typer.echo(f"refused: {error}", err=True)
        raise typer.Exit(code=1) from error
    if apply and (
        result.aborted
        or not result.fully_verified
        or result.metrics.failure > 0
        or result.metrics.conflict > 0
        or result.metrics.mismatch > 0
        or result.metrics.dropped > 0
        or result.metrics.upgrade_required > 0
    ):
        raise typer.Exit(code=1)
