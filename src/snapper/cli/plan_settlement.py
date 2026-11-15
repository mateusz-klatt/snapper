"""Operator CLI for previewing and settling terminal execution plans.

Execution plans whose child create command reached a durable terminal state
while the plan executor missed the live bus transition remain in an actionable
status (`active` or `cancel_requested`). This command previews every candidate
transition and SHA-256 binds the target database endpoint, code revision, mode
scope, and exact candidate set before any SCD2 write:

1. Without `--confirm`, the command is strictly read-only and prints the
   credential-free target database identity, operational run-order requirement,
   residual concurrency window disclosure, candidate transitions with child
   evidence ids, skipped actionable plans, and `preview_sha256`.
2. With `--confirm`, `--expect-digest` is mandatory and must equal the current
   64-character hexadecimal `preview_sha256` inside the locked write
   transaction before any row is closed or inserted.
3. Operational requirement: `--confirm` must run with `plan_executor` stopped,
   or be followed immediately by restarting the API / `plan_executor` container
   under operator authorization.
"""

import asyncio
import json
import sys
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Final
from uuid import uuid4

import typer

from snapper.application.plans.settlement import RESIDUAL_VENUE_EVENT_WINDOW_NOTE
from snapper.application.plans.settlement import RUN_ORDER_REQUIREMENT_NOTE
from snapper.application.plans.settlement import PlanSettlementConfigError
from snapper.application.plans.settlement import PlanSettlementDriftError
from snapper.application.plans.settlement import PlanSettlementPreview
from snapper.application.plans.settlement import apply_plan_settlements
from snapper.application.plans.settlement import preview_plan_settlements
from snapper.application.plans.settlement import serialize_candidate
from snapper.application.plans.settlement import settlement_code_revision
from snapper.application.plans.settlement import settlement_database_endpoint
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import refuse
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.data.repository import dispose_repositories

_VALID_MODES: Final[frozenset[str]] = frozenset({"live", "paper", "all"})
_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")
_SHA256_HEX_LENGTH: Final[int] = 64


def _normalize_mode(raw_mode: str) -> str | None:
    """Validate `--mode` and map `all` to None for repository inspection.

    Args:
        raw_mode: Raw mode string supplied on the command line.

    Returns:
        `live`, `paper`, or None when `all` was requested.
    """
    normalized = raw_mode.strip().lower()
    if normalized not in _VALID_MODES:
        refuse(f"refused: --mode must be one of {sorted(_VALID_MODES)}, got {raw_mode!r}")
    return None if normalized == "all" else normalized


def _normalize_digest(raw_digest: str | None, *, confirm: bool) -> str | None:
    """Validate the optional or confirmation-required SHA-256 digest.

    Args:
        raw_digest: Raw `--expect-digest` value, or None when omitted.
        confirm: Whether `--confirm` was supplied.

    Returns:
        Canonical lowercase 64-character hex digest, or None during dry runs
        without an asserted digest.
    """
    if raw_digest is None:
        if confirm:
            refuse(
                "refused: --confirm requires --expect-digest <sha256> from a prior dry-run preview"
            )
        return None
    normalized = raw_digest.strip().lower()
    if len(normalized) != _SHA256_HEX_LENGTH or any(ch not in _HEX_DIGITS for ch in normalized):
        refuse(
            "refused: --expect-digest must be a 64-character hexadecimal sha256 digest, "
            f"got {raw_digest!r}"
        )
    return normalized


def _echo_preview(preview: PlanSettlementPreview) -> None:
    """Print every candidate transition, skipped plan, and SHA-256 digest.

    Args:
        preview: Settlement preview to display.
    """
    scope_label = preview.mode if preview.mode is not None else "all"
    typer.echo(f"mode scope: {scope_label}")
    typer.echo(f"run order requirement: {RUN_ORDER_REQUIREMENT_NOTE}")
    typer.echo(f"residual concurrency window: {RESIDUAL_VENUE_EVENT_WINDOW_NOTE}")
    typer.echo(f"settlement candidates: {len(preview.candidates)}")
    for item in preview.candidates:
        child_summary = ",".join(
            (
                f"{child.client_order_id}:{child.command_status}"
                f":cmd={child.command_public_id}"
                f":term_ev={child.terminal_venue_event_id}"
                f":fill_ev={child.fill_venue_event_id}"
                f":cum_fill={child.cumulative_fill_size!r}"
            )
            for child in item.children
        )
        typer.echo(
            f"  plan_public_id={item.plan_public_id} plan_type={item.plan_type} "
            f"exchange={item.exchange} mode={item.mode} "
            f"status={item.current_status}->{item.target_status} "
            f"filled_quantity={item.filled_quantity!r} last_error={item.last_error!r} "
            f"children={child_summary}"
        )
    typer.echo(f"skipped active plans: {len(preview.skipped)}")
    for skipped in preview.skipped:
        typer.echo(
            f"  plan_public_id={skipped.plan_public_id} plan_type={skipped.plan_type} "
            f"exchange={skipped.exchange} mode={skipped.mode} "
            f"status={skipped.current_status} skip_code={skipped.skip_code} "
            f"detail={skipped.detail!r}"
        )
    typer.echo(f"preview_sha256: {preview.digest}")


async def _run_settlement(
    db_url: str,
    target_database: JsonObject,
    mode_filter: str | None,
    expected_digest: str | None,
    *,
    confirm: bool,
) -> None:
    """Preview eligible plan settlements and optionally apply the confirmed set.

    Args:
        db_url: Configured database URL.
        target_database: Validated credential-free database endpoint.
        mode_filter: `live`, `paper`, or None for all modes.
        expected_digest: Optional or confirmation-bound SHA-256 digest.
        confirm: Whether to execute the SCD2 status transitions.
    """
    code_revision = settlement_code_revision((sys.modules[__name__],))
    repository = configured_repository(db_url)
    try:
        preview = await preview_plan_settlements(
            repository,
            mode=mode_filter,
            target_database=target_database,
            code_revision=code_revision,
        )
        _echo_preview(preview)
        if expected_digest is not None and preview.digest != expected_digest:
            refuse(
                f"refused: --expect-digest {expected_digest} differs from "
                f"current preview_sha256 {preview.digest}"
            )
        if not preview.candidates:
            typer.echo("nothing to do; settled 0 execution plans")
            return
        if not confirm:
            typer.echo(
                "DRY RUN: nothing was written; pass "
                f"--confirm --expect-digest {preview.digest} to settle these plans"
            )
            return
        settled_at = datetime.now(UTC)
        session_id = f"cli-settle-plans-{uuid4()}"
        receipt = await apply_plan_settlements(
            repository,
            preview,
            settled_at=settled_at,
            session_id=session_id,
        )
        typer.echo(f"settled {receipt.count} execution plan(s) at {receipt.settled_at.isoformat()}")
        payload: JsonObject = {
            "code_revision": receipt.code_revision,
            "count": receipt.count,
            "mode": receipt.mode,
            "preview_sha256": receipt.digest,
            "residual_window": RESIDUAL_VENUE_EVENT_WINDOW_NOTE,
            "run_order_requirement": RUN_ORDER_REQUIREMENT_NOTE,
            "session_id": receipt.session_id,
            "settled_at": receipt.settled_at.isoformat(),
            "settled_plans": [serialize_candidate(item) for item in receipt.settled_candidates],
            "settled_public_ids": list(receipt.settled_public_ids),
            "target_database": receipt.target_database,
        }
        typer.echo(json.dumps(payload, sort_keys=True))
    except PlanSettlementDriftError as error:
        refuse(f"refused: {error}")
    finally:
        await dispose_repositories()


def settle_plans(
    mode: Annotated[
        str,
        typer.Option(
            "--mode",
            help="Execution mode scope to inspect: live, paper, or all.",
        ),
    ] = "live",
    expect_digest: Annotated[
        str | None,
        typer.Option(
            "--expect-digest",
            help="Expected 64-character hex preview_sha256 from a prior dry run.",
        ),
    ] = None,
    confirm: Annotated[
        bool,
        typer.Option(
            "--confirm",
            help=(
                "Apply the SCD2 transitions bound to --expect-digest "
                "(requires plan_executor stopped or restarted immediately)."
            ),
        ),
    ] = False,
) -> None:
    """Preview or SCD2-settle execution plans whose child orders are terminal.

    Without `--confirm`, prints the target database identity, operational
    requirements, candidate plan transitions, skipped actionable plans, and
    `preview_sha256` without writing. With `--confirm`, requires
    `--expect-digest` matching `preview_sha256` and writes all transitions
    atomically in one database transaction.

    Args:
        mode: Execution mode filter (`live`, `paper`, or `all`).
        expect_digest: Asserted 64-character hexadecimal SHA-256 preview digest.
        confirm: Separately authorize applying the previewed transitions.
    """
    mode_filter = _normalize_mode(mode)
    expected_digest = _normalize_digest(expect_digest, confirm=confirm)
    db_url = get_bootstrap_settings().db_url
    try:
        target_database = settlement_database_endpoint(db_url)
    except PlanSettlementConfigError as error:
        refuse(f"refused: {error}")
    typer.echo(f"target database: {database_identity(db_url)}")
    asyncio.run(
        _run_settlement(
            db_url,
            target_database,
            mode_filter,
            expected_digest,
            confirm=confirm,
        )
    )


def main(argv: list[str] | None = None) -> int:
    """Execute `settle-plans` as a standalone module CLI entrypoint.

    Args:
        argv: Optional command-line arguments; defaults to `sys.argv[1:]`.

    Returns:
        Exit status code (`0` on success, `1` on refusal).
    """
    cli = typer.Typer(add_completion=False)
    cli.command(name="settle-plans")(settle_plans)
    exit_code: int | None = cli(
        args=argv,
        prog_name="python -m snapper.cli.plan_settlement",
        standalone_mode=False,
    )
    return 0 if exit_code is None else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
