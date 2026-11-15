"""Operator CLI that repairs the evidence of stale cancel commands (G3-C).

``snapper repair-cancel-targets`` evaluates each named cancel command and, by
default, only writes ``preview.json`` and ``summary.json`` and prints the
digest to vote on. A write needs ``--confirm`` with the voted run's
directory and its digest; it recomputes the plan, refuses unless it equals
the vote byte for byte, and then records the missing ``order_terminal`` of a
venue-cancelled Kraken spot order, or fails a never-submitted Walutomat
cancel. Neither output nor refusals ever print the database URL or an
error's text.
"""

import asyncio
import json
import re
import sys
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Annotated
from typing import Final
from uuid import UUID

import typer

from snapper.application.trade.cancel_target_repair import CancelTargetRepairRefusedError
from snapper.application.trade.cancel_target_repair import RepairPlan
from snapper.application.trade.cancel_target_repair import WriteSession
from snapper.application.trade.cancel_target_repair import apply_write
from snapper.application.trade.cancel_target_repair import code_revision
from snapper.application.trade.cancel_target_repair import plan_repair
from snapper.application.trade.cancel_target_repair import prepare_write
from snapper.application.trade.cancel_target_repair import receipt_document
from snapper.application.trade.cancel_target_repair import refuse_duplicates
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import prepare_output_dir
from snapper.application.updaters.historical.candle_repair import write_exclusive
from snapper.cli import operator_support
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import refuse
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories

_DIGEST: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{64}")
_WRITE_RESULTS_OK: Final[frozenset[str]] = frozenset(
    {"recorded", "failed", "already_recorded", "nothing_to_write"}
)
"""Line results after which a write counts as complete."""


def _document_bytes(document: JsonValue) -> bytes:
    """Render a document as indented, key-sorted JSON with a final newline."""
    return (json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def canonical_cancel_ids(values: Sequence[str]) -> list[str]:
    """Validate the named cancel ids and keep their order.

    Args:
        values: The ``--cancel`` values as given.

    Returns:
        Each id in canonical UUID spelling, in the given order.

    Raises:
        CancelTargetRepairRefusedError: For a malformed, missing or repeated id.
    """
    canonical: list[str] = []
    for value in values:
        try:
            canonical.append(str(UUID(value)))
        except ValueError as error:
            raise CancelTargetRepairRefusedError(f"--cancel is not a UUID: {value!r}") from error
    refuse_duplicates(canonical)
    return canonical


def write_target(
    confirm: bool, voted_preview: Path | None, expect_digest: str | None
) -> tuple[Path, str] | None:
    """Cross-check the write options.

    Args:
        confirm: Whether ``--confirm`` was given.
        voted_preview: The voted run's directory, if given.
        expect_digest: The voted digest, if given.

    Returns:
        The voted directory and digest for a write, or ``None`` for a dry run.

    Raises:
        CancelTargetRepairRefusedError: When the options do not match.
    """
    if not confirm:
        if voted_preview is not None or expect_digest is not None:
            raise CancelTargetRepairRefusedError(
                "--voted-preview and --expect-digest only apply with --confirm"
            )
        return None
    digest = (expect_digest or "").lower()
    if voted_preview is None or _DIGEST.fullmatch(digest) is None:
        raise CancelTargetRepairRefusedError(
            "--confirm needs --voted-preview and a sha256 --expect-digest"
        )
    return voted_preview, digest


def _write_plan_files(output_dir: Path, plan: RepairPlan) -> None:
    """Write the preview and the summary of a plan."""
    write_exclusive(output_dir / "preview.json", _document_bytes(plan.preview))
    write_exclusive(output_dir / "summary.json", _document_bytes(plan.summary))


def _write_receipt(output_dir: Path, session: WriteSession) -> None:
    """Write the receipt of a write, complete or interrupted, and then its plan files."""
    write_exclusive(output_dir / "receipt.json", _document_bytes(receipt_document(session)))
    _write_plan_files(output_dir, session.plan)


async def _dry_run(
    repository: SQLAlchemyRepository,
    cancel_ids: list[str],
    binding: tuple[JsonObject, JsonObject],
) -> RepairPlan:
    """Plan the repair without writing to the database."""
    database, code = binding
    return await plan_repair(repository, cancel_ids, database=database, code=code)


async def execute(
    repository: SQLAlchemyRepository,
    cancel_ids: list[str],
    output_dir: Path,
    voted: tuple[Path, str] | None,
    binding: tuple[JsonObject, JsonObject],
) -> int:
    """Run the dry run or the write, print the outcome and return the exit code.

    Args:
        repository: Repository to read and write.
        cancel_ids: Validated cancel public ids.
        output_dir: Prepared, empty output directory.
        voted: The voted directory and digest, or ``None`` for a dry run.
        binding: The database identity and the code revision of the run.

    Returns:
        0 when the run completed; 1 when a write left a line unwritten.
    """
    try:
        if voted is None:
            plan = await _dry_run(repository, cancel_ids, binding)
            _write_plan_files(output_dir, plan)
            typer.echo(f"digest: {plan.digest}")
            typer.echo(json.dumps(plan.summary["counts"], sort_keys=True))
            typer.echo(
                f"DRY RUN: nothing was written; files are in {output_dir}. To write, vote on "
                "the digest and rerun with --confirm --voted-preview <dir> --expect-digest <digest>"
            )
            return 0
        session = await prepare_write(repository, cancel_ids, voted, binding, datetime.now(UTC))
        typer.echo(f"write session: {session.session_id}")
        try:
            await apply_write(repository, session)
        except BaseException:
            typer.echo(
                f"write interrupted: the rows carrying session {session.session_id} are the "
                "authority for what was written"
            )
            raise
        finally:
            _write_receipt(output_dir, session)
        typer.echo(json.dumps(session.results, sort_keys=True))
        complete = all(
            isinstance(result, dict) and result.get("result") in _WRITE_RESULTS_OK
            for result in session.results
        )
        return 0 if complete else 1
    finally:
        await dispose_repositories()


def _binding(db_url: str) -> tuple[JsonObject, JsonObject]:
    """Bind the run to its database and to its code."""
    return database_endpoint(db_url), code_revision([sys.modules[__name__], operator_support])


def repair_cancel_targets(
    cancel: Annotated[
        list[str], typer.Option("--cancel", help="Public id of a cancel command; repeatable")
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", help="New or empty directory")],
    confirm: Annotated[bool, typer.Option("--confirm", help="Write the voted plan")] = False,
    voted_preview: Annotated[
        Path | None, typer.Option("--voted-preview", help="Output directory of the voted run")
    ] = None,
    expect_digest: Annotated[
        str | None, typer.Option("--expect-digest", help="sha256 of the voted plan")
    ] = None,
) -> None:
    """Preview or apply the evidence repair of stale cancel commands.

    Args:
        cancel: Public ids of the cancel commands, in the order to evaluate.
        output_dir: New or empty directory for the preview, summary and receipt.
        confirm: Whether to write the voted plan.
        voted_preview: Output directory of the voted dry run.
        expect_digest: The voted digest.

    Raises:
        typer.Exit: With the command's exit code; refusals exit with 1.
    """
    try:
        cancel_ids = canonical_cancel_ids(cancel)
        voted = write_target(confirm, voted_preview, expect_digest)
        db_url = get_bootstrap_settings().db_url
        typer.echo(f"target database: {database_identity(db_url)}")
        prepare_output_dir(output_dir)
        binding = _binding(db_url)
        code = asyncio.run(
            execute(configured_repository(db_url), cancel_ids, output_dir, voted, binding)
        )
    except (CancelTargetRepairRefusedError, RepairRefusedError) as error:
        refuse(f"refused: {error}")
    except Exception as error:
        refuse(f"failed: {type(error).__name__}")
    raise typer.Exit(code=code)
