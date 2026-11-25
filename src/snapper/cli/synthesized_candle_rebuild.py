"""Operator command for guarded reconstruction and verification of synthesized candles."""

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid7

import typer

from snapper.application.updaters.historical import synthesized_candle_rebuild as rebuild
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import prepare_output_dir
from snapper.application.updaters.historical.guarded_candle_write import WriteOptions
from snapper.cli.operator_support import WRITE_OPTIONS
from snapper.cli.operator_support import RefusingParser
from snapper.cli.operator_support import UsageError
from snapper.cli.operator_support import add_write_arguments
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import failure_text
from snapper.cli.operator_support import refuse
from snapper.cli.operator_support import write_options
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories

_POOL = {"DB_POOL_SIZE": "1", "DB_MAX_OVERFLOW": "0"}


@dataclass(frozen=True, slots=True)
class _Invocation:
    """The command arguments and the invocation's fixed identity."""

    request: rebuild.RebuildRequest
    writing: WriteOptions | None
    run_id: str


def _parser() -> argparse.ArgumentParser:
    """Declare the rebuild options with the live-edge guard and run-order note."""
    parser = RefusingParser(
        prog="snapper rebuild-synthesized-candles",
        description="Rebuild higher-timeframe Kraken spot candles from recorded repairs.",
        epilog=(
            "For recent data, choose an --end at or before the UTC day floor "
            "of (run start minus 719 minutes) to stay clear of Kraken catchup."
        ),
        allow_abbrev=False,
    )
    parser.add_argument("--repair-run", type=Path, action="append", required=True)
    parser.add_argument("--run-records", type=Path, required=True)
    parser.add_argument(
        "--end", required=True, help="UTC minute at least 180 minutes before the run"
    )
    parser.add_argument("--output-dir", type=Path, required=True, help="New or empty directory")
    parser.add_argument("--verify-receipt", type=Path, help="Rebuild write directory to verify")
    parser.add_argument("--verify-record", type=Path, help="Run record for the rebuild write")
    add_write_arguments(parser)
    return parser


def _verification(values: dict[str, object]) -> rebuild.VerificationFiles | None:
    """Require both verification options, and only for a dry run."""
    directory = cast(Path | None, values["verify_receipt"])
    record = cast(Path | None, values["verify_record"])
    if directory is None and record is None:
        return None
    if values["confirm"]:
        raise RepairRefusedError("verification options are only available in a dry run")
    if directory is None or record is None:
        raise RepairRefusedError("--verify-receipt and --verify-record must be supplied together")
    return rebuild.VerificationFiles(directory, record)


def _arguments(argv: Sequence[str], run_id: str) -> _Invocation:
    """Parse a complete command invocation before constructing a repository."""
    try:
        values = cast(dict[str, object], vars(_parser().parse_args(list(argv))))
    except UsageError as error:
        raise RepairRefusedError("invalid rebuild options (see --help)") from error
    try:
        end = datetime.fromisoformat(cast(str, values["end"]))
    except ValueError as error:
        raise RepairRefusedError("--end is not an ISO 8601 time") from error
    rebuild.validate_end(end, datetime.now(UTC))
    verification = _verification(values)
    writing = write_options(
        cast(bool, values["confirm"]),
        {name: values[name] for name, _ in WRITE_OPTIONS if values[name] is not None},
    )
    return _Invocation(
        rebuild.RebuildRequest(
            tuple(cast(list[Path], values["repair_run"])),
            cast(Path, values["run_records"]),
            end,
            cast(Path, values["output_dir"]),
            verification,
        ),
        writing,
        run_id,
    )


def _repository(db_url: str, run_id: str) -> SQLAlchemyRepository:
    """Set the pool clamp for construction of the named engine."""
    prior = {name: os.environ.get(name) for name in _POOL}
    os.environ.update(_POOL)
    try:
        return configured_repository(db_url, application_name=f"snapper:rebuild-synth:{run_id}")
    finally:
        for name, value in prior.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _print_summary(summary: JsonObject) -> None:
    """Print action and verification counts without reproducing evidence details."""
    counts = {name: summary[name] for name in ("actions", "by_timeframe", "by_day")}
    counts["runs"] = [
        {
            name: row[name]
            for name in ("run_id", "applied", "applied_superseded", "resolved_uncertain")
        }
        for row in cast(list[JsonObject], summary["runs"])
    ]
    if "verification" in summary:
        counts["verification"] = {
            run_id: cast(JsonObject, observed)["counts"]
            for run_id, observed in cast(JsonObject, summary["verification"]).items()
        }
        typer.echo(
            "Verified and regressed counts cover write targets at their recorded snapshots. "
            "The other counts account for the rest of the derived scope."
        )
    typer.echo(json.dumps(counts, sort_keys=True))


async def _execute(context: RunContext, invocation: _Invocation) -> int:
    """Run the requested mode, print its status and dispose the operator repository."""
    request, options = invocation.request, invocation.writing
    try:
        if options is None:
            artifacts = await rebuild.plan_rebuild(context, request)
            typer.echo(f"digest: {artifacts.digest}")
            _print_summary(artifacts.summary)
            typer.echo("DRY RUN: nothing was written")
            return 0
        report = await rebuild.write_rebuild(context, request, options)
        typer.echo(f"voted digest: {report.voted_digest}")
        typer.echo(f"rebuilt plan digest: {report.plan_digest}")
        typer.echo(f"totals: {json.dumps(report.totals, sort_keys=True)}")
        typer.echo(f"stop reason: {report.stop_reason}")
        if not report.complete:
            typer.echo(f"stopped: {report.stop_reason}; see receipt.json", err=True)
            return 1
        return 0
    finally:
        await dispose_repositories()


def _run(db_url: str, invocation: _Invocation) -> int:
    """Bind the command to its credential-free endpoint and installed code."""
    typer.echo(f"target database: {database_identity(db_url)}")
    endpoint = database_endpoint(db_url)
    prepare_output_dir(invocation.request.output_dir)
    revision = rebuild.code_revision(sys.modules[__name__])
    context = RunContext(
        _repository(db_url, invocation.run_id), endpoint, revision, run_id=invocation.run_id
    )
    return asyncio.run(_execute(context, invocation))


def rebuild_synthesized_candles(ctx: typer.Context) -> None:
    """Preview, apply or verify guarded higher-timeframe candle rebuilds.

    Args:
        ctx: Typer context carrying the rebuild command arguments.

    Raises:
        typer.Exit: Carries the command's exit status.
    """
    run_id = str(uuid7())
    try:
        invocation = _arguments(ctx.args, run_id)
        code = _run(get_bootstrap_settings().db_url, invocation)
    except typer.Exit:
        raise
    except RepairRefusedError as error:
        refuse(f"refused: {error}")
    except Exception as error:
        refuse(f"failed: {failure_text(error)}")
    raise typer.Exit(code)
