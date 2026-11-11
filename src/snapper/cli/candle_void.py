"""Operator CLI that voids the active candles of retired Kraken instruments.

``snapper void-candles`` closes every active candle of the given instruments, in
every timeframe, at one database instant, under the candle writer fence. The
default is a dry run that writes ``preview.csv``, ``summary.json`` and
``approval.json`` and prints the digest to vote on. A write needs ``--confirm``
together with the voted run's directory and its digest. ``--reconcile <run dir>``
settles a write whose commit acknowledgement was lost.

The command reads its argument tail with :mod:`argparse`; every parse error is a
refusal. Neither output nor refusals ever print the database URL or an error's
text.
"""

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import NoReturn
from typing import cast

import typer

from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import error_fields
from snapper.application.updaters.historical.candle_repair import prepare_output_dir
from snapper.application.updaters.historical.candle_void import SUCCESS_STATUSES
from snapper.application.updaters.historical.candle_void import EvidenceFile
from snapper.application.updaters.historical.candle_void import VoidContext
from snapper.application.updaters.historical.candle_void import VoidOutcome
from snapper.application.updaters.historical.candle_void import VoidRefusedError
from snapper.application.updaters.historical.candle_void import VoidRequest
from snapper.application.updaters.historical.candle_void import VoteBinding
from snapper.application.updaters.historical.candle_void import plan_void
from snapper.application.updaters.historical.candle_void import reconcile_void
from snapper.application.updaters.historical.candle_void import void_code_revision
from snapper.application.updaters.historical.candle_void import write_void
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import refuse
from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import dispose_repositories

VOID_CANDLES_CONTEXT: Final[dict[str, object]] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}
"""Click settings that hand every argument, ``--help`` included, to the parser."""

_DIGEST: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{64}")
_EVIDENCE: Final[re.Pattern[str]] = re.compile(r"([a-z][a-z0-9_]*)=(.+)")


class _UsageError(Exception):
    """An argument the parser does not accept."""


class _RefusingParser(argparse.ArgumentParser):
    """Raise on a usage error instead of exiting, so the command can refuse."""

    def error(self, message: str) -> NoReturn:
        """Report a usage error to the caller.

        Args:
            message: Parser's description of the error.

        Raises:
            _UsageError: Always.
        """
        raise _UsageError(message)


def _parser() -> argparse.ArgumentParser:
    """Declare the command's options.

    Returns:
        The parser.
    """
    parser = _RefusingParser(
        prog="snapper void-candles",
        description=(
            "Void every active candle of retired Kraken instruments at one database instant. "
            "Without --confirm nothing is written."
        ),
        allow_abbrev=False,
    )
    parser.add_argument("--instrument", action="append", default=[], help="Instrument public id")
    parser.add_argument(
        "--evidence", action="append", default=[], help="name=path of an evidence file"
    )
    parser.add_argument("--output-dir", type=Path, help="New or empty directory")
    parser.add_argument("--confirm", action="store_true", help="Close the voted candles")
    parser.add_argument("--voted-preview", type=Path, help="Output directory of the voted run")
    parser.add_argument("--expect-digest", help="sha256 of the voted approval.json")
    parser.add_argument("--reconcile", type=Path, help="Settle the write run in this directory")
    return parser


@dataclass(frozen=True, slots=True)
class _Arguments:
    """Parsed arguments, typed but not yet cross-checked.

    Attributes:
        instruments: Requested instrument ids, as given.
        evidence: Requested evidence, as given.
        output_dir: Output directory, if any.
        confirm: Whether ``--confirm`` was given.
        voted_preview: Voted directory, if any.
        expect_digest: Expected digest, if any.
        reconcile: Run directory to reconcile, if any.
    """

    instruments: tuple[str, ...]
    evidence: tuple[str, ...]
    output_dir: Path | None
    confirm: bool
    voted_preview: Path | None
    expect_digest: str | None
    reconcile: Path | None


def _arguments(argv: Sequence[str]) -> _Arguments:
    """Parse the argument tail; a usage error refuses.

    Args:
        argv: Argument tail.

    Returns:
        The parsed arguments.
    """
    try:
        namespace = _parser().parse_args(list(argv))
    except _UsageError as error:
        refuse(f"refused: {error}")
    values = cast(dict[str, object], vars(namespace))
    return _Arguments(
        instruments=tuple(cast(list[str], values["instrument"])),
        evidence=tuple(cast(list[str], values["evidence"])),
        output_dir=cast(Path | None, values["output_dir"]),
        confirm=cast(bool, values["confirm"]),
        voted_preview=cast(Path | None, values["voted_preview"]),
        expect_digest=cast(str | None, values["expect_digest"]),
        reconcile=cast(Path | None, values["reconcile"]),
    )


def _evidence(values: Sequence[str]) -> tuple[EvidenceFile, ...]:
    """Parse ``name=path`` evidence arguments.

    Args:
        values: Raw ``--evidence`` values.

    Returns:
        The evidence files.
    """
    files: list[EvidenceFile] = []
    for value in values:
        match = _EVIDENCE.fullmatch(value)
        if match is None:
            refuse(f"refused: --evidence must be name=path, got {value!r}")
        files.append(EvidenceFile(name=match.group(1), path=Path(match.group(2))))
    return tuple(files)


def _request(arguments: _Arguments) -> VoidRequest:
    """Cross-check a dry run's or a write's options into a request.

    Args:
        arguments: Parsed arguments.

    Returns:
        A validated request.
    """
    instruments = tuple(sorted({value.strip() for value in arguments.instruments}))
    if not instruments or any(not value for value in instruments):
        refuse("refused: at least one non-empty --instrument is required")
    if arguments.output_dir is None:
        refuse("refused: --output-dir is required")
    return VoidRequest(
        instrument_public_ids=instruments,
        evidence=_evidence(arguments.evidence),
        output_dir=arguments.output_dir,
    )


def _binding(arguments: _Arguments) -> VoteBinding | None:
    """Return the vote of a confirmed run; refuse write options without ``--confirm``.

    Args:
        arguments: Parsed arguments.

    Returns:
        The vote, or None for a dry run.
    """
    given = arguments.voted_preview is not None or arguments.expect_digest is not None
    if not arguments.confirm:
        if given:
            refuse("refused: --voted-preview and --expect-digest only apply with --confirm")
        return None
    digest = (arguments.expect_digest or "").lower()
    if arguments.voted_preview is None or _DIGEST.fullmatch(digest) is None:
        refuse("refused: --confirm needs --voted-preview and a sha256 --expect-digest")
    return VoteBinding(voted_dir=arguments.voted_preview, expect_digest=digest)


def _reconcile_only(arguments: _Arguments) -> Path | None:
    """Return the run directory of ``--reconcile``, which takes no other option.

    Args:
        arguments: Parsed arguments.

    Returns:
        The run directory, or None when ``--reconcile`` is absent.
    """
    if arguments.reconcile is None:
        return None
    others = (
        arguments.instruments
        or arguments.evidence
        or arguments.output_dir is not None
        or arguments.confirm
        or arguments.voted_preview is not None
        or arguments.expect_digest is not None
    )
    if others:
        refuse("refused: --reconcile takes no other option")
    return arguments.reconcile


def _failure_text(error: BaseException) -> str:
    """Describe an unexpected failure by class and SQLSTATE only.

    Args:
        error: The failure.

    Returns:
        Its class name and SQLSTATE, never its text.
    """
    fields = error_fields(error)
    sqlstate = fields["sqlstate"]
    return f"{fields['error']}" + (f" sqlstate={sqlstate}" if sqlstate else "")


def _report(outcome: VoidOutcome) -> int:
    """Print an outcome and return its exit code.

    Args:
        outcome: Outcome of a write or a reconciliation.

    Returns:
        0 when the voted candles are closed, 1 otherwise.
    """
    typer.echo(f"outcome: {outcome.status}")
    typer.echo(json.dumps(outcome.record, indent=2, sort_keys=True))
    if outcome.status in SUCCESS_STATUSES:
        return 0
    typer.echo(f"stopped: {outcome.status}", err=True)
    return 1


async def _execute(
    context: VoidContext,
    request: VoidRequest | None,
    binding: VoteBinding | None,
    reconcile: Path | None,
) -> int:
    """Run the dry run, the write or the reconciliation and return the exit code.

    Args:
        context: Run context.
        request: Validated request, or None for a reconciliation.
        binding: The vote, or None.
        reconcile: Run directory to reconcile, or None.

    Returns:
        The exit code.
    """
    try:
        if reconcile is not None:
            return _report(await reconcile_void(context, reconcile))
        assert request is not None
        if binding is None:
            plan = await plan_void(context, request)
            typer.echo(f"digest: {plan.digest}")
            typer.echo(json.dumps(plan.summary, indent=2, sort_keys=True))
            typer.echo(
                f"DRY RUN: nothing was written; files are in {request.output_dir}. To write, "
                "vote on the digest and rerun with --confirm --voted-preview <dir> "
                "--expect-digest <digest> and the same --instrument and --evidence"
            )
            return 0
        return _report(await write_void(context, request, binding))
    finally:
        await dispose_repositories()


def void_candles(ctx: typer.Context) -> None:
    """Preview or apply the guarded void of retired Kraken instruments' candles.

    Options, read by the command's own parser (``--help`` lists them):
    ``--instrument <public id>`` (repeatable); ``--evidence <name>=<path>``
    (repeatable, ``writer_cessation`` required); ``--output-dir``, new or empty.
    Writing also takes ``--confirm --voted-preview <dir> --expect-digest
    <sha256>``. ``--reconcile <run dir>`` alone settles an earlier write.

    Args:
        ctx: Context whose extra arguments are the command's options.

    Raises:
        typer.Exit: With the command's exit code; refusals exit with 1.
    """
    arguments = _arguments(ctx.args)
    reconcile = _reconcile_only(arguments)
    request = None if reconcile is not None else _request(arguments)
    binding = None if reconcile is not None else _binding(arguments)
    db_url = get_bootstrap_settings().db_url
    try:
        typer.echo(f"target database: {database_identity(db_url)}")
        if request is not None:
            prepare_output_dir(request.output_dir)
        context = VoidContext(
            repository=configured_repository(db_url),
            database=database_endpoint(db_url),
            code_revision=void_code_revision([sys.modules[__name__]]),
        )
        code = asyncio.run(_execute(context, request, binding, reconcile))
    except (RepairRefusedError, VoidRefusedError) as error:
        refuse(f"refused: {error}")
    except Exception as error:
        refuse(f"failed: {_failure_text(error)}")
    raise typer.Exit(code=code)
