"""Operator CLI that repairs absent and false-zero one-minute candles.

``snapper repair-candles`` takes the correct bars from a saved Kraken REST
OHLC snapshot or from stored trades and writes them only through the
guarded repository writers, which never close, update or replace a native or
calculated row. The default is a dry run that writes ``preview.csv``,
``approval.json`` and ``summary.json`` and prints the digest to vote on. A
write needs ``--confirm`` together with the voted run's directory and its
digest, and applies only the voted rows that still match the rebuilt plan.

The command has more options than a typed function may take, so it reads
its argument tail with :mod:`argparse`; every parse error is a refusal.
Neither output nor refusals ever print the database URL or an error's text.
"""

import argparse
import asyncio
import json
import math
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final
from typing import NoReturn
from typing import cast

import typer

from snapper.application.updaters.historical.candle_repair import SNAPSHOT_EXCHANGE
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RepairRequest
from snapper.application.updaters.historical.candle_repair import RepairSource
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import WriteOptions
from snapper.application.updaters.historical.candle_repair import code_revision
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import error_fields
from snapper.application.updaters.historical.candle_repair import plan_repair
from snapper.application.updaters.historical.candle_repair import prepare_output_dir
from snapper.application.updaters.historical.candle_repair import write_repair
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import refuse
from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import dispose_repositories

REPAIR_CANDLES_CONTEXT: Final[dict[str, object]] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}
"""Click settings that hand every argument, ``--help`` included, to the parser."""

_DIGEST: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{64}")
_DEFAULT_CHUNK_PAUSE_SECONDS: Final[float] = 1.0
_DEFAULT_MAX_CONSECUTIVE_LOCK_TIMEOUTS: Final[int] = 3
_WRITE_OPTIONS: Final[tuple[tuple[str, str], ...]] = (
    ("voted_preview", "--voted-preview"),
    ("expect_digest", "--expect-digest"),
    ("max_rows", "--max-rows"),
    ("chunk_pause_seconds", "--chunk-pause-seconds"),
    ("max_consecutive_lock_timeouts", "--max-consecutive-lock-timeouts"),
)


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
    """Declare the command's options."""
    parser = _RefusingParser(
        prog="snapper repair-candles",
        description=(
            "Repair absent and false-zero 1m candles from a saved Kraken OHLC snapshot or "
            "from stored trades. Without --confirm nothing is written."
        ),
        allow_abbrev=False,
    )
    parser.add_argument("--source", required=True, choices=[item.value for item in RepairSource])
    parser.add_argument("--snapshot-dir", type=Path, help="Snapshot root (snapshot source)")
    parser.add_argument("--exchange", help="Exchange of the stored trades (trades source)")
    parser.add_argument("--symbol", action="append", default=[], help="Native symbol; repeatable")
    parser.add_argument(
        "--all-mapped", action="store_true", help="Every mapped snapshot file (snapshot source)"
    )
    parser.add_argument("--start", required=True, help="Inclusive UTC minute, ISO 8601")
    parser.add_argument("--end", required=True, help="Exclusive UTC minute, ISO 8601")
    parser.add_argument("--output-dir", required=True, type=Path, help="New or empty directory")
    parser.add_argument("--confirm", action="store_true", help="Write the voted rows")
    parser.add_argument("--voted-preview", type=Path, help="Output directory of the voted run")
    parser.add_argument("--expect-digest", help="sha256 of the voted approval.json")
    parser.add_argument("--max-rows", type=int, help="Most rows this write applies")
    parser.add_argument("--chunk-pause-seconds", type=float, help="Pause between chunks (1.0)")
    parser.add_argument(
        "--max-consecutive-lock-timeouts", type=int, help="Lock timeouts that stop a write (3)"
    )
    return parser


@dataclass(frozen=True, slots=True)
class _Arguments:
    """Parsed arguments, typed but not yet cross-checked."""

    source: RepairSource
    snapshot_dir: Path | None
    exchange: str | None
    symbols: tuple[str, ...]
    all_mapped: bool
    start: str
    end: str
    output_dir: Path
    confirm: bool
    writing: dict[str, object]


def _arguments(argv: Sequence[str]) -> _Arguments:
    """Parse the argument tail; a usage error refuses."""
    try:
        namespace = _parser().parse_args(list(argv))
    except _UsageError as error:
        refuse(f"refused: {error}")
    values = cast(dict[str, object], vars(namespace))
    return _Arguments(
        source=RepairSource(cast(str, values["source"])),
        snapshot_dir=cast(Path | None, values["snapshot_dir"]),
        exchange=cast(str | None, values["exchange"]),
        symbols=tuple(cast(list[str], values["symbol"])),
        all_mapped=cast(bool, values["all_mapped"]),
        start=cast(str, values["start"]),
        end=cast(str, values["end"]),
        output_dir=cast(Path, values["output_dir"]),
        confirm=cast(bool, values["confirm"]),
        writing={name: values[name] for name, _ in _WRITE_OPTIONS if values[name] is not None},
    )


def _instant(option: str, value: str) -> datetime:
    """Parse an ISO time; the window checks it is an aware UTC minute."""
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        refuse(f"refused: {option} is not an ISO 8601 time: {value!r}")


def _symbols(arguments: _Arguments) -> tuple[str, ...]:
    """Return the requested symbols, sorted and unique, or none with ``--all-mapped``."""
    symbols = tuple(sorted({symbol.strip() for symbol in arguments.symbols}))
    if any(not symbol for symbol in symbols):
        refuse("refused: --symbol must not be empty")
    if arguments.all_mapped == bool(symbols):
        refuse("refused: pass either --symbol (repeatable) or --all-mapped")
    return symbols


def _source_options(arguments: _Arguments) -> tuple[str, Path | None]:
    """Check the options that belong to the chosen source and return its exchange."""
    if arguments.source is RepairSource.SNAPSHOT:
        if arguments.snapshot_dir is None or arguments.exchange is not None:
            refuse("refused: --source snapshot takes --snapshot-dir and no --exchange")
        return SNAPSHOT_EXCHANGE, arguments.snapshot_dir
    if arguments.exchange is None or arguments.snapshot_dir is not None or arguments.all_mapped:
        refuse("refused: --source trades takes --exchange, and no --snapshot-dir or --all-mapped")
    return arguments.exchange, None


def _request(arguments: _Arguments) -> RepairRequest:
    """Cross-check the parsed options into a request.

    Args:
        arguments: Parsed arguments.

    Returns:
        A validated request.
    """
    exchange, snapshot_dir = _source_options(arguments)
    symbols = _symbols(arguments)
    try:
        window = RepairWindow(
            _instant("--start", arguments.start), _instant("--end", arguments.end)
        )
    except RepairRefusedError as error:
        refuse(f"refused: {error}")
    return RepairRequest(
        source=arguments.source,
        exchange=exchange,
        window=window,
        symbols=symbols,
        all_mapped=arguments.all_mapped,
        snapshot_dir=snapshot_dir,
        output_dir=arguments.output_dir,
    )


def _write_options(arguments: _Arguments) -> WriteOptions | None:
    """Return the write options of a confirmed run; refuse them without ``--confirm``."""
    writing = arguments.writing
    if not arguments.confirm:
        if writing:
            given = [option for name, option in _WRITE_OPTIONS if name in writing]
            refuse(f"refused: {', '.join(given)} only apply with --confirm")
        return None
    digest = str(writing.get("expect_digest", "")).lower()
    if "voted_preview" not in writing or _DIGEST.fullmatch(digest) is None:
        refuse("refused: --confirm needs --voted-preview and a sha256 --expect-digest")
    max_rows = cast(int | None, writing.get("max_rows"))
    pause = cast(float, writing.get("chunk_pause_seconds", _DEFAULT_CHUNK_PAUSE_SECONDS))
    lock_timeouts = cast(
        int,
        writing.get("max_consecutive_lock_timeouts", _DEFAULT_MAX_CONSECUTIVE_LOCK_TIMEOUTS),
    )
    if (max_rows is not None and max_rows < 1) or not math.isfinite(pause) or pause < 0:
        refuse("refused: --max-rows must be positive and --chunk-pause-seconds finite and >= 0")
    if lock_timeouts < 1:
        refuse("refused: --max-consecutive-lock-timeouts must be at least 1")
    return WriteOptions(
        voted_preview=cast(Path, writing["voted_preview"]),
        expect_digest=digest,
        max_rows=max_rows,
        chunk_pause_seconds=pause,
        max_consecutive_lock_timeouts=lock_timeouts,
    )


def _failure_text(error: BaseException) -> str:
    """Describe an unexpected failure by class and SQLSTATE only."""
    fields = error_fields(error)
    sqlstate = fields["sqlstate"]
    return f"{fields['error']}" + (f" sqlstate={sqlstate}" if sqlstate else "")


async def _execute(
    context: RunContext, request: RepairRequest, options: WriteOptions | None
) -> int:
    """Run the dry run or the write, print its outcome and return the exit code."""
    try:
        if options is None:
            artifacts = await plan_repair(context, request)
            typer.echo(f"digest: {artifacts.digest}")
            typer.echo(json.dumps(artifacts.summary, indent=2, sort_keys=True))
            typer.echo(
                f"DRY RUN: nothing was written; files are in {request.output_dir}. To write, "
                "vote on the digest and rerun with --confirm --voted-preview <dir> "
                "--expect-digest <digest>"
            )
            return 0
        report = await write_repair(context, request, options)
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


def _run(db_url: str, request: RepairRequest, options: WriteOptions | None) -> int:
    """Bind the run to its database and code, then execute it.

    Args:
        db_url: Configured database URL; it is never printed.
        request: Validated request.
        options: Write options, or None for a dry run.

    Returns:
        The exit code.
    """
    typer.echo(f"target database: {database_identity(db_url)}")
    prepare_output_dir(request.output_dir)
    context = RunContext(
        repository=configured_repository(db_url),
        database=database_endpoint(db_url),
        code_revision=code_revision([sys.modules[__name__]]),
    )
    return asyncio.run(_execute(context, request, options))


def repair_candles(ctx: typer.Context) -> None:
    """Preview or apply guarded repairs of absent and false-zero 1m candles.

    Options, read by the command's own parser (``--help`` lists them):
    ``--source snapshot --snapshot-dir <dir>`` or ``--source trades --exchange
    <name>``; ``--symbol`` (repeatable) or, for the snapshot, ``--all-mapped``;
    ``--start`` and ``--end``, UTC minutes of a half-open window ending at least
    180 minutes before the run; ``--output-dir``, new or empty. Writing also
    takes ``--confirm --voted-preview <dir> --expect-digest <sha256>`` and
    optionally ``--max-rows``, ``--chunk-pause-seconds`` (1.0) and
    ``--max-consecutive-lock-timeouts`` (3).

    Args:
        ctx: Context whose extra arguments are the command's options.

    Raises:
        typer.Exit: With the command's exit code; refusals exit with 1.
    """
    arguments = _arguments(ctx.args)
    request = _request(arguments)
    options = _write_options(arguments)
    db_url = get_bootstrap_settings().db_url
    try:
        code = _run(db_url, request, options)
    except RepairRefusedError as error:
        refuse(f"refused: {error}")
    except Exception as error:
        refuse(f"failed: {_failure_text(error)}")
    raise typer.Exit(code=code)
