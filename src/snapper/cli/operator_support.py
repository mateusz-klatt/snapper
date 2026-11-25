"""Shared pieces of operator commands that change durable state on purpose.

Such a command refuses with one line and a non-zero status, names its target
database without credentials, and reaches it through the configured repository.
"""

import argparse
import math
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Final
from typing import NoReturn
from typing import cast

import typer
from sqlalchemy.engine import make_url

from snapper.application.updaters.historical.guarded_candle_write import WriteOptions
from snapper.application.updaters.historical.guarded_candle_write import error_fields
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository


def refuse(message: str) -> NoReturn:
    """Print a refusal and terminate the command.

    Args:
        message: Refusal shown to the operator.

    Returns:
        Never; the call always raises.

    Raises:
        typer.Exit: Always, with a non-zero status.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=1)


def database_identity(db_url: str) -> str:
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


def configured_repository(
    db_url: str, *, application_name: str | None = None
) -> SQLAlchemyRepository:
    """Resolve the configured repository.

    Args:
        db_url: Configured SQLAlchemy database URL.
        application_name: Optional PostgreSQL connection name for this invocation.

    Returns:
        Repository for the target database, dedicated to this invocation when named.
    """
    if application_name is not None:
        return SQLAlchemyRepository(db_url, application_name=application_name)
    return cast(SQLAlchemyRepository, get_repository(db_url))


_DIGEST: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{64}")
_DEFAULT_CHUNK_PAUSE_SECONDS: Final[float] = 1.0
_DEFAULT_MAX_CONSECUTIVE_LOCK_TIMEOUTS: Final[int] = 3
WRITE_OPTIONS: Final[tuple[tuple[str, str], ...]] = (
    ("voted_preview", "--voted-preview"),
    ("expect_digest", "--expect-digest"),
    ("max_rows", "--max-rows"),
    ("chunk_pause_seconds", "--chunk-pause-seconds"),
    ("max_consecutive_lock_timeouts", "--max-consecutive-lock-timeouts"),
)


class UsageError(Exception):
    """An argument the parser does not accept."""


class RefusingParser(argparse.ArgumentParser):
    """Raise on a usage error instead of exiting, so the command can refuse."""

    def error(self, message: str) -> NoReturn:
        """Report a usage error to the caller.

        Args:
            message: Parser's description of the error.

        Raises:
            UsageError: Always.
        """
        raise UsageError(message)


def add_write_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the shared approval and retry options."""
    parser.add_argument("--confirm", action="store_true", help="Write the voted rows")
    parser.add_argument("--voted-preview", type=Path, help="Output directory of the voted run")
    parser.add_argument("--expect-digest", help="sha256 of the voted approval.json")
    parser.add_argument("--max-rows", type=int, help="Most rows this write applies")
    parser.add_argument("--chunk-pause-seconds", type=float, help="Pause between chunks (1.0)")
    parser.add_argument(
        "--max-consecutive-lock-timeouts", type=int, help="Lock timeouts that stop a write (3)"
    )


def write_options(confirm: bool, writing: Mapping[str, object]) -> WriteOptions | None:
    """Return the write options of a confirmed run; refuse them without ``--confirm``."""
    if not confirm:
        if writing:
            given = [option for name, option in WRITE_OPTIONS if name in writing]
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


def failure_text(error: BaseException) -> str:
    """Describe an unexpected failure by class and SQLSTATE only."""
    fields = error_fields(error)
    sqlstate = fields["sqlstate"]
    return f"{fields['error']}" + (f" sqlstate={sqlstate}" if sqlstate else "")
