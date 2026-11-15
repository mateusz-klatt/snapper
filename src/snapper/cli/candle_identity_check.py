"""Read-only operator check that stored candle ids are deterministic.

``snapper check-candle-identities --from <instant> [--to <instant>]`` scans
the active version of every candle key whose opening lies in ``[from, to)``
and compares its stored public id with the key's deterministic id
(:func:`~snapper.core.candle_identity.deterministic_candle_public_id`). The
range is over openings: a legacy key updated later keeps its stored random id,
as it should, and is outside any range that starts after it opened.

It prints one JSON report: the identity algorithm and the SHA-256 of the
modules that decide it, so a receipt can show the check ran on the writers'
code; the credential-free database identity; the range; the keys scanned and
mismatched per timeframe; and a bounded sample of mismatches. It writes
nothing, and exits non-zero when any key mismatches.
"""

import asyncio
import json
import sys
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Final
from typing import cast

import typer

from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import database_identity
from snapper.cli.operator_support import refuse
from snapper.config.settings import get_bootstrap_settings
from snapper.core import candle_identity
from snapper.core.candle_identity import CANDLE_IDENTITY_ALGORITHM
from snapper.core.candle_identity import is_deterministic_candle_public_id
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data import repository as repository_module
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories
from snapper.data.repository_types import CandleIdentityRow

MISMATCH_SAMPLE_SIZE: Final[int] = 20
"""Mismatching keys listed in the report; every one of them is counted."""


@dataclass(slots=True)
class IdentityScan:
    """Counts and a bounded sample of one identity scan.

    Attributes:
        scanned: Keys scanned per timeframe.
        mismatched: Keys per timeframe whose stored id is not deterministic.
        sample: The first mismatching keys, at most :data:`MISMATCH_SAMPLE_SIZE`.
    """

    scanned: dict[str, int] = field(default_factory=dict)
    mismatched: dict[str, int] = field(default_factory=dict)
    sample: list[JsonValue] = field(default_factory=list)

    def observe(self, row: CandleIdentityRow) -> None:
        """Count one key and remember it when its stored id is not deterministic.

        Args:
            row: Natural key and stored public id of one active version.
        """
        timeframe = row["timeframe"]
        self.scanned[timeframe] = self.scanned.get(timeframe, 0) + 1
        if is_deterministic_candle_public_id(
            row["public_id"], row["instrument_public_id"], timeframe, row["open_at"]
        ):
            return
        self.mismatched[timeframe] = self.mismatched.get(timeframe, 0) + 1
        if len(self.sample) < MISMATCH_SAMPLE_SIZE:
            self.sample.append(
                {
                    "instrument_public_id": row["instrument_public_id"],
                    "timeframe": timeframe,
                    "open_at": row["open_at"].isoformat(),
                    "public_id": row["public_id"],
                }
            )

    def report(self) -> JsonObject:
        """Return the per-timeframe counts and the sample as JSON values.

        Returns:
            ``timeframes`` with scanned and mismatched counts per timeframe,
            the totals, and the sample.
        """
        timeframes: JsonObject = {
            timeframe: {"scanned": count, "mismatched": self.mismatched.get(timeframe, 0)}
            for timeframe, count in sorted(self.scanned.items())
        }
        return {
            "timeframes": timeframes,
            "scanned": sum(self.scanned.values()),
            "mismatched": sum(self.mismatched.values()),
            "sample": self.sample,
        }


def module_hashes(modules: tuple[ModuleType, ...]) -> JsonObject:
    """Hash the loaded source of every module that decides a candle id.

    Args:
        modules: Imported modules to hash.

    Returns:
        SHA-256 of each module's file, keyed by its dotted path as a file path.
    """
    return {
        f"{module.__name__.replace('.', '/')}.py": sha256(
            Path(cast(str, module.__file__)).read_bytes()
        ).hexdigest()
        for module in modules
    }


def utc_instant(text: str, option: str) -> datetime:
    """Parse one range bound, refusing anything but an aware UTC instant.

    Args:
        text: ISO-8601 text from the command line.
        option: The option name, for the refusal.

    Returns:
        The bound as an aware UTC datetime.
    """
    try:
        instant = datetime.fromisoformat(text)
    except ValueError:
        refuse(f"{option} must be an ISO-8601 UTC instant")
    if instant.utcoffset() != timedelta(0):
        refuse(f"{option} must be an ISO-8601 UTC instant")
    return instant.astimezone(UTC)


async def scan_identities(
    repository: SQLAlchemyRepository, start: datetime, end: datetime
) -> IdentityScan:
    """Scan every active key opening in ``[start, end)``.

    Args:
        repository: Repository of the target database.
        start: Inclusive lower bound on the opening.
        end: Exclusive upper bound on the opening.

    Returns:
        The completed scan.
    """
    scan = IdentityScan()
    async for row in repository.iter_active_candle_identities(start, end):
        scan.observe(row)
    return scan


async def _run(start: datetime, end: datetime, db_url: str) -> IdentityScan:
    """Scan the configured database and release its connections.

    Args:
        start: Inclusive lower bound on the opening.
        end: Exclusive upper bound on the opening.
        db_url: Configured SQLAlchemy database URL.

    Returns:
        The completed scan.
    """
    try:
        return await scan_identities(configured_repository(db_url), start, end)
    finally:
        await dispose_repositories()


def check_candle_identities(
    from_: str = typer.Option(..., "--from", help="Inclusive opening bound, ISO-8601 UTC."),
    to: str | None = typer.Option(
        None, "--to", help="Exclusive opening bound, ISO-8601 UTC; default now."
    ),
) -> None:
    """Report candle keys whose stored id is not their deterministic id.

    Args:
        from_: Inclusive lower bound on the opening.
        to: Exclusive upper bound on the opening; the instant the command
            starts when omitted.

    Raises:
        typer.Exit: With status 1 after the report when any key mismatches.
    """
    captured = datetime.now(UTC)
    start = utc_instant(from_, "--from")
    end = captured if to is None else utc_instant(to, "--to")
    if end <= start:
        refuse("--to must be later than --from")
    db_url = get_bootstrap_settings().db_url
    scan = asyncio.run(_run(start, end, db_url))
    report: JsonObject = {
        "algorithm": CANDLE_IDENTITY_ALGORITHM,
        "modules": module_hashes((candle_identity, repository_module, sys.modules[__name__])),
        "database": database_identity(db_url),
        "from": start.isoformat(),
        "to": end.isoformat(),
        **scan.report(),
    }
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if scan.mismatched:
        raise typer.Exit(code=1)
