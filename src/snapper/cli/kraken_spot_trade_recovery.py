"""Compare hashed Kraken Spot recovery evidence with an offline trade snapshot."""

import argparse
import csv
import json
import math
import sys
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Final
from uuid import UUID

import typer

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import confined_input
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    load_kraken_spot_trade_evidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import resolve_input_root
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    StoredKrakenSpotTrade,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    plan_kraken_spot_trade_recovery as reconcile_trades,
)

PLAN_KRAKEN_SPOT_TRADE_RECOVERY_CONTEXT: Final[dict[str, object]] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}
"""Click settings that forward the command's arguments and help to argparse."""

_STORED_COLUMNS = ["instrument_public_id", "trade_id", "executed_at", "price", "size", "side"]


def _stored_trades(path: Path) -> Iterator[StoredKrakenSpotTrade]:
    """Stream the six exported comparison fields, preserving stored timestamp precision."""
    with path.open(encoding="utf-8", newline="") as stream:
        rows = csv.reader(stream, strict=True)
        if next(rows, None) != _STORED_COLUMNS:
            raise ValueError("Unexpected stored trade snapshot header")
        for row in rows:
            try:
                yield _stored_row(row)
            except (ValueError, OverflowError) as error:
                raise ValueError(f"Invalid stored trade on CSV line {rows.line_num}") from error


def _stored_row(row: list[str]) -> StoredKrakenSpotTrade:
    """Parse a PostgreSQL export row without coercing an ambiguous identity."""
    instrument, trade_id, timestamp, price, size, side = row
    if str(UUID(instrument)) != instrument or not trade_id or trade_id.strip() != trade_id:
        raise ValueError("Invalid stored instrument or trade identity")
    executed_at = datetime.fromisoformat(timestamp)
    if executed_at.tzinfo is None:
        raise ValueError("Stored execution timestamp requires an offset")
    price_value, size_value = float(price), float(size)
    if not all(math.isfinite(value) and value > 0 for value in (price_value, size_value)):
        raise ValueError("Stored price and size must be finite and positive")
    if side not in {"buy", "sell"}:
        raise ValueError("Invalid stored trade side")
    return StoredKrakenSpotTrade(
        instrument_public_id=instrument,
        trade_id=trade_id,
        executed_at=executed_at.astimezone(UTC),
        price=price_value,
        size=size_value,
        side=side,
    )


def _missing_by_symbol(evidence: Iterable[KrakenSpotTradeEvidence]) -> dict[str, list[str]]:
    """List the source trade IDs still absent from the supplied snapshot."""
    result: dict[str, list[str]] = {}
    for row in evidence:
        result.setdefault(row.symbol, []).append(row.trade_id)
    return result


def main(argv: list[str] | None = None) -> int:
    """Print the CSV comparison as JSON or report a validation failure on stderr.

    Args:
        argv: Command arguments, or None to use the process arguments.

    Returns:
        Zero for a completed comparison, or one for an invalid input.
    """
    parser = argparse.ArgumentParser(
        prog="snapper plan-kraken-spot-trade-recovery", description=__doc__
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("."),
        help="Input directory (default: current directory)",
    )
    parser.add_argument("--evidence", required=True, type=Path, help="Kraken Spot evidence CSV")
    parser.add_argument("--evidence-sha256", required=True, help="Expected digest of evidence CSV")
    parser.add_argument(
        "--stored-trades", required=True, type=Path, help="Stored trade snapshot CSV"
    )
    parser.add_argument("--start", required=True, help="Inclusive UTC execution time")
    parser.add_argument("--end", required=True, help="Exclusive UTC execution time")
    args = parser.parse_args(argv)

    try:
        root = resolve_input_root(args.input_root)
        evidence_path = confined_input(root, args.evidence)
        stored_path = confined_input(root, args.stored_trades)
        window = load_kraken_spot_trade_evidence(
            evidence_path, args.evidence_sha256, args.start, args.end
        )
        plan = reconcile_trades(window.rows, _stored_trades(stored_path))
    except (OSError, ValueError, csv.Error) as error:
        print(f"Kraken Spot trade recovery plan failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "evidence_rows": len(window.rows),
                "already_present": plan.already_present,
                "missing_from_snapshot": len(plan.missing),
                "missing_trade_ids_by_symbol": _missing_by_symbol(plan.missing),
            },
            sort_keys=True,
        )
    )
    return 0


def plan_kraken_spot_trade_recovery(ctx: typer.Context) -> None:
    """Compare verified Kraken Spot trade evidence with an offline snapshot.

    Args:
        ctx: Context whose extra arguments are passed to argparse.

    Raises:
        typer.Exit: With the planner's exit code.
    """
    raise typer.Exit(code=main(list(ctx.args)))
