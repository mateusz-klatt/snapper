"""Compare hashed Kraken Spot recovery evidence with an offline trade snapshot."""

import argparse
import csv
import hashlib
import io
import json
import math
import sys
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import UUID

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    read_kraken_spot_trade_evidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    StoredKrakenSpotTrade,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    plan_kraken_spot_trade_recovery,
)

_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_STORED_COLUMNS = ["instrument_public_id", "trade_id", "executed_at", "price", "size", "side"]


def _read_evidence(path: Path, expected_digest: str) -> list[KrakenSpotTradeEvidence]:
    """Read and validate the same bounded bytes that the digest attests."""
    with path.open("rb") as stream:
        data = stream.read(_MAX_EVIDENCE_BYTES + 1)
    if len(data) > _MAX_EVIDENCE_BYTES:
        raise ValueError("Evidence file exceeds the size limit")
    if hashlib.sha256(data).hexdigest() != expected_digest.lower():
        raise ValueError("Evidence SHA-256 differs from the manifest")
    return read_kraken_spot_trade_evidence(io.StringIO(data.decode("utf-8"), newline=""))


def _utc_bound(value: str) -> datetime:
    """Parse a UTC window bound aligned to the stored execution-time precision."""
    bound = datetime.fromisoformat(value)
    if bound.utcoffset() != timedelta(0) or bound.microsecond % 1000:
        raise ValueError("Window bounds must be UTC and millisecond-aligned")
    return bound


def _check_evidence_scope(
    evidence: Sequence[KrakenSpotTradeEvidence], start: datetime, end: datetime
) -> None:
    """Require in-window rows and a one-to-one instrument mapping in the evidence."""
    if start >= end:
        raise ValueError("Window start must precede end")
    by_symbol: dict[str, str] = {}
    by_instrument: dict[str, str] = {}
    for row in evidence:
        if not start <= row.executed_at < end:
            raise ValueError("Evidence includes an execution outside the requested window")
        if (
            by_symbol.setdefault(row.symbol, row.instrument_public_id) != row.instrument_public_id
            or by_instrument.setdefault(row.instrument_public_id, row.symbol) != row.symbol
        ):
            raise ValueError("Evidence contains an ambiguous symbol or instrument assignment")


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path, help="Kraken Spot evidence CSV")
    parser.add_argument("--evidence-sha256", required=True, help="Expected digest of evidence CSV")
    parser.add_argument(
        "--stored-trades", required=True, type=Path, help="Stored trade snapshot CSV"
    )
    parser.add_argument("--start", required=True, help="Inclusive UTC execution time")
    parser.add_argument("--end", required=True, help="Exclusive UTC execution time")
    args = parser.parse_args(argv)

    try:
        evidence = _read_evidence(args.evidence, args.evidence_sha256)
        _check_evidence_scope(evidence, _utc_bound(args.start), _utc_bound(args.end))
        plan = plan_kraken_spot_trade_recovery(evidence, _stored_trades(args.stored_trades))
    except (OSError, ValueError, csv.Error) as error:
        print(f"Kraken Spot trade recovery plan failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "evidence_rows": len(evidence),
                "already_present": plan.already_present,
                "missing_from_snapshot": len(plan.missing),
                "missing_trade_ids_by_symbol": _missing_by_symbol(plan.missing),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
