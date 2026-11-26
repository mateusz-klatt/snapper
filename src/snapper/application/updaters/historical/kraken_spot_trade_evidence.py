"""Read held Kraken Spot trade evidence for offline reconciliation."""

import csv
import re
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from math import inf
from typing import Literal
from typing import TextIO

from snapper.infrastructure.exchanges.adapters.kraken_timestamps import normalize_kraken_timestamp

MAX_KRAKEN_SPOT_TRADE_EVIDENCE_ROWS = 10_000

_CSV_HEADER = [
    "exchange",
    "symbol",
    "instrument_public_id",
    "exchange_trade_id",
    "executed_at_utc",
    "exchange_unix_seconds",
    "price",
    "size",
    "exchange_side",
    "exchange_order_type",
    "exchange_misc",
]
_SYMBOL = re.compile(r"[A-Z0-9]+(?:\.[A-Z0-9]+)*-[A-Z0-9]+(?:\.[A-Z0-9]+)*")
_PUBLIC_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TRADE_ID = re.compile(r"[1-9][0-9]{0,63}")
_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)"
)
_UNIX_SECONDS = re.compile(r"[0-9]{1,12}(?:\.[0-9]+)?")
_DECIMAL = re.compile(r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_SIDES: dict[str, Literal["buy", "sell"]] = {"b": "buy", "s": "sell"}
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MICROSECOND = timedelta(microseconds=1)
_MICROSECONDS_PER_SECOND = 1_000_000
_MICROSECONDS_PER_MILLISECOND = 1_000


@dataclass(frozen=True, slots=True)
class KrakenSpotTradeEvidence:
    """A source trade with its historical instrument and normalized venue identity."""

    symbol: str
    instrument_public_id: str
    trade_id: str
    executed_at: datetime
    price: Decimal
    size: Decimal
    side: Literal["buy", "sell"]


def read_kraken_spot_trade_evidence(stream: TextIO) -> list[KrakenSpotTradeEvidence]:
    """Parse a bounded evidence CSV, keeping all rows for duplicate detection.

    Args:
        stream: A text stream with the Kraken Spot evidence header and data rows.

    Returns:
        Canonical evidence rows in their source order.

    Raises:
        ValueError: If the CSV is malformed, invalid, empty, or exceeds the row limit.
    """
    reader = csv.reader(stream, strict=True)
    rows: list[KrakenSpotTradeEvidence] = []
    try:
        if next(reader, None) != _CSV_HEADER:
            raise ValueError("Invalid Kraken Spot evidence CSV header")
        for fields in reader:
            if len(rows) >= MAX_KRAKEN_SPOT_TRADE_EVIDENCE_ROWS:
                raise ValueError(
                    f"Kraken Spot evidence exceeds {MAX_KRAKEN_SPOT_TRADE_EVIDENCE_ROWS} rows"
                )
            try:
                rows.append(_parse_row(fields))
            except ValueError as exc:
                raise ValueError(f"Kraken Spot evidence CSV line {reader.line_num}: {exc}") from exc
    except csv.Error as exc:
        raise ValueError(f"Malformed Kraken Spot evidence CSV at line {reader.line_num}") from exc
    if not rows:
        raise ValueError("Kraken Spot evidence CSV contains no rows")
    return rows


def _parse_row(fields: list[str]) -> KrakenSpotTradeEvidence:
    """Validate the shape, venue and field values of one evidence row."""
    if len(fields) != len(_CSV_HEADER):
        raise ValueError(f"Expected {len(_CSV_HEADER)} fields")
    (
        exchange,
        symbol,
        instrument_public_id,
        trade_id,
        executed_at,
        unix_seconds,
        price,
        size,
        exchange_side,
        order_type,
        _misc,
    ) = fields
    if exchange != "kraken":
        raise ValueError("Expected kraken exchange")
    if not _SYMBOL.fullmatch(symbol):
        raise ValueError("Invalid Kraken Spot native symbol")
    if not _PUBLIC_ID.fullmatch(instrument_public_id):
        raise ValueError("Expected canonical instrument_public_id UUID")
    if not _TRADE_ID.fullmatch(trade_id):
        raise ValueError("Expected positive numeric exchange_trade_id")
    side = _SIDES.get(exchange_side)
    if side is None:
        raise ValueError("Expected exchange_side b or s")
    if order_type not in {"l", "m"}:
        raise ValueError("Expected exchange_order_type l or m")
    return KrakenSpotTradeEvidence(
        symbol=symbol,
        instrument_public_id=instrument_public_id,
        trade_id=trade_id,
        executed_at=_parse_execution_time(executed_at, unix_seconds),
        price=_parse_trade_decimal(price, "price"),
        size=_parse_trade_decimal(size, "size"),
        side=side,
    )


def _parse_execution_time(executed_at: str, unix_seconds: str) -> datetime:
    """Validate the ISO display time and normalize from the exact source epoch."""
    if not _UTC_TIMESTAMP.fullmatch(executed_at):
        raise ValueError(
            "Expected executed_at_utc in UTC ISO-8601 with up to six fractional digits"
        )
    if not _UNIX_SECONDS.fullmatch(unix_seconds):
        raise ValueError("Expected decimal exchange_unix_seconds")
    offset = datetime.fromisoformat(executed_at) - _UNIX_EPOCH
    seconds, _, fraction = unix_seconds.partition(".")
    raw_microseconds = int(seconds) * _MICROSECONDS_PER_SECOND + int(fraction[:6].ljust(6, "0"))
    difference = offset // _ONE_MICROSECOND - raw_microseconds
    if difference not in (0, 1) or (difference == 1 and not fraction[6:].strip("0")):
        raise ValueError("executed_at_utc and exchange_unix_seconds disagree")
    return normalize_kraken_timestamp(raw_microseconds // _MICROSECONDS_PER_MILLISECOND)


def _parse_trade_decimal(raw_value: str, field: str) -> Decimal:
    """Retain the original decimal precision and reject values unusable as DB floats."""
    error = f"Expected finite positive {field} representable as a database float"
    if not _DECIMAL.fullmatch(raw_value):
        raise ValueError(error)
    try:
        value = Decimal(raw_value)
    except InvalidOperation as exc:
        raise ValueError(error) from exc
    if not 0 < float(value) < inf:
        raise ValueError(error)
    return value
