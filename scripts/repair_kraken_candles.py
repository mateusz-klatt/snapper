"""Repair bounded Kraken native candles from archives, Futures charts or Equities bars.

No archive member is extracted. Records and database batches have fixed bounds;
missing VWAP and charts trade counts stay null. Reruns inspect all knowledge
versions and never close, update or replace a candle.
"""

import argparse
import asyncio
import csv
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from functools import partial
from io import TextIOWrapper
from pathlib import Path
from re import fullmatch
from uuid import uuid7
from zipfile import ZipFile

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from snapper.application.updaters.historical.repair_batch import retry_batch
from snapper.application.updaters.historical.repair_window import Window
from snapper.config.settings import get_settings
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_equities_ws

_BAR_SECONDS: dict[str, int] = {"1m": 60, "15m": 900, "30m": 1800, "1h": 3600, "1d": 86400}


@dataclass
class Totals:
    """Per-run candidate counts, including rejected data and exact unmapped pairs."""

    in_window: int = 0
    absent: int = 0
    already_present: int = 0
    invalid: int = 0
    written: int = 0
    unmapped: set[str] = field(default_factory=set)
    unmapped_rows: int = 0
    members: int = 0
    skipped_symbols: set[str] = field(default_factory=set)
    failed_symbols: set[str] = field(default_factory=set)
    errors: int = 0


@dataclass
class Writer:
    """Bound parsing and persistence memory independently of archive size."""

    repository: Repository
    batch_size: int
    dry_run: bool
    totals: Totals = field(default_factory=Totals)
    pending: list[CandleUpsertRow] = field(default_factory=list)
    timeframe: str = "1m"
    session_id: str = field(default_factory=lambda: str(uuid7()))
    sequence: int = 0

    def __post_init__(self) -> None:
        """Reject unsafe or empty batch sizes before external access."""
        if not 1 <= self.batch_size <= 100:
            raise ValueError("batch-size must be between 1 and 100")

    def build(self, values: list[str], instrument: str) -> CandleUpsertRow:
        """Validate an official record without inventing missing statistics.

        Args:
            values: Timestamp, OHLC, volume and optional archive trade count.
            instrument: Exact active instrument public identity.

        Returns:
            Validated native candidate with acquisition provenance.
        """
        opening = datetime.fromtimestamp(int(values[0]), UTC)
        if int(values[0]) % _BAR_SECONDS[self.timeframe]:
            raise ValueError("native opening must be aligned to its bar")
        self.sequence += 1
        row: CandleUpsertRow = {
            "instrument_public_id": instrument,
            "open_at": opening,
            "timestamp": datetime.now(UTC),
            "timeframe": self.timeframe,
            "open": float(values[1]),
            "high": float(values[2]),
            "low": float(values[3]),
            "close": float(values[4]),
            "volume": float(values[5]),
            "vwap": None,
            "trades": int(values[6]) if len(values) == 7 else None,
            "source": "native",
            "complete": True,
            "session_id": self.session_id,
            "sequence_id": self.sequence,
        }
        SQLAlchemyRepository._validate_missing_candle_business(row, True)
        return row

    async def append(self, row: CandleUpsertRow) -> None:
        """Queue a row and flush when the bounded batch fills.

        Args:
            row: Validated in-window candidate.
        """
        if any(item["open_at"] == row["open_at"] for item in self.pending):
            await self.flush()
        self.pending.append(row)
        if len(self.pending) == self.batch_size:
            await self.flush()

    async def flush(self) -> None:
        """Count or insert a batch, releasing the writer fence before returning."""
        if not self.pending:
            return
        inserted = await retry_batch(
            partial(
                self.repository.insert_missing_native_candles,
                self.pending.copy(),
                dry_run=self.dry_run,
            )
        )
        self.totals.absent += len(inserted)
        self.totals.already_present += len(self.pending) - len(inserted)
        if not self.dry_run:
            self.totals.written += len(inserted)
        self.pending.clear()


_LEGACY_REST_PAIRS: tuple[str, ...] = (
    r"[XZ]([A-Z0-9]{3})[XZ]([A-Z0-9]{3})",
    r"X(X[A-Z0-9]{2})([A-Z0-9]{4,5})",
    r"([A-Z0-9]{4})Z(USD|EUR|GBP|CAD|JPY|AUD|CHF)",
)


def archive_pair(alias: str, pairs: set[str]) -> str:
    """Prefer an exact REST name before explicit legacy X/Z-prefix normalization.

    Kraken REST names keep legacy asset prefixes in three observed forms:
    ``XXBTZUSD`` and ``XETHXXBT`` on both assets, ``XXBTEUROP`` on the base only
    and ``USDTZUSD`` on a fiat quote only. Each candidate must be an exact archive
    pair, so a name matching no form or no pair stays unmapped and is reported.

    Args:
        alias: Active Kraken REST alias, never a base/quote concatenation.
        pairs: Exact one-minute pair names found in this archive.

    Returns:
        The direct name, the legacy name with its asset prefixes removed, or the alias.
    """
    if alias in pairs:
        return alias
    for pattern in _LEGACY_REST_PAIRS:
        legacy = fullmatch(pattern, alias)
        if legacy is not None and legacy[1] + legacy[2] in pairs:
            return legacy[1] + legacy[2]
    return alias


async def archive_instruments(
    repository: Repository, pairs: set[str]
) -> dict[str, tuple[str, str]]:
    """Resolve only active Kraken REST aliases and refuse normalized collisions.

    Args:
        repository: Read-only source of symbol, alias and instrument identities.
        pairs: Exact pair names in the selected archive's one-minute members.

    Returns:
        Archive pair to native symbol and instrument; relay instruments without aliases are absent.
    """
    now = datetime.now(UTC)
    async with repository.session() as session:
        result = await session.execute(
            select(Symbol.native_symbol, SymbolAlias.exchange_symbol, Instrument.public_id)
            .select_from(SymbolAlias)
            .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
            .join(Instrument, Instrument.symbol_public_id == Symbol.public_id)
            .where(
                SymbolAlias.exchange == "kraken",
                SymbolAlias.channel == "rest",
                Instrument.exchange == "kraken",
                *where_active(SymbolAlias, now),
                *where_active(Symbol, now),
                *where_active(Instrument, now),
            )
        )
        mapping: dict[str, tuple[str, str]] = {}
        for native, alias, public_id in result.all():
            pair = archive_pair(alias, pairs)
            if pair not in pairs:
                continue
            identity = native, public_id
            if pair in mapping and mapping[pair] != identity:
                raise ValueError(f"ambiguous archive pair: {pair}")
            mapping[pair] = identity
        return mapping


def member_pair(name: str) -> str | None:
    """Accept a root member or one leading directory without extracting any paths.

    Args:
        name: Untrusted ZIP member name.

    Returns:
        Exact one-minute pair name or None for ignored members.
    """
    match = fullmatch(r"(?:(?!\.{1,2}/)[^/\\]+/)?([A-Z0-9.]+)_1\.csv", name)
    return match[1] if match is not None else None


def records(handle: TextIOWrapper) -> Iterator[list[str]]:
    """Stream bounded single-line CSV records, marking oversized records invalid.

    Args:
        handle: One archive member; never an extracted filesystem path.

    Yields:
        Parsed fields, or an empty list for malformed or oversized records.
    """
    while line := handle.readline(4097):
        if len(line) > 4096:
            while line and not line.endswith("\n"):
                line = handle.readline(4097)
            yield []
            continue
        try:
            yield next(csv.reader([line], strict=True))
        except csv.Error:
            yield []


async def consume_record(
    values: list[str], identity: tuple[str, str] | None, writer: Writer, window: Window
) -> None:
    """Count a record and skip invalid or unmapped native data without guessing.

    Args:
        values: Untrusted single-line archive fields.
        identity: Exact resolved symbol and instrument, or an unmapped pair.
        writer: Bounded insertion state and per-run counts.
        window: Half-open historical window.
    """
    try:
        if len(values) != 7:
            raise ValueError("archive records require seven columns")
        opening = datetime.fromtimestamp(int(values[0]), UTC)
        if not window.contains(opening):
            return
        writer.totals.in_window += 1
        if identity is None:
            writer.totals.unmapped_rows += 1
        row = writer.build(values, identity[1] if identity is not None else "unmapped")
    except (ValueError, OverflowError, OSError) as error:
        writer.totals.invalid += 1
        logger.warning(f"Skipped invalid archive record: {error}")
        return
    if identity is not None:
        await writer.append(row)


async def load_archive(args: argparse.Namespace, writer: Writer, window: Window) -> None:
    """Stream selected 1m members from the official quarterly ZIP.

    Args:
        args: Archive path and optional exact native symbol restriction.
        writer: Bounded insert-only persistence state.
        window: Requested historical range.
    """
    with ZipFile(args.archive) as archive:
        members = [(member, member_pair(member.filename)) for member in archive.infolist()]
        pairs = {pair for _, pair in members if pair is not None}
        writer.totals.members = sum(pair is not None for _, pair in members)
        mapping = await archive_instruments(writer.repository, pairs)
        for member, pair in members:
            if pair is None:
                continue
            identity = mapping.get(pair)
            if identity is None:
                writer.totals.unmapped.add(pair)
            elif args.symbol and args.symbol != identity[0]:
                continue
            with (
                archive.open(member) as binary,
                TextIOWrapper(binary, encoding="utf-8", errors="replace") as handle,
            ):
                for values in records(handle):
                    await consume_record(values, identity, writer, window)
            await writer.flush()


def snapshot_values(candle: OhlcvSnapshot) -> list[str]:
    """Convert an SDK snapshot at the external boundary without rounding timestamps.

    Args:
        candle: Official Futures trade OHLCV, with no VWAP or trade count.

    Returns:
        Six fields accepted by the shared native validator.
    """
    opening = int(candle.timestamp)
    if opening != candle.timestamp:
        raise ValueError("charts timestamp must be an integer second")
    return [
        str(opening),
        *(
            str(value)
            for value in (candle.open, candle.high, candle.low, candle.close, candle.volume)
        ),
    ]


async def futures_symbol(
    client: KrakenFuturesExchangeClient, identity: tuple[str, str], writer: Writer, window: Window
) -> None:
    """Page fixed bounded charts slices, filtering both bounds even on sparse responses.

    Args:
        client: Connected public Kraken Futures client using fetch_ohlcv.
        identity: Exact native symbol and active instrument identity.
        writer: Shared bounded persistence state.
        window: Explicit half-open range, independent of the newest stored candle.
    """
    try:
        symbol = native_to_ccxt(identity[0])
    except ValueError:
        writer.totals.skipped_symbols.add(identity[0])
        return
    cursor = int(window.start.timestamp() * 1000)
    end = int(window.end.timestamp() * 1000)
    while cursor < end:
        page_limit = min(500, (end - cursor) // 60000)
        candles = await client.get_ohlcv(symbol, "1m", since=cursor, limit=page_limit)
        next_cursor = cursor + page_limit * 60000
        for candle in candles:
            try:
                values = snapshot_values(candle)
                opening = datetime.fromtimestamp(int(values[0]), UTC)
                if (
                    not window.contains(opening)
                    or not cursor <= candle.timestamp * 1000 < next_cursor
                ):
                    continue
                writer.totals.in_window += 1
                row = writer.build(values, identity[1])
            except (ValueError, OverflowError, OSError) as error:
                writer.totals.invalid += 1
                logger.warning(f"Skipped invalid charts record: {error}")
                continue
            await writer.append(row)
        await writer.flush()
        returned = (
            int(candle.timestamp * 1000)
            for candle in candles
            if cursor <= candle.timestamp * 1000 < next_cursor and candle.timestamp % 60 == 0
        )
        cursor = max(returned, default=next_cursor - 60000) + 60000
        await asyncio.sleep(1)


async def load_futures(args: argparse.Namespace, writer: Writer, window: Window) -> None:
    """Repair selected active Futures instruments without creating catalog rows.

    Args:
        args: Optional native symbol filter; resume is intentionally unavailable.
        writer: Shared bounded insertion state.
        window: Requested half-open historical range.
    """
    now = datetime.now(UTC)
    symbols = await writer.repository.get_exchange_instruments("kraken_futures", now)
    if args.symbol:
        symbols = [args.symbol]
    mapping = await writer.repository.get_instrument_public_ids_by_symbols(
        set(symbols), "kraken_futures", now
    )
    client = KrakenFuturesExchangeClient()
    try:
        await client.connect()
        for symbol in symbols:
            if symbol not in mapping:
                writer.totals.unmapped.add(symbol)
                continue
            try:
                await futures_symbol(client, (symbol, mapping[symbol]), writer, window)
            except DBAPIError:
                raise
            except Exception as error:
                writer.totals.failed_symbols.add(symbol)
                writer.totals.errors += 1
                logger.error(f"Futures repair failed for {symbol}: {error}")
                writer.pending.clear()
    finally:
        await client.disconnect()


async def append_equities_snapshot(
    candle: OhlcvSnapshot, instrument: str, writer: Writer, window: Window
) -> None:
    """Queue one in-window venue bar, counting and skipping an invalid one.

    Args:
        candle: Official FCM bar, with its VWAP and trade count left unknown.
        instrument: Exact active instrument public identity.
        writer: Shared bounded persistence state.
        window: Requested half-open historical range.
    """
    try:
        values = snapshot_values(candle)
        if not window.contains(datetime.fromtimestamp(int(values[0]), UTC)):
            return
        writer.totals.in_window += 1
        row = writer.build(values, instrument)
    except (ValueError, OverflowError, OSError) as error:
        writer.totals.invalid += 1
        logger.warning(f"Skipped invalid Equities record: {error}")
        return
    await writer.append(row)


async def equities_symbol(
    client: KrakenEquitiesExchangeClient,
    identity: tuple[str, str],
    writer: Writer,
    window: Window,
) -> None:
    """Store one contract's bars from the single bounded window the venue serves.

    Args:
        client: Equities client; the endpoint needs no connection or credentials.
        identity: Exact native symbol and active instrument identity.
        writer: Shared bounded persistence state.
        window: Explicit half-open range, independent of the newest stored candle.
    """
    try:
        native_to_kraken_equities_ws(identity[0])
    except ValueError:
        writer.totals.skipped_symbols.add(identity[0])
        return
    candles = await client.get_ohlcv(
        identity[0], writer.timeframe, since=int(window.start.timestamp() * 1000)
    )
    for candle in candles:
        await append_equities_snapshot(candle, identity[1], writer, window)
    await writer.flush()
    await asyncio.sleep(1)


async def load_equities(args: argparse.Namespace, writer: Writer, window: Window) -> None:
    """Repair selected active FCM contracts; delisted ones have no venue bars.

    Args:
        args: Optional native symbol filter.
        writer: Shared bounded insertion state, carrying the requested bar size.
        window: Requested half-open historical range.
    """
    now = datetime.now(UTC)
    symbols = await writer.repository.get_exchange_instruments("kraken_equities", now)
    if args.symbol:
        symbols = [args.symbol]
    mapping = await writer.repository.get_instrument_public_ids_by_symbols(
        set(symbols), "kraken_equities", now
    )
    client = KrakenEquitiesExchangeClient()
    for symbol in symbols:
        if symbol not in mapping:
            writer.totals.unmapped.add(symbol)
            continue
        try:
            await equities_symbol(client, (symbol, mapping[symbol]), writer, window)
        except DBAPIError:
            raise
        except Exception as error:
            writer.totals.failed_symbols.add(symbol)
            writer.totals.errors += 1
            logger.error(f"Equities repair failed for {symbol}: {error}")
            writer.pending.clear()


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Validate operator bounds and source options before opening a repository.

    Args:
        argv: Command arguments without the program name.

    Returns:
        Parsed and validated repair arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("archive", "futures", "equities"), default="archive")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--timeframe", choices=tuple(_BAR_SECONDS), default="1m")
    parser.add_argument("--start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--end", type=datetime.fromisoformat, required=True)
    parser.add_argument("--symbol")
    parser.add_argument("--batch-size", type=int, choices=range(1, 101), default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-unmapped",
        action="store_true",
        help="Acknowledge in-window rows without a mapping.",
    )
    parser.add_argument(
        "--allow-invalid", action="store_true", help="Acknowledge skipped invalid records."
    )
    args = parser.parse_args(argv)
    if args.source == "archive" and args.archive is None:
        parser.error("--archive is required for archive repairs")
    if args.source != "archive" and args.archive is not None:
        parser.error("--archive is only applicable to archive repairs")
    if (args.source == "equities") == (args.timeframe == "1m"):
        parser.error("--timeframe above 1m is required for equities and only applies there")
    Window(args.start, args.end)
    return args


async def run(args: argparse.Namespace) -> Totals:
    """Run a repair and dispose the repository on success or failure.

    Args:
        args: Validated operator arguments.

    Returns:
        Per-run counts printed for both dry-run and write modes.
    """
    window = Window(args.start, args.end)
    repository = get_repository(get_settings().db_url)
    writer = Writer(repository, args.batch_size, args.dry_run, timeframe=args.timeframe)
    try:
        if args.source == "archive":
            await load_archive(args, writer, window)
        elif args.source == "futures":
            await load_futures(args, writer, window)
        else:
            await load_equities(args, writer, window)
        await writer.flush()
    except Exception as error:
        writer.totals.errors += 1
        logger.error(f"Repair aborted after acknowledged commits: {error}")
    finally:
        await repository.engine.dispose()
    totals = writer.totals
    print(
        f"rows_in_window={totals.in_window} absent={totals.absent} "
        f"already_present={totals.already_present} invalid={totals.invalid} "
        f"written={totals.written} members={totals.members} errors={totals.errors} "
        f"unmapped_rows={totals.unmapped_rows} unmapped_pairs={len(totals.unmapped)} "
        f"unmapped={','.join(sorted(totals.unmapped))} "
        f"skipped_count={len(totals.skipped_symbols)} "
        f"skipped_symbols={','.join(sorted(totals.skipped_symbols))} "
        f"failed_symbols={','.join(sorted(totals.failed_symbols))}"
    )
    return totals


def exit_status(args: argparse.Namespace, totals: Totals) -> int:
    """Fail incomplete repairs unless skipped input data was explicitly acknowledged.

    Args:
        args: Source and explicit acknowledgements from the command line.
        totals: All per-run counters, including previously committed batches.

    Returns:
        One for failures, an empty archive selection or unacknowledged rejected data.
    """
    if totals.errors or (totals.invalid and not args.allow_invalid):
        return 1
    if totals.unmapped_rows and not args.allow_unmapped:
        return 1
    return int(args.source == "archive" and (totals.members == 0 or totals.in_window == 0))


def main() -> int:
    """Run the bounded native repair command.

    Returns:
        Zero only for a successful scan with rejected data explicitly acknowledged.
    """
    args = parse_args(sys.argv[1:])
    return exit_status(args, asyncio.run(run(args)))


if __name__ == "__main__":
    raise SystemExit(main())
