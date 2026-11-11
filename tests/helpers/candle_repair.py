"""Builders shared by the candle repair driver tests: catalog rows, snapshot files and candles."""

import gzip
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

from snapper.application.updaters.historical.candle_repair import RepairRequest
from snapper.application.updaters.historical.candle_repair import RepairSource
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TradeUpsertRow

RUN_TIME = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
"""Fixed time of a test run."""

OPEN = datetime(2026, 11, 10, 15, 0, tzinfo=UTC)
"""First minute of the usual snapshot."""

FETCHED_AT = OPEN + timedelta(minutes=10, seconds=30)
"""Acquisition time of the usual snapshot: ten closed minutes and the open one."""

CATALOG_TIME = datetime(2026, 1, 1, tzinfo=UTC)
"""Knowledge time of every seeded catalog row."""

XRP = "00000000-0000-4000-8000-000000000c01"
BTC = "00000000-0000-4000-8000-000000000c02"
ETH = "00000000-0000-4000-8000-000000000c03"
"""Instrument public IDs of the seeded symbols."""

CODE_REVISION = {"snapper/application/updaters/historical/candle_repair.py": "0" * 64}
"""A fixed code revision for contexts that do not hash the installed source."""


async def seed_instrument(
    repository: SQLAlchemyRepository,
    instrument_id: str,
    native_symbol: str,
    alias: str | None,
    *,
    channel: str = "rest",
) -> None:
    """Store an active Kraken symbol, its instrument and optionally one alias.

    Args:
        repository: Target repository.
        instrument_id: Instrument public ID; the symbol ID is derived from it.
        native_symbol: Native symbol name.
        alias: Exchange symbol of the alias, or None for no alias.
        channel: Alias channel.
    """
    symbol_id = instrument_id[:-4] + "5" + instrument_id[-3:]
    common = {"timestamp": CATALOG_TIME, "session_id": "seed", "sequence_id": 1}
    base, quote = native_symbol.split("-")
    async with repository.session() as session:
        session.add(
            Symbol(
                public_id=symbol_id,
                native_symbol=native_symbol,
                base=base,
                quote=quote,
                created_at=CATALOG_TIME,
                **common,
            )
        )
        session.add(
            Instrument(
                public_id=instrument_id, symbol_public_id=symbol_id, exchange="kraken", **common
            )
        )
        if alias is not None:
            session.add(
                SymbolAlias(
                    symbol_public_id=symbol_id,
                    exchange="kraken",
                    channel=channel,
                    exchange_symbol=alias,
                    created_at=CATALOG_TIME,
                    **common,
                )
            )
        await session.commit()


def kraken_row(
    minute: datetime, trades: int, *, price: float = 1.1, volume: float = 10.0
) -> list[object]:
    """Build one native Kraken OHLC row ``[time, open, high, low, close, vwap, volume, count]``.

    Args:
        minute: Opening minute.
        trades: Trade count.
        price: Close price; open, high and low are derived from it.
        volume: Volume.

    Returns:
        Row with string prices, as Kraken sends them.
    """
    return [
        int(minute.timestamp()),
        repr(price),
        repr(price + 0.1),
        repr(price - 0.1),
        repr(price),
        repr(price),
        repr(volume),
        trades,
    ]


def kraken_rows(
    start: datetime, minutes: int, *, trades: int = 3, price: float = 1.1
) -> list[list[object]]:
    """Build consecutive traded rows followed by nothing else.

    Args:
        start: First minute.
        minutes: Number of rows.
        trades: Trade count of every row.
        price: Close price of every row.

    Returns:
        Rows in time order.
    """
    return [
        kraken_row(start + timedelta(minutes=offset), trades, price=price)
        for offset in range(minutes)
    ]


def snapshot_bytes(pair: str, rows: list[list[object]], fetched_at: object) -> bytes:
    """Encode one saved response as the snapshot stores it.

    Args:
        pair: Pair key.
        rows: Native rows, the last one being the open interval.
        fetched_at: Acquisition time to store.

    Returns:
        Gzipped JSON bytes.
    """
    last = rows[-1][0] if rows else 0
    payload = {"error": [], "result": {pair: rows, "last": last}, "fetched_at": fetched_at}
    return gzip.compress(json.dumps(payload).encode())


def write_snapshot(
    root: Path,
    pair: str,
    rows: list[list[object]],
    fetched_at: object = FETCHED_AT.timestamp(),
    *,
    folder: str = "ohlc",
) -> Path:
    """Write one saved response under the snapshot root.

    Args:
        root: Snapshot root.
        pair: Pair key and file name.
        rows: Native rows.
        fetched_at: Acquisition time.
        folder: ``ohlc`` or ``ohlc_tokenized``.

    Returns:
        The file path.
    """
    directory = root / folder
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{pair}.json.gz"
    path.write_bytes(snapshot_bytes(pair, rows, fetched_at))
    return path


def candle(
    instrument_id: str, minute: datetime, *, close: float = 1.0, **changes: object
) -> CandleUpsertRow:
    """Build a stored candle row, by default a flat synthesized zero known at ``OPEN``.

    Args:
        instrument_id: Instrument public ID.
        minute: Opening minute.
        close: Price of all four OHLC fields and of the VWAP.
        **changes: Other fields to replace, such as ``source``, ``timestamp``,
            ``volume`` or ``trades``.

    Returns:
        A complete row for ``upsert_candles``.
    """
    row: dict[str, object] = {
        "instrument_public_id": instrument_id,
        "open_at": minute,
        "timestamp": OPEN,
        "timeframe": "1m",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 0.0,
        "vwap": close,
        "trades": 0,
        "source": "synthesized",
        "complete": True,
        "session_id": "live",
        "sequence_id": 1,
    }
    return cast(CandleUpsertRow, row | changes)


def trade(
    instrument_id: str, executed_at: datetime, price: float, size: float, trade_id: str
) -> TradeUpsertRow:
    """Build a stored trade.

    Args:
        instrument_id: Instrument public ID.
        executed_at: Execution time.
        price: Price.
        size: Size.
        trade_id: Venue trade ID.

    Returns:
        A row for ``upsert_trades``.
    """
    return {
        "instrument_public_id": instrument_id,
        "timestamp": executed_at + timedelta(seconds=1),
        "executed_at": executed_at,
        "price": price,
        "size": size,
        "side": "buy",
        "trade_id": trade_id,
        "session_id": "feed",
        "sequence_id": 1,
    }


def snapshot_request(
    snapshot_dir: Path,
    output_dir: Path,
    *,
    symbols: tuple[str, ...] = ("XRP-EUR",),
    window: RepairWindow | None = None,
) -> RepairRequest:
    """Build a snapshot request over the usual window.

    Args:
        snapshot_dir: Snapshot root.
        output_dir: Output directory, created if missing.
        symbols: Requested symbols; empty means ``--all-mapped``.
        window: Repair window; defaults to 14:00 to 16:00 on the snapshot day.

    Returns:
        The request.
    """
    output_dir.mkdir(exist_ok=True)
    return RepairRequest(
        source=RepairSource.SNAPSHOT,
        exchange="kraken",
        window=window or RepairWindow(OPEN - timedelta(hours=1), OPEN + timedelta(hours=1)),
        symbols=symbols,
        all_mapped=not symbols,
        snapshot_dir=snapshot_dir,
        output_dir=output_dir,
    )


def trades_request(
    output_dir: Path, window: RepairWindow, symbols: tuple[str, ...]
) -> RepairRequest:
    """Build a trades request.

    Args:
        output_dir: Output directory, created if missing.
        window: Repair window.
        symbols: Requested symbols.

    Returns:
        The request.
    """
    output_dir.mkdir(exist_ok=True)
    return RepairRequest(
        source=RepairSource.TRADES,
        exchange="kraken",
        window=window,
        symbols=symbols,
        all_mapped=False,
        snapshot_dir=None,
        output_dir=output_dir,
    )


async def instant_sleep(seconds: float) -> None:
    """Pause for nothing; the delay is asserted through a recording wrapper instead.

    Args:
        seconds: Requested pause.
    """
    del seconds


def fixed_context(repository: SQLAlchemyRepository, *, now: datetime = RUN_TIME) -> RunContext:
    """Build a run context with a fixed clock, no real pauses and a fixed code revision.

    Args:
        repository: Target repository.
        now: Time returned by the clock.

    Returns:
        The context.
    """
    return RunContext(
        repository=repository,
        database={"dialect": "sqlite", "path": "/test/candle-repair.db"},
        code_revision=dict(CODE_REVISION),
        clock=lambda: now,
        sleep=instant_sleep,
    )
