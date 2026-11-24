"""Append-only Kraken ledger postings and the per-account ingest cursor.

Inserts ignore an existing ``(wallet_public_id, exchange, mode, venue_ledger_id)``.
Every write and read canonicalizes ``wallet_public_id`` to the lowercase hyphenated
UUID spelling, so an alias spelling of one wallet can never fork its cursor or
duplicate its postings.
The cursor row is created once, and later passes keep the reconciliation
boundary recorded on that first insert.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from typing import TypedDict
from typing import cast
from uuid import UUID

from sqlalchemy import Insert
from sqlalchemy import Select
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import VenueLedgerCursor
from snapper.data.models import VenueLedgerEntry

_ENTRY_CONFLICT = ("wallet_public_id", "exchange", "mode", "venue_ledger_id")


def _canonical_wallet(wallet_public_id: str) -> str:
    """Return the canonical lowercase hyphenated spelling of a wallet UUID.

    Args:
        wallet_public_id: Wallet identity in any UUID spelling.

    Returns:
        The canonical spelling used as the stored ledger scope.

    Raises:
        ValueError: When the value is not a UUID.
    """
    return str(UUID(wallet_public_id))


class VenueLedgerInsertRow(TypedDict):
    """One append-only ledger posting."""

    wallet_public_id: str
    exchange: str
    mode: str
    venue_ledger_id: str
    refid: str
    ledger_time_text: str
    ledger_type: str
    subtype: str
    aclass: str
    asset: str
    amount_text: str
    fee_text: str
    balance_text: str
    observed_at: datetime
    observation_id: str


@dataclass(frozen=True)
class VenueLedgerCursorWrite:
    """Cursor, frozen-window progress, and latest pass evidence for one account."""

    wallet_public_id: str
    exchange: str
    mode: str
    cursor_second: int | None
    cursor_ids: tuple[str, ...]
    window_start: int | None
    window_end: int | None
    next_ofs: int | None
    expected_count: int | None
    window_cursor_second: int | None
    window_cursor_ids: tuple[str, ...] | None
    boundary_as_of: datetime
    boundary_watermark: int
    boundary_session_id: str
    boundary_sequence_id: int
    evidence_status: str | None
    reported_count: int | None
    ingested_count: int
    pass_started_at: datetime
    observed_at: datetime
    observation_id: str
    trade_balance_json: str | None
    trade_balance_observed_at: datetime | None
    api_points_used: int


@dataclass(frozen=True)
class VenueLedgerCursorRecord:
    """Stored cursor, resumable frozen window, and latest ledger-pass evidence."""

    wallet_public_id: str
    exchange: str
    mode: str
    cursor_second: int | None
    cursor_ids: tuple[str, ...]
    window_start: int | None
    window_end: int | None
    next_ofs: int | None
    expected_count: int | None
    window_cursor_second: int | None
    window_cursor_ids: tuple[str, ...] | None
    boundary_as_of: datetime
    boundary_watermark: int
    boundary_session_id: str
    boundary_sequence_id: int
    evidence_status: str | None
    reported_count: int | None
    ingested_count: int
    pass_started_at: datetime
    observed_at: datetime
    observation_id: str
    trade_balance_json: str | None
    trade_balance_observed_at: datetime | None
    api_points_used: int


@dataclass(frozen=True)
class VenueLedgerEntryRecord:
    """One stored ledger posting."""

    wallet_public_id: str
    exchange: str
    mode: str
    venue_ledger_id: str
    refid: str
    ledger_time_text: str
    ledger_type: str
    subtype: str
    aclass: str
    asset: str
    amount_text: str
    fee_text: str
    balance_text: str
    observed_at: datetime
    observation_id: str


def _canonical_entry(row: VenueLedgerInsertRow) -> VenueLedgerInsertRow:
    """Return a copy of one posting carrying the canonical wallet spelling."""
    canonical = dict(row)
    canonical["wallet_public_id"] = _canonical_wallet(row["wallet_public_id"])
    return cast(VenueLedgerInsertRow, canonical)


def ledger_entry_insert(dialect: str, rows: Sequence[VenueLedgerInsertRow]) -> Insert:
    """Build an idempotent ledger insert for the session dialect.

    Args:
        dialect: SQLAlchemy dialect name, ``sqlite`` or ``postgresql``.
        rows: Postings to insert.

    Returns:
        A conflict-do-nothing insert on the venue ledger identity.
    """
    insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert
    return (
        insert_fn(VenueLedgerEntry)
        .values([_canonical_entry(row) for row in rows])
        .on_conflict_do_nothing(index_elements=list(_ENTRY_CONFLICT))
    )


async def write_ledger_pass(
    session: AsyncSession,
    dialect: str,
    entries: Sequence[VenueLedgerInsertRow],
    cursor: VenueLedgerCursorWrite,
) -> int:
    """Insert new postings and upsert the cursor inside the caller's transaction.

    Args:
        session: Open async session. The caller commits.
        dialect: Session dialect name.
        entries: Postings from one pass. An empty sequence still saves the cursor.
        cursor: Cursor and evidence for the pass.

    Returns:
        How many postings were newly inserted.
    """
    canonical_entries = [_canonical_entry(row) for row in entries]
    canonical_cursor = replace(
        cursor,
        wallet_public_id=_canonical_wallet(cursor.wallet_public_id),
    )
    inserted = await _insert_entries(session, dialect, canonical_entries)
    await _save_cursor(session, canonical_cursor)
    return inserted


async def fetch_ledger_cursor(
    session: AsyncSession,
    wallet_public_id: str,
    exchange: str,
    mode: str,
) -> VenueLedgerCursorRecord | None:
    """Return the account cursor, or ``None`` before the first pass.

    Args:
        session: Open async session.
        wallet_public_id: Account wallet.
        exchange: Lowercase exchange name.
        mode: Account mode.

    Returns:
        The cursor row, when one exists.
    """
    row = await session.scalar(_cursor_select(_canonical_wallet(wallet_public_id), exchange, mode))
    if row is None:
        return None
    return _cursor_record(row)


async def fetch_ledger_entries(
    session: AsyncSession,
    wallet_public_id: str,
    exchange: str,
    mode: str,
) -> list[VenueLedgerEntryRecord]:
    """Return stored postings for one account in insertion order.

    Args:
        session: Open async session.
        wallet_public_id: Account wallet.
        exchange: Lowercase exchange name.
        mode: Account mode.

    Returns:
        Append-only postings ordered by primary key.
    """
    stmt = (
        select(VenueLedgerEntry)
        .where(
            VenueLedgerEntry.wallet_public_id == _canonical_wallet(wallet_public_id),
            VenueLedgerEntry.exchange == exchange,
            VenueLedgerEntry.mode == mode,
        )
        .order_by(VenueLedgerEntry.id)
    )
    rows = (await session.scalars(stmt)).all()
    return [_entry_record(row) for row in rows]


async def _insert_entries(
    session: AsyncSession,
    dialect: str,
    entries: Sequence[VenueLedgerInsertRow],
) -> int:
    """Insert entries and return how many ids were new for the scope."""
    if not entries:
        return 0
    scope = entries[0]
    ids = [row["venue_ledger_id"] for row in entries]
    before = await _count_ids(session, scope, ids)
    await session.execute(ledger_entry_insert(dialect, entries))
    await session.flush()
    after = await _count_ids(session, scope, ids)
    return after - before


async def _count_ids(
    session: AsyncSession,
    scope: VenueLedgerInsertRow,
    ids: list[str],
) -> int:
    """Count stored ids for one account scope."""
    stmt = (
        select(func.count())
        .select_from(VenueLedgerEntry)
        .where(
            VenueLedgerEntry.wallet_public_id == scope["wallet_public_id"],
            VenueLedgerEntry.exchange == scope["exchange"],
            VenueLedgerEntry.mode == scope["mode"],
            VenueLedgerEntry.venue_ledger_id.in_(ids),
        )
    )
    counted = await session.scalar(stmt)
    if not isinstance(counted, int):
        raise RuntimeError("ledger insert count was not an integer")
    return counted


async def _save_cursor(session: AsyncSession, cursor: VenueLedgerCursorWrite) -> None:
    """Insert the first cursor or update progress without moving the boundary."""
    existing = await session.scalar(
        _cursor_select(cursor.wallet_public_id, cursor.exchange, cursor.mode)
    )
    if existing is None:
        session.add(_new_cursor(cursor))
        await session.flush()
        return
    _apply_cursor_progress(existing, cursor)
    await session.flush()


def _cursor_select(wallet_public_id: str, exchange: str, mode: str) -> Select[VenueLedgerCursor]:
    """Return the select for one account cursor."""
    return select(VenueLedgerCursor).where(
        VenueLedgerCursor.wallet_public_id == wallet_public_id,
        VenueLedgerCursor.exchange == exchange,
        VenueLedgerCursor.mode == mode,
    )


def _new_cursor(cursor: VenueLedgerCursorWrite) -> VenueLedgerCursor:
    """Build the first cursor row, including the backfill boundary."""
    return VenueLedgerCursor(
        wallet_public_id=cursor.wallet_public_id,
        exchange=cursor.exchange,
        mode=cursor.mode,
        cursor_second=cursor.cursor_second,
        cursor_ids_json=_dump_ids(cursor.cursor_ids),
        window_start=cursor.window_start,
        window_end=cursor.window_end,
        next_ofs=cursor.next_ofs,
        expected_count=cursor.expected_count,
        window_cursor_second=cursor.window_cursor_second,
        window_cursor_ids_json=_dump_window_ids(cursor.window_cursor_ids),
        boundary_as_of=cursor.boundary_as_of,
        boundary_watermark=cursor.boundary_watermark,
        boundary_session_id=cursor.boundary_session_id,
        boundary_sequence_id=cursor.boundary_sequence_id,
        evidence_status=cursor.evidence_status,
        reported_count=cursor.reported_count,
        ingested_count=cursor.ingested_count,
        pass_started_at=cursor.pass_started_at,
        observed_at=cursor.observed_at,
        observation_id=cursor.observation_id,
        trade_balance_json=cursor.trade_balance_json,
        trade_balance_observed_at=cursor.trade_balance_observed_at,
        api_points_used=cursor.api_points_used,
    )


def _apply_cursor_progress(existing: VenueLedgerCursor, cursor: VenueLedgerCursorWrite) -> None:
    """Update pass progress and leave the original reconciliation boundary in place."""
    existing.cursor_second = cursor.cursor_second
    existing.cursor_ids_json = _dump_ids(cursor.cursor_ids)
    existing.window_start = cursor.window_start
    existing.window_end = cursor.window_end
    existing.next_ofs = cursor.next_ofs
    existing.expected_count = cursor.expected_count
    existing.window_cursor_second = cursor.window_cursor_second
    existing.window_cursor_ids_json = _dump_window_ids(cursor.window_cursor_ids)
    existing.evidence_status = cursor.evidence_status
    existing.reported_count = cursor.reported_count
    existing.ingested_count = cursor.ingested_count
    existing.pass_started_at = cursor.pass_started_at
    existing.observed_at = cursor.observed_at
    existing.observation_id = cursor.observation_id
    existing.trade_balance_json = cursor.trade_balance_json
    existing.trade_balance_observed_at = cursor.trade_balance_observed_at
    existing.api_points_used = cursor.api_points_used


def _cursor_record(row: VenueLedgerCursor) -> VenueLedgerCursorRecord:
    """Copy a cursor ORM row into a detached record."""
    return VenueLedgerCursorRecord(
        wallet_public_id=row.wallet_public_id,
        exchange=row.exchange,
        mode=row.mode,
        cursor_second=row.cursor_second,
        cursor_ids=_load_ids(row.cursor_ids_json),
        window_start=row.window_start,
        window_end=row.window_end,
        next_ofs=row.next_ofs,
        expected_count=row.expected_count,
        window_cursor_second=row.window_cursor_second,
        window_cursor_ids=_load_window_ids(row.window_cursor_ids_json),
        boundary_as_of=row.boundary_as_of,
        boundary_watermark=row.boundary_watermark,
        boundary_session_id=row.boundary_session_id,
        boundary_sequence_id=row.boundary_sequence_id,
        evidence_status=row.evidence_status,
        reported_count=row.reported_count,
        ingested_count=row.ingested_count,
        pass_started_at=row.pass_started_at,
        observed_at=row.observed_at,
        observation_id=row.observation_id,
        trade_balance_json=row.trade_balance_json,
        trade_balance_observed_at=row.trade_balance_observed_at,
        api_points_used=row.api_points_used,
    )


def _entry_record(row: VenueLedgerEntry) -> VenueLedgerEntryRecord:
    """Copy a ledger ORM row into a detached record."""
    return VenueLedgerEntryRecord(
        wallet_public_id=row.wallet_public_id,
        exchange=row.exchange,
        mode=row.mode,
        venue_ledger_id=row.venue_ledger_id,
        refid=row.refid,
        ledger_time_text=row.ledger_time_text,
        ledger_type=row.ledger_type,
        subtype=row.subtype,
        aclass=row.aclass,
        asset=row.asset,
        amount_text=row.amount_text,
        fee_text=row.fee_text,
        balance_text=row.balance_text,
        observed_at=row.observed_at,
        observation_id=row.observation_id,
    )


def _dump_window_ids(ids: tuple[str, ...] | None) -> str | None:
    """Encode an active window's cursor candidate, or preserve a cleared window."""
    if ids is None:
        return None
    return _dump_ids(ids)


def _load_window_ids(raw: str | None) -> tuple[str, ...] | None:
    """Decode an active window's cursor candidate, or preserve a cleared window."""
    if raw is None:
        return None
    return _load_ids(raw)


def _dump_ids(ids: tuple[str, ...]) -> str:
    """Encode cursor ids as a JSON array."""
    return json.dumps(list(ids), separators=(",", ":"))


def _load_ids(raw: str) -> tuple[str, ...]:
    """Decode a cursor id JSON array."""
    parsed: object = cast(object, json.loads(raw))
    if not isinstance(parsed, list):
        raise ValueError("venue ledger cursor ids are malformed")
    ids: list[str] = []
    for item in parsed:
        if not isinstance(item, str):
            raise ValueError("venue ledger cursor ids are malformed")
        ids.append(item)
    return tuple(ids)
