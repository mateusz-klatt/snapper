"""Observe Kraken ledger postings for ``margin_ledger_replay`` accounts.

The pass reads TradeBalance and Ledgers through the caller-supplied client,
which must be the executor's existing Kraken client. It does not change
reconciliation verdicts. Evidence statuses are stored on the cursor row.
"""

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import uuid7

from loguru import logger

from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.venue_ledger_store import VenueLedgerCursorRecord
from snapper.data.venue_ledger_store import VenueLedgerCursorWrite
from snapper.data.venue_ledger_store import VenueLedgerInsertRow
from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_LEDGER_CALL_COST
from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_LEDGER_PAGE_SIZE
from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_LEDGER_PASS_BUDGET_POINTS
from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_TRADE_BALANCE_CALL_COST
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerEntry
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPage
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPermissionMissingError
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerReadPausedError
from snapper.infrastructure.exchanges.kraken_ledger import KrakenTradeBalance
from snapper.infrastructure.exchanges.kraken_ledger import LedgerCollectRequest
from snapper.infrastructure.exchanges.kraken_ledger import LedgerPageQuery
from snapper.infrastructure.exchanges.kraken_ledger import LedgerWindowOutcome
from snapper.infrastructure.exchanges.kraken_ledger import collect_ledger_window
from snapper.infrastructure.exchanges.kraken_ledger import ledger_unix_second
from snapper.infrastructure.exchanges.kraken_ledger import trade_balance_text


class MarginLedgerRepository(Protocol):
    """Repository surface used by one margin-ledger observation pass."""

    async def get_active_portfolio_reconciliation_method_config(
        self,
        wallet_public_id: str,
        exchange: str,
        mode: str,
    ) -> PortfolioReconciliationMethodConfigRow | None:
        """Return the active reconciliation method, when one is configured.

        Args:
            wallet_public_id: Account wallet.
            exchange: Lowercase exchange name.
            mode: Account mode.

        Returns:
            The active method row, or ``None`` when the account has none.
        """
        ...

    async def get_venue_ledger_cursor(
        self,
        wallet_public_id: str,
        exchange: str,
        mode: str,
    ) -> VenueLedgerCursorRecord | None:
        """Return the stored ledger cursor for the account.

        Args:
            wallet_public_id: Account wallet.
            exchange: Lowercase exchange name.
            mode: Account mode.

        Returns:
            The cursor, or ``None`` before the first pass.
        """
        ...

    async def commit_venue_ledger_pass(
        self,
        entries: Sequence[VenueLedgerInsertRow],
        cursor: VenueLedgerCursorWrite,
    ) -> int:
        """Insert postings and save the cursor.

        Args:
            entries: Append-only postings from one pass.
            cursor: Cursor and evidence for that pass.

        Returns:
            How many postings were newly inserted.
        """
        ...


class KrakenLedgerReader(Protocol):
    """Private Kraken reads. Implementations must reuse the executor's client."""

    async def read_trade_balance(self) -> KrakenTradeBalance:
        """Read TradeBalance on the executor's Kraken client.

        Returns:
            Evidence fields from the private TradeBalance call.
        """
        ...

    async def read_venue_ledger_page(self, start: int, end: int, offset: int) -> KrakenLedgerPage:
        """Read one Ledgers page on the executor's Kraken client.

        Args:
            start: Inclusive window start as a unix second.
            end: Inclusive window end as a unix second.
            offset: Newest-first offset inside the frozen window.

        Returns:
            The page entries and Kraken's window count.
        """
        ...


@dataclass(frozen=True)
class LedgerBoundaryMark:
    """Reconciliation boundary that floors the ledger backfill."""

    as_of: datetime
    source_watermark: int
    session_id: str
    sequence_id: int


@dataclass(frozen=True)
class MarginLedgerObservationRequest:
    """One account-observer pass for Kraken margin ledger evidence."""

    repository: MarginLedgerRepository
    client: KrakenLedgerReader
    wallet_public_id: str
    exchange: str
    mode: str
    boundary: LedgerBoundaryMark | None
    pass_started_at: datetime
    paused: bool
    pause_check: Callable[[], bool] | None = None


@dataclass(frozen=True)
class _TradeRead:
    """TradeBalance evidence and the counter points it consumed."""

    points: int
    text: str | None
    observed_at: datetime | None


@dataclass(frozen=True)
class _WindowState:
    """A frozen window and its durable progress across observer passes."""

    start: int
    end: int
    next_ofs: int
    expected_count: int | None
    received_count: int
    cursor_second: int | None
    cursor_ids: tuple[str, ...]


@dataclass(frozen=True)
class _ReadyPass:
    """A pass that may call Kraken."""

    cursor: VenueLedgerCursorRecord | None
    boundary_as_of: datetime
    boundary_watermark: int
    boundary_session_id: str
    boundary_sequence_id: int
    window: _WindowState


async def run_kraken_margin_ledger_observation(request: MarginLedgerObservationRequest) -> None:
    """Ingest one budget-limited Kraken ledger window for a margin-ledger account.

    Args:
        request: Account scope, the executor's Kraken client, and pass clocks.
    """
    ready = await _ready_pass(request)
    if ready is None or (request.pause_check is not None and request.pause_check()):
        return
    trade = await _read_trade_balance(request)
    outcome = await _collect(request, ready, trade.points)
    await _persist(request, ready, trade, outcome)
    _log_outcome(request.exchange, outcome)


async def _ready_pass(request: MarginLedgerObservationRequest) -> _ReadyPass | None:
    """Return the window to read, or ``None`` when this pass must not call Kraken."""
    config = await request.repository.get_active_portfolio_reconciliation_method_config(
        request.wallet_public_id,
        request.exchange,
        request.mode,
    )
    if config is None or config["method"] != "margin_ledger_replay":
        return None
    if request.paused or (request.pause_check is not None and request.pause_check()):
        logger.info(
            "[{}] kraken margin ledger read paused for in-flight private REST",
            request.exchange,
        )
        return None
    cursor = await request.repository.get_venue_ledger_cursor(
        request.wallet_public_id,
        request.exchange,
        request.mode,
    )
    if _permission_blocked(cursor):
        logger.info(
            "[{}] kraken margin ledger permission missing; reads disabled until owner reset",
            request.exchange,
        )
        return None
    resolved = _resolved_boundary(cursor, request.boundary)
    if resolved is None:
        logger.info(
            "[{}] kraken margin ledger skipped: reconciliation boundary unavailable",
            request.exchange,
        )
        return None
    pass_end = ledger_unix_second(request.pass_started_at)
    window = _window_state(cursor, resolved[4], pass_end)
    if window.start > window.end:
        logger.info(
            "[{}] kraken margin ledger skipped: cursor is ahead of the pass", request.exchange
        )
        return None
    return _ReadyPass(
        cursor=cursor,
        boundary_as_of=resolved[0],
        boundary_watermark=resolved[1],
        boundary_session_id=resolved[2],
        boundary_sequence_id=resolved[3],
        window=window,
    )


def _window_state(
    cursor: VenueLedgerCursorRecord | None,
    start: int,
    pass_end: int,
) -> _WindowState:
    """Resume a complete persisted window, or freeze a new one at pass start."""
    if (
        cursor is not None
        and cursor.window_start is not None
        and cursor.window_end is not None
        and cursor.next_ofs is not None
    ):
        return _WindowState(
            start=cursor.window_start,
            end=cursor.window_end,
            next_ofs=cursor.next_ofs,
            expected_count=cursor.expected_count,
            received_count=cursor.ingested_count,
            cursor_second=cursor.window_cursor_second,
            cursor_ids=cursor.window_cursor_ids or (),
        )
    return _WindowState(
        start=start,
        end=pass_end,
        next_ofs=0,
        expected_count=None,
        received_count=0,
        cursor_second=None,
        cursor_ids=(),
    )


def _permission_blocked(cursor: VenueLedgerCursorRecord | None) -> bool:
    """Latch permission denial until an owner explicitly resets the evidence."""
    return cursor is not None and cursor.evidence_status == "ledger_permission_missing"


def _resolved_boundary(
    cursor: VenueLedgerCursorRecord | None,
    boundary: LedgerBoundaryMark | None,
) -> tuple[datetime, int, str, int, int] | None:
    """Return the recorded boundary and the inclusive start second.

    The first pass uses the reconciliation boundary. Later passes keep that
    boundary and either resume their frozen window or overlap the cursor second.
    """
    if cursor is not None:
        start = (
            cursor.cursor_second
            if cursor.cursor_second is not None
            else ledger_unix_second(cursor.boundary_as_of)
        )
        return (
            cursor.boundary_as_of,
            cursor.boundary_watermark,
            cursor.boundary_session_id,
            cursor.boundary_sequence_id,
            start,
        )
    if boundary is None:
        return None
    return (
        boundary.as_of,
        boundary.source_watermark,
        boundary.session_id,
        boundary.sequence_id,
        ledger_unix_second(boundary.as_of),
    )


async def _read_trade_balance(request: MarginLedgerObservationRequest) -> _TradeRead:
    """Read TradeBalance evidence. A failure still consumes its counter point."""
    try:
        balance = await request.client.read_trade_balance()
    except KrakenLedgerPermissionMissingError:
        logger.warning("[{}] kraken trade balance permission missing", request.exchange)
        return _TradeRead(KRAKEN_TRADE_BALANCE_CALL_COST, None, None)
    except Exception as exc:
        logger.warning("[{}] kraken trade balance read failed: {}", request.exchange, exc)
        return _TradeRead(KRAKEN_TRADE_BALANCE_CALL_COST, None, None)
    return _TradeRead(
        KRAKEN_TRADE_BALANCE_CALL_COST,
        trade_balance_text(balance),
        request.pass_started_at,
    )


async def _collect(
    request: MarginLedgerObservationRequest,
    ready: _ReadyPass,
    spent: int,
) -> LedgerWindowOutcome:
    """Page Ledgers with the counter points left after TradeBalance."""

    async def fetch_page(query: LedgerPageQuery) -> KrakenLedgerPage:
        if request.pause_check is not None and request.pause_check():
            raise KrakenLedgerReadPausedError
        return await request.client.read_venue_ledger_page(query.start, query.end, query.offset)

    return await collect_ledger_window(
        LedgerCollectRequest(
            start=ready.window.start,
            end=ready.window.end,
            budget_points=KRAKEN_LEDGER_PASS_BUDGET_POINTS - spent,
            call_cost=KRAKEN_LEDGER_CALL_COST,
            page_size=KRAKEN_LEDGER_PAGE_SIZE,
            offset=ready.window.next_ofs,
            expected_count=ready.window.expected_count,
            received_count=ready.window.received_count,
            window_cursor_second=ready.window.cursor_second,
            window_cursor_ids=ready.window.cursor_ids,
        ),
        fetch_page,
    )


async def _persist(
    request: MarginLedgerObservationRequest,
    ready: _ReadyPass,
    trade: _TradeRead,
    outcome: LedgerWindowOutcome,
) -> None:
    """Store postings and the cursor, keeping the original backfill boundary."""
    observation_id = str(uuid7())
    cursor_second, cursor_ids = _progress_cursor(ready.cursor, outcome)
    status = None if outcome.status == "ok" else outcome.status
    drained = outcome.status == "ok"
    await request.repository.commit_venue_ledger_pass(
        _entry_rows(request, outcome.entries, request.pass_started_at, observation_id),
        VenueLedgerCursorWrite(
            wallet_public_id=request.wallet_public_id,
            exchange=request.exchange,
            mode=request.mode,
            cursor_second=cursor_second,
            cursor_ids=cursor_ids,
            window_start=None if drained else ready.window.start,
            window_end=None if drained else ready.window.end,
            next_ofs=None if drained else outcome.next_ofs,
            expected_count=None if drained else outcome.expected_count,
            window_cursor_second=None if drained else outcome.window_cursor_second,
            window_cursor_ids=None if drained else outcome.window_cursor_ids,
            boundary_as_of=ready.boundary_as_of,
            boundary_watermark=ready.boundary_watermark,
            boundary_session_id=ready.boundary_session_id,
            boundary_sequence_id=ready.boundary_sequence_id,
            evidence_status=status,
            reported_count=outcome.reported_count,
            ingested_count=outcome.received_count,
            pass_started_at=request.pass_started_at,
            observed_at=request.pass_started_at,
            observation_id=observation_id,
            trade_balance_json=trade.text,
            trade_balance_observed_at=trade.observed_at,
            api_points_used=trade.points + outcome.points_used,
        ),
    )


def _progress_cursor(
    previous: VenueLedgerCursorRecord | None,
    outcome: LedgerWindowOutcome,
) -> tuple[int | None, tuple[str, ...]]:
    """Advance the cursor only after a reconciled window."""
    if outcome.status == "ok":
        return outcome.cursor_second, outcome.cursor_ids
    if previous is None:
        return None, ()
    return previous.cursor_second, previous.cursor_ids


def _entry_rows(
    request: MarginLedgerObservationRequest,
    entries: tuple[KrakenLedgerEntry, ...],
    observed_at: datetime,
    observation_id: str,
) -> list[VenueLedgerInsertRow]:
    """Project parsed entries into append-only insert rows."""
    return [
        {
            "wallet_public_id": request.wallet_public_id,
            "exchange": request.exchange,
            "mode": request.mode,
            "venue_ledger_id": entry.venue_ledger_id,
            "refid": entry.refid,
            "ledger_time_text": entry.ledger_time,
            "ledger_type": entry.ledger_type,
            "subtype": entry.subtype,
            "aclass": entry.aclass,
            "asset": entry.asset,
            "amount_text": entry.amount,
            "fee_text": entry.fee,
            "balance_text": entry.balance,
            "observed_at": observed_at,
            "observation_id": observation_id,
        }
        for entry in entries
    ]


def _log_outcome(exchange: str, outcome: LedgerWindowOutcome) -> None:
    """Log a reconciled pass or an evidence status."""
    if outcome.status == "ok":
        logger.info(
            "[{}] kraken margin ledger reconciled count={}",
            exchange,
            outcome.reported_count,
        )
        return
    logger.warning(
        "[{}] kraken margin ledger status {} ingested={} reported={}",
        exchange,
        outcome.status,
        len(outcome.entries),
        outcome.reported_count,
    )
