"""Tests for Kraken margin-ledger observation without verdict changes."""

import json
from collections.abc import Callable
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.portfolio import reconciliation_dispatch
from snapper.application.portfolio.kraken_margin_ledger import LedgerBoundaryMark
from snapper.application.portfolio.kraken_margin_ledger import MarginLedgerObservationRequest
from snapper.application.portfolio.kraken_margin_ledger import run_kraken_margin_ledger_observation
from snapper.application.portfolio.reconciliation_dispatch import SpotReplayBoundaryCapture
from snapper.core.types import ExchangeEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.venue_ledger_store import VenueLedgerCursorRecord
from snapper.data.venue_ledger_store import VenueLedgerCursorWrite
from snapper.data.venue_ledger_store import VenueLedgerInsertRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_LEDGER_PAGE_SIZE
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerEntry
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPage
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPermissionMissingError
from snapper.infrastructure.exchanges.kraken_ledger import KrakenTradeBalance
from snapper.infrastructure.exchanges.kraken_ledger import ledger_unix_second
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.base import PendingOrderState
from tests.application.portfolio.test_reconciliation_dispatch import _NOW as _DISPATCH_NOW
from tests.application.portfolio.test_reconciliation_dispatch import _account
from tests.application.portfolio.test_reconciliation_dispatch import _config
from tests.application.portfolio.test_reconciliation_dispatch import _evaluation
from tests.application.portfolio.test_reconciliation_dispatch import _repository

_PASS = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
_BOUNDARY_AT = _PASS - timedelta(seconds=30)
_END = ledger_unix_second(_PASS)
_START = ledger_unix_second(_BOUNDARY_AT)
_WALLET = "00000000-0000-7000-8000-000000000101"
_SESSION = "00000000-0000-7000-8000-000000000201"
_OBSERVATION = "00000000-0000-7000-8000-000000000301"
_MARK = LedgerBoundaryMark(
    as_of=_BOUNDARY_AT,
    source_watermark=4,
    session_id=_SESSION,
    sequence_id=7,
)


class _LedgerExecutor(ExchangeExecutorService[KrakenExchangeClient]):
    """Executor stub for margin-ledger observation tests."""

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Return an uninitialized client.

        Returns:
            A client shell that tests replace before use.
        """
        return KrakenExchangeClient.__new__(KrakenExchangeClient)

    def _get_exchange_name(self) -> str:
        """Return the live Kraken venue name.

        Returns:
            The Kraken exchange identifier.
        """
        return "kraken"


class _Repo:
    """In-memory method config and ledger cursor."""

    def __init__(self, method: str | None) -> None:
        self.method = method
        self.cursor: VenueLedgerCursorRecord | None = None
        self.commits: list[tuple[tuple[VenueLedgerInsertRow, ...], VenueLedgerCursorWrite]] = []

    async def get_active_portfolio_reconciliation_method_config(
        self,
        wallet_public_id: str,
        exchange: str,
        mode: str,
    ) -> PortfolioReconciliationMethodConfigRow | None:
        """Return the configured method, or none when the test leaves it unset."""
        if self.method is None:
            return None
        return {
            "wallet_public_id": wallet_public_id,
            "exchange": exchange,
            "mode": mode,
            "method": self.method,
            "classified_after_observation_id": None,
            "public_id": _OBSERVATION,
            "timestamp": _PASS,
            "session_id": _SESSION,
            "sequence_id": 1,
        }

    async def get_venue_ledger_cursor(
        self,
        wallet_public_id: str,
        exchange: str,
        mode: str,
    ) -> VenueLedgerCursorRecord | None:
        """Return the cursor saved by the previous commit."""
        del wallet_public_id, exchange, mode
        return self.cursor

    async def commit_venue_ledger_pass(
        self,
        entries: Sequence[VenueLedgerInsertRow],
        cursor: VenueLedgerCursorWrite,
    ) -> int:
        """Remember the pass and expose it as the next stored cursor."""
        self.commits.append((tuple(entries), cursor))
        self.cursor = VenueLedgerCursorRecord(
            wallet_public_id=cursor.wallet_public_id,
            exchange=cursor.exchange,
            mode=cursor.mode,
            cursor_second=cursor.cursor_second,
            cursor_ids=cursor.cursor_ids,
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
        return len(entries)


class _Reader:
    """Ledger reader double bound to one object, like the executor's client."""

    def __init__(
        self,
        pages: list[KrakenLedgerPage] | None = None,
        *,
        balance_error: Exception | None = None,
        ledger_error: Exception | None = None,
    ) -> None:
        self.pages = [] if pages is None else pages
        self.balance_error = balance_error
        self.ledger_error = ledger_error
        self.calls: list[tuple[int, int, int]] = []
        self.balance_calls = 0

    async def read_trade_balance(self) -> KrakenTradeBalance:
        """Count the call, then return evidence or the configured failure."""
        self.balance_calls += 1
        if self.balance_error is not None:
            raise self.balance_error
        return KrakenTradeBalance(eb="1", tb="1", m="0", n="1", c="0", v="0", ml="2")

    async def read_venue_ledger_page(self, start: int, end: int, offset: int) -> KrakenLedgerPage:
        """Record the frozen window and return the next page."""
        self.calls.append((start, end, offset))
        if self.ledger_error is not None:
            raise self.ledger_error
        if not self.pages:
            raise AssertionError("unexpected ledger page")
        return self.pages.pop(0)


def _executor() -> _LedgerExecutor:
    """Build an executor whose repository satisfies the observer's type gate."""
    with patch.object(
        base_module, "get_settings", return_value=MagicMock(db_url="sqlite:///:memory:")
    ):
        executor = _LedgerExecutor()
    executor.running = True
    executor.wallet_public_id = "wallet-1"
    executor.repository = MagicMock(spec=SQLAlchemyRepository)
    executor.repository.record_venue_account_snapshot = AsyncMock(return_value=1)
    executor.repository.get_spot_execution_watermark = AsyncMock(
        return_value=(0, datetime(2026, 7, 14, 9, 59, tzinfo=UTC))
    )
    return executor


def _bare_kraken() -> KrakenExchangeClient:
    """Return a Kraken client that skips balance, position, and history reads."""
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    client.balance_capability = CapabilityStatus.UNSUPPORTED
    client.position_capability = CapabilityStatus.NOT_APPLICABLE
    client.position_observation_capability = CapabilityStatus.NOT_APPLICABLE
    client.account_history_capability = CapabilityStatus.UNSUPPORTED
    return client


def _entry(ledger_id: str, second: int) -> KrakenLedgerEntry:
    """Return one posting inside a caller-chosen second."""
    return KrakenLedgerEntry(
        venue_ledger_id=ledger_id,
        refid="R",
        ledger_time=str(second),
        ledger_second=second,
        ledger_type="margin",
        subtype="",
        aclass="currency",
        asset="ZEUR",
        amount="1.0000",
        fee="0.0000",
        balance="1.0000",
    )


def _preset(
    *,
    second: int | None = None,
    ids: tuple[str, ...] = (),
    status: str | None = None,
) -> VenueLedgerCursorRecord:
    """Return a stored cursor that already recorded the test boundary."""
    return VenueLedgerCursorRecord(
        wallet_public_id=_WALLET,
        exchange="kraken",
        mode="live",
        cursor_second=second,
        cursor_ids=ids,
        boundary_as_of=_BOUNDARY_AT,
        boundary_watermark=4,
        boundary_session_id=_SESSION,
        boundary_sequence_id=7,
        evidence_status=status,
        reported_count=None,
        ingested_count=0,
        pass_started_at=_PASS,
        observed_at=_PASS,
        observation_id=_OBSERVATION,
        trade_balance_json=None,
        trade_balance_observed_at=None,
        api_points_used=0,
    )


def _full(prefix: str, count: int) -> KrakenLedgerPage:
    """Return one full newest page whose reported count is still unfinished."""
    entries = tuple(_entry(f"{prefix}{index}", _END) for index in range(KRAKEN_LEDGER_PAGE_SIZE))
    return KrakenLedgerPage(entries=entries, count=count)


async def _observe(
    repo: _Repo,
    reader: _Reader,
    *,
    paused: bool | Callable[[], bool] = False,
    boundary: LedgerBoundaryMark | None = _MARK,
    started: datetime = _PASS,
) -> VenueLedgerCursorWrite | None:
    """Run one observation and return the cursor write when one was stored."""
    await run_kraken_margin_ledger_observation(
        MarginLedgerObservationRequest(
            repository=repo,
            client=reader,
            wallet_public_id=_WALLET,
            exchange="kraken",
            mode="live",
            boundary=boundary,
            pass_started_at=started,
            paused=paused if isinstance(paused, bool) else False,
            pause_check=paused if callable(paused) else None,
        )
    )
    if not repo.commits:
        return None
    return repo.commits[-1][1]


@pytest.mark.asyncio
async def test_observe_once_uses_the_executor_kraken_client() -> None:
    """The account observer passes its own Kraken client to ledger observation.

    Given a live executor whose exchange client is an existing Kraken instance,
    When the account observer completes one cycle,
    Then ledger observation receives that same instance and does not construct
        another Kraken client.
    """
    executor = _executor()
    client = _bare_kraken()
    executor.exchange_client = client
    with (
        patch.object(KrakenExchangeClient, "__init__", side_effect=AssertionError("second client")),
        patch.object(base_module, "run_kraken_margin_ledger_observation", new=AsyncMock()) as run,
    ):
        await executor._observe_account_once()
    run.assert_awaited_once()
    assert run.await_args is not None
    request = cast(MarginLedgerObservationRequest, run.await_args.args[0])
    assert request.client is client
    assert request.wallet_public_id == "wallet-1"
    assert request.exchange == "kraken"
    assert request.mode == "live"
    cast(MagicMock, executor.repository).record_venue_account_snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_observe_once_keeps_the_snapshot_when_ledger_observation_fails() -> None:
    """A ledger failure does not discard the account snapshot.

    Given a Kraken client whose ledger observation raises,
    When the account observer runs,
    Then the snapshot is still recorded and the exception does not escape.
    """
    executor = _executor()
    executor.exchange_client = _bare_kraken()
    with patch.object(
        base_module,
        "run_kraken_margin_ledger_observation",
        new=AsyncMock(side_effect=RuntimeError("ledger boom")),
    ):
        await executor._observe_account_once()
    cast(MagicMock, executor.repository).record_venue_account_snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_margin_ledger_observer_skips_paper_and_non_kraken_clients() -> None:
    """Ledger observation runs only for the live Kraken client.

    Given a paper executor and a non-Kraken client,
    When each account cycle reaches the ledger hook,
    Then neither invokes ledger observation.
    """
    executor = _executor()
    with patch.object(base_module, "run_kraken_margin_ledger_observation", new=AsyncMock()) as run:
        with patch.object(executor, "_get_exchange_name", return_value=ExchangeEnum.PAPER):
            await executor._observe_kraken_margin_ledger(
                executor.repository,
                _bare_kraken(),
                None,
                _PASS,
            )
        await executor._observe_kraken_margin_ledger(
            executor.repository,
            MagicMock(),
            None,
            _PASS,
        )
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_margin_ledger_reads_pause_for_open_or_inflight_commands() -> None:
    """Open and in-flight order work pauses ledger reads.

    Given a pending order, then an in-flight command, then neither,
    When the ledger hook builds its request,
    Then the first two requests are paused and the idle request is not.
    """
    executor = _executor()
    client = _bare_kraken()
    with patch.object(base_module, "run_kraken_margin_ledger_observation", new=AsyncMock()) as run:
        executor.pending_orders["cid"] = cast(PendingOrderState, object())
        executor._order_inflight_started = None
        await executor._observe_kraken_margin_ledger(executor.repository, client, None, _PASS)
        executor.pending_orders.clear()
        executor._order_inflight_started = 1.0
        await executor._observe_kraken_margin_ledger(executor.repository, client, None, _PASS)
        executor._order_inflight_started = None
        await executor._observe_kraken_margin_ledger(executor.repository, client, None, _PASS)
    paused = [
        cast(MarginLedgerObservationRequest, call.args[0]).paused for call in run.await_args_list
    ]
    assert paused == [True, True, False]


@pytest.mark.asyncio
async def test_margin_ledger_hook_copies_the_reconciliation_boundary() -> None:
    """The ledger floor is the reconciliation boundary from this cycle.

    Given one captured boundary and one absent capture,
    When the ledger hook runs,
    Then the request carries that boundary's as-of, watermark, session, and sequence,
        or no boundary when the capture is absent.
    """
    executor = _executor()
    captured = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
    boundary = SpotReplayBoundaryCapture(
        wallet_public_id="wallet-1",
        exchange="kraken",
        mode="live",
        session_id="session-1",
        sequence_id=7,
        source_watermark=4,
        as_of=captured - timedelta(seconds=4),
        watermark_captured_at=captured - timedelta(seconds=3),
        request_started_at=captured - timedelta(seconds=2),
        request_completed_at=captured - timedelta(seconds=1),
        watermark_after=4,
        watermark_after_captured_at=captured,
        watermark_unchanged=True,
    )
    with patch.object(base_module, "run_kraken_margin_ledger_observation", new=AsyncMock()) as run:
        await executor._observe_kraken_margin_ledger(
            executor.repository,
            _bare_kraken(),
            boundary,
            _PASS,
        )
        await executor._observe_kraken_margin_ledger(
            executor.repository,
            _bare_kraken(),
            None,
            _PASS,
        )
    copied = cast(MarginLedgerObservationRequest, run.await_args_list[0].args[0]).boundary
    assert copied is not None
    assert copied.as_of == boundary.as_of
    assert copied.source_watermark == 4
    assert copied.session_id == "session-1"
    assert copied.sequence_id == 7
    assert cast(MarginLedgerObservationRequest, run.await_args_list[1].args[0]).boundary is None


def test_position_evidence_is_serialized_only_when_present() -> None:
    """Kraken evidence fields stay out of snapshots that do not have them.

    Given one position with vol_closed, fee, and rollovertm and one without,
    When open positions are serialized,
    Then only the populated snapshot contains those keys.
    """
    observed_at = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
    present = OpenPositionSnapshot(
        symbol="BTC-EUR",
        side=OrderSideEnum.SELL,
        size=0.01,
        entry_price=50000.0,
        mark_price=51000.0,
        unrealized_pnl=-10.0,
        unrealized_funding=0.0,
        timestamp=observed_at,
        vol_closed=0.0,
        fee="0.13000",
        rollovertm="1785585600",
    )
    absent = OpenPositionSnapshot(
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        size=1.0,
        entry_price=100.0,
        mark_price=110.0,
        unrealized_pnl=10.0,
        unrealized_funding=0.0,
        timestamp=observed_at,
    )
    parsed = cast(
        list[dict[str, object]],
        json.loads(ExchangeExecutorService._serialize_open_positions([present, absent])),
    )
    assert parsed[0]["vol_closed"] == 0.0
    assert parsed[0]["fee"] == "0.13000"
    assert parsed[0]["rollovertm"] == "1785585600"
    assert "vol_closed" not in parsed[1]
    assert "fee" not in parsed[1]
    assert "rollovertm" not in parsed[1]


@pytest.mark.asyncio
async def test_observation_runs_only_for_margin_ledger_replay() -> None:
    """Spot and unclassified accounts do not call Kraken.

    Given a spot method and an account with no method,
    When each observation pass runs,
    Then neither reads TradeBalance or Ledgers.
    """
    spot = _Reader()
    await _observe(_Repo("spot_execution_replay"), spot)
    missing = _Reader()
    await _observe(_Repo(None), missing)
    assert spot.balance_calls == 0
    assert spot.calls == []
    assert missing.balance_calls == 0
    assert missing.calls == []


@pytest.mark.asyncio
async def test_paused_or_unbounded_passes_do_not_call_kraken() -> None:
    """Pause, a missing boundary, and a future cursor suppress private reads.

    Given a paused margin account, an account with no boundary, and a cursor
        ahead of the pass,
    When each pass runs,
    Then none of them calls Kraken.
    """
    paused = _Reader()
    await _observe(_Repo("margin_ledger_replay"), paused, paused=True)
    unbounded = _Reader()
    await _observe(_Repo("margin_ledger_replay"), unbounded, boundary=None)
    ahead_repo = _Repo("margin_ledger_replay")
    ahead_repo.cursor = _preset(second=_END + 5)
    ahead = _Reader()
    await _observe(ahead_repo, ahead)
    assert paused.balance_calls == 0
    assert unbounded.balance_calls == 0
    assert ahead.balance_calls == 0
    assert ahead.calls == []


@pytest.mark.asyncio
async def test_permission_missing_is_stored_and_not_retried() -> None:
    """A missing ledger permission disables automatic retries.

    Given Ledgers denying permission,
    When the pass stores that status and another pass runs days later,
    Then the later pass makes no private call.
    """
    reader = _Reader(ledger_error=KrakenLedgerPermissionMissingError("EAPI:Invalid permissions"))
    repo = _Repo("margin_ledger_replay")
    first = await _observe(repo, reader)
    assert first is not None
    assert first.evidence_status == "ledger_permission_missing"
    assert first.cursor_second is None
    assert len(reader.calls) == 1
    await _observe(repo, reader, started=_PASS + timedelta(days=7))
    assert reader.balance_calls == 1
    assert len(reader.calls) == 1
    assert len(repo.commits) == 1


@pytest.mark.asyncio
async def test_pass_budget_stops_before_the_reported_count() -> None:
    """One pass spends at most the ledger budget and records backlog.

    Given a margin account whose window count needs more pages than the budget,
    When the observer runs,
    Then it reads TradeBalance once and two Ledgers pages, and the cursor stays put.
    """
    count = KRAKEN_LEDGER_PAGE_SIZE * 3
    reader = _Reader([_full("A", count), _full("B", count)])
    stored = await _observe(_Repo("margin_ledger_replay"), reader)
    assert reader.balance_calls == 1
    assert [call[2] for call in reader.calls] == [0, KRAKEN_LEDGER_PAGE_SIZE]
    assert all(call[0] == _START and call[1] == _END for call in reader.calls)
    assert stored is not None
    assert stored.evidence_status == "ledger_backlog"
    assert stored.cursor_second is None
    assert stored.api_points_used == 5
    assert stored.boundary_as_of == _BOUNDARY_AT
    assert stored.ingested_count == KRAKEN_LEDGER_PAGE_SIZE * 2


@pytest.mark.asyncio
async def test_cursor_overlaps_the_boundary_second_and_keeps_the_boundary() -> None:
    """The next pass rereads the cursor second and does not move the boundary.

    Given a reconciled pass whose newest second becomes the next start,
    When a later pass supplies a different reconciliation boundary,
    Then the Ledgers start is that second and the stored boundary is the first one.
    """
    reader = _Reader(
        [
            KrakenLedgerPage(entries=(_entry("OLD", _END - 1), _entry("EDGE", _END)), count=2),
            KrakenLedgerPage(entries=(_entry("EDGE", _END), _entry("NEW", _END + 20)), count=2),
        ]
    )
    repo = _Repo("margin_ledger_replay")
    first = await _observe(repo, reader)
    assert first is not None
    assert first.cursor_second == _END
    assert first.cursor_ids == ("EDGE",)
    later = LedgerBoundaryMark(
        as_of=_PASS,
        source_watermark=99,
        session_id="00000000-0000-7000-8000-000000000299",
        sequence_id=99,
    )
    second = await _observe(
        repo,
        reader,
        boundary=later,
        started=_PASS + timedelta(seconds=20),
    )
    assert reader.calls[1][0] == _END
    assert second is not None
    assert second.boundary_as_of == _BOUNDARY_AT
    assert second.boundary_watermark == 4
    assert second.cursor_second == _END + 20
    assert {row["venue_ledger_id"] for row in repo.commits[1][0]} == {"EDGE", "NEW"}


@pytest.mark.asyncio
async def test_count_gap_does_not_advance_an_existing_cursor() -> None:
    """A reconciled count mismatch stores ledger_cursor_gap and keeps the cursor.

    Given a cursor at an earlier second and a short page whose count does not match,
    When the pass finishes,
    Then the evidence status is ledger_cursor_gap and the previous cursor remains.
    """
    repo = _Repo("margin_ledger_replay")
    repo.cursor = _preset(second=_START, ids=("KEEP",))
    reader = _Reader([KrakenLedgerPage(entries=(_entry("A", _END),), count=2)])
    stored = await _observe(repo, reader)
    assert stored is not None
    assert stored.evidence_status == "ledger_cursor_gap"
    assert stored.cursor_second == _START
    assert stored.cursor_ids == ("KEEP",)
    assert stored.boundary_as_of == _BOUNDARY_AT


@pytest.mark.asyncio
async def test_backlog_without_a_cursor_starts_at_the_boundary() -> None:
    """The first pass floors the backfill at the reconciliation boundary.

    Given no stored cursor and a window the budget cannot finish,
    When the pass runs,
    Then Ledgers starts at the boundary second and the cursor is not advanced.
    """
    count = KRAKEN_LEDGER_PAGE_SIZE * 3
    reader = _Reader([_full("A", count), _full("B", count)])
    stored = await _observe(_Repo("margin_ledger_replay"), reader)
    assert reader.calls[0][0] == _START
    assert stored is not None
    assert stored.cursor_second is None
    assert stored.boundary_as_of == _BOUNDARY_AT
    assert stored.boundary_session_id == _SESSION
    assert stored.boundary_sequence_id == 7


@pytest.mark.asyncio
async def test_trade_balance_failure_still_reads_ledgers() -> None:
    """TradeBalance failure is evidence loss, not a ledger-permission failure.

    Given TradeBalance raising a runtime error and a reconcilable ledger page,
    When the pass runs,
    Then Ledgers is read, the cursor advances, and no trade-balance text is stored.
    """
    reader = _Reader(
        [KrakenLedgerPage(entries=(_entry("A", _END),), count=1)],
        balance_error=RuntimeError("balance down"),
    )
    stored = await _observe(_Repo("margin_ledger_replay"), reader)
    assert reader.calls[0][0] == _START
    assert stored is not None
    assert stored.evidence_status is None
    assert stored.trade_balance_json is None
    assert stored.cursor_second == _END
    assert stored.api_points_used == 3


@pytest.mark.asyncio
async def test_trade_balance_permission_does_not_mark_ledger_permission() -> None:
    """Only a Ledgers permission error sets ledger_permission_missing.

    Given TradeBalance denying permission and a reconcilable ledger page,
    When the pass runs,
    Then the evidence status stays reconciled and no backoff is stored.
    """
    reader = _Reader(
        [KrakenLedgerPage(entries=(_entry("A", _END),), count=1)],
        balance_error=KrakenLedgerPermissionMissingError("Permission denied"),
    )
    stored = await _observe(_Repo("margin_ledger_replay"), reader)
    assert stored is not None
    assert stored.evidence_status is None
    assert stored.trade_balance_json is None
    assert len(reader.calls) == 1


@pytest.mark.asyncio
async def test_naive_pass_timestamp_is_rejected_before_a_private_call() -> None:
    """A naive pass clock is not used as a ledger window end.

    Given a margin account and a naive pass start,
    When observation runs,
    Then ValueError is raised and Kraken is not called.
    """
    reader = _Reader()
    with pytest.raises(ValueError, match="timezone-aware"):
        await _observe(
            _Repo("margin_ledger_replay"),
            reader,
            started=datetime(2026, 11, 23, 18, 0),
        )
    assert reader.balance_calls == 0


@pytest.mark.asyncio
async def test_empty_reconciled_window_advances_the_cursor_to_the_pass_end() -> None:
    """A zero count moves the cursor to the fixed pass end.

    Given a margin account and an empty Ledgers page whose count is zero,
    When the pass reconciles,
    Then the cursor second is the pass-start second and no posting is stored.
    """
    repo = _Repo("margin_ledger_replay")
    reader = _Reader([KrakenLedgerPage(entries=(), count=0)])
    stored = await _observe(repo, reader)
    assert stored is not None
    assert stored.evidence_status is None
    assert stored.cursor_second == _END
    assert stored.cursor_ids == ()
    assert stored.ingested_count == 0
    assert repo.commits[0][0] == ()
    assert stored.trade_balance_json == (
        '{"c":"0","eb":"1","m":"0","ml":"2","n":"1","tb":"1","v":"0"}'
    )


@pytest.mark.asyncio
async def test_dispatcher_still_returns_margin_ledger_replay_not_implemented() -> None:
    """Observation does not change the margin-ledger reconciliation verdict.

    Given a live account configured for margin ledger replay,
    When the reconciliation dispatcher selects that method,
    Then the result remains error / margin_ledger_replay_not_implemented.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("margin_ledger_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _DISPATCH_NOW,
    )
    assert result == _evaluation(
        "margin_ledger_replay",
        "error",
        "margin_ledger_replay_not_implemented",
        exchange="kraken",
    )
    signal_read.assert_not_awaited()
    bundle_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("pause_at", [0, 1, 2, 3])
async def test_new_command_activity_pauses_subsequent_private_reads(pause_at: int) -> None:
    """Recheck executor activity after repository awaits and between pages.

    Given command activity beginning during setup, TradeBalance, or paging,
    When the observer reaches its next private call,
    Then it pauses without spending another API-counter reservation.
    """
    checks = 0

    def paused() -> bool:
        nonlocal checks
        checks += 1
        return checks > pause_at

    reader = _Reader([_full("A", KRAKEN_LEDGER_PAGE_SIZE * 2)])
    repo = _Repo("margin_ledger_replay")
    stored = await _observe(repo, reader, paused=paused)
    if pause_at < 2:
        assert reader.balance_calls == 0
        assert stored is None
    else:
        assert reader.balance_calls == 1
        assert len(reader.calls) == pause_at - 2
        assert stored is not None
        assert stored.evidence_status == "ledger_backlog"
        assert stored.api_points_used == 1 + 2 * len(reader.calls)
