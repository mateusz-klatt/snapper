"""Tests for venue reconciliation in executor."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest
from loguru import logger

from snapper.application.trade.command_request import order_request_from_command
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.base import PendingOrderState


class _DummyExecutor(ExchangeExecutorService[Any]):
    """Test executor stub."""

    def _create_exchange_client(self) -> Any:
        return MagicMock()

    def _get_exchange_name(self) -> str:
        return "kraken"


def _make_executor() -> Any:
    """Build a minimal executor with mocked infrastructure."""
    with patch.object(
        base_module, "get_settings", return_value=MagicMock(db_url="sqlite:///:memory:")
    ):
        ex: Any = _DummyExecutor()
    ex.running = True
    ex.settings = MagicMock()
    ex.settings.recon_balance_threshold = 1.0
    ex.exchange_client = AsyncMock()
    ex.repository = MagicMock()
    return ex


def _make_order_snapshot(
    order_id: str = "ex-1",
    filled: float = 0.0,
    price: float | None = 100.0,
    status: ExchangeOrderStatusEnum = ExchangeOrderStatusEnum.OPEN,
    client_order_id: str | None = "cid-1",
) -> ExchangeOrderSnapshot:
    """Build an ExchangeOrderSnapshot for testing."""
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id=client_order_id,
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=price,
        status=status,
        filled=filled,
        remaining=1.0 - filled,
        timestamp=datetime.now(UTC).timestamp(),
    )


def _make_pending(
    exchange_order_id: str = "ex-1",
    cum_qty: float = 0.0,
    instrument: str = "BTC-USD",
    side: str = "buy",
) -> PendingOrderState:
    """Build a PendingOrderState for testing."""
    request = SimpleNamespace(
        instrument=instrument,
        side=side,
        price=100.0,
        client_order_id="cid-1",
        strategy_tag=None,
    )
    pending = PendingOrderState(request=request)
    pending.exchange_order_id = exchange_order_id
    pending.last_seen_cum_qty = cum_qty
    return pending


class TestReconciliation:
    """Venue reconciliation detects fill gaps and disappeared orders."""

    @pytest.mark.asyncio
    async def test_recon_detects_disappeared_order(self) -> None:
        """Pending order not on exchange is processed as terminal.

        Given: pending order with exchange_order_id="ex-1", exchange returns empty,
        When: _reconcile_with_exchange runs,
        Then: _process_execution is called with canceled execution.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        canceled_snap = _make_order_snapshot(filled=0.0, status=ExchangeOrderStatusEnum.CANCELED)
        ex.exchange_client.get_order = AsyncMock(return_value=canceled_snap)
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_called_once()
        corrective = ex._process_execution.call_args.args[0]
        assert corrective.exec_type == "canceled"
        assert corrective.order_id == "ex-1"

    @pytest.mark.asyncio
    async def test_recon_disappeared_order_with_fill_emits_gap_then_terminal(self) -> None:
        """Disappeared order that was filled emits corrective fill then closed.

        Given: pending with cum_qty=0, get_order shows filled=1.0 and status=closed,
        When: _reconcile_with_exchange runs,
        Then: two calls: first a corrective fill, then a terminal "filled" event.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        filled_snap = _make_order_snapshot(
            filled=1.0, price=100.0, status=ExchangeOrderStatusEnum.CLOSED
        )
        ex.exchange_client.get_order = AsyncMock(return_value=filled_snap)
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        assert ex._process_execution.call_count == 2
        fill_call = ex._process_execution.call_args_list[0].args[0]
        terminal_call = ex._process_execution.call_args_list[1].args[0]
        assert fill_call.exec_type == "trade"
        assert fill_call.last_qty == pytest.approx(1.0)
        assert terminal_call.exec_type == "filled"
        assert terminal_call.cum_qty is None

    @pytest.mark.asyncio
    async def test_recon_disappeared_closed_no_price_skips_fill_emits_terminal(self) -> None:
        """Disappeared CLOSED order with no price skips fill gap, emits terminal only.

        Given: pending with cum_qty=0, get_order shows filled=1.0 but price=None, status=CLOSED,
        When: _reconcile_with_exchange runs,
        Then: corrective fill is skipped (no price), only terminal "filled" emitted without cum_qty.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        closed_no_price = _make_order_snapshot(
            filled=1.0, price=None, status=ExchangeOrderStatusEnum.CLOSED
        )
        ex.exchange_client.get_order = AsyncMock(return_value=closed_no_price)
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(return_value=None)
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_called_once()
        terminal = ex._process_execution.call_args.args[0]
        assert terminal.exec_type == "filled"
        assert terminal.cum_qty is None
        assert terminal.last_qty is None

    @pytest.mark.asyncio
    async def test_recon_disappeared_expired_order(self) -> None:
        """Disappeared expired order emits terminal 'expired' event.

        Given: pending order, get_order returns EXPIRED,
        When: _reconcile_with_exchange runs,
        Then: terminal event has exec_type="expired".
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        expired_snap = _make_order_snapshot(filled=0.0, status=ExchangeOrderStatusEnum.EXPIRED)
        ex.exchange_client.get_order = AsyncMock(return_value=expired_snap)
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_called_once()
        corrective = ex._process_execution.call_args.args[0]
        assert corrective.exec_type == "expired"

    @pytest.mark.asyncio
    async def test_recon_disappeared_non_terminal_status_deferred(self) -> None:
        """Disappeared order with non-terminal status stays pending for retry.

        Given: pending order, get_order returns OPEN (API race),
        When: _reconcile_with_exchange runs,
        Then: no terminal event emitted, order stays in pending.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        open_snap = _make_order_snapshot(filled=0.0, status=ExchangeOrderStatusEnum.OPEN)
        ex.exchange_client.get_order = AsyncMock(return_value=open_snap)
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()
        assert "cid-1" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_recon_disappeared_get_order_fails_skips(self) -> None:
        """get_order failure for disappeared order skips gracefully.

        Given: pending order, get_order raises,
        When: _reconcile_with_exchange runs,
        Then: no corrective event, no crash.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_order = AsyncMock(side_effect=RuntimeError("timeout"))
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_detects_fill_gap(self) -> None:
        """Exchange shows more fills than local — corrective fill is processed.

        Given: pending with cum_qty=5, exchange shows filled=10 at price=100,
        When: _reconcile_with_exchange runs,
        Then: _process_execution is called with delta fill of 5.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=5.0)
        snap = _make_order_snapshot(filled=10.0, price=100.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_called_once()
        corrective = ex._process_execution.call_args.args[0]
        assert corrective.exec_type == "trade"
        assert corrective.last_qty == pytest.approx(5.0)
        assert corrective.last_price == pytest.approx(100.0)
        assert corrective.cum_qty == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_recon_no_action_when_in_sync(self) -> None:
        """Exchange matches local state — no corrective events.

        Given: pending with cum_qty=5, exchange shows filled=5,
        When: _reconcile_with_exchange runs,
        Then: _process_execution is not called.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=5.0)
        snap = _make_order_snapshot(filled=5.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_balance_mismatch_logs_warning(self) -> None:
        """Balance mismatch beyond threshold triggers WARNING log.

        Given: exchange balance where free+used != total (beyond threshold),
        When: _reconcile_with_exchange runs,
        Then: logger.warning is called with mismatch details.
        """
        ex = _make_executor()
        ex.settings.recon_balance_threshold = 0.5
        bal = AccountBalance(currency="USD", free=100.0, used=50.0, total=200.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={"USD": bal})
        ex._process_execution = AsyncMock()

        with patch.object(base_module.logger, "warning") as mock_warn:
            await ex._reconcile_with_exchange()

        assert any("balance mismatch" in str(c) for c in mock_warn.call_args_list)

    @pytest.mark.asyncio
    async def test_recon_handler_respects_running_flag(self) -> None:
        """Reconciliation handler exits when running becomes False.

        Given: executor with running=False,
        When: _reconciliation_handler starts,
        Then: it exits immediately without calling _reconcile_with_exchange.
        """
        ex = _make_executor()
        ex.running = False
        ex._reconcile_with_exchange = AsyncMock()

        await ex._reconciliation_handler()

        ex._reconcile_with_exchange.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_handler_httpx_error_logs_warning_not_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Transient httpx.HTTPError in reconciliation logs WARNING, not ERROR+TB.

        Given: An executor whose ``_reconcile_with_exchange`` raises
            ``httpx.ConnectTimeout`` on the first cycle and flips
            ``running`` to False to terminate the loop,
        When: ``_reconciliation_handler`` runs that single failing cycle,
        Then: The transient HTTP error is logged as WARNING with
            'transient HTTP error' and 'will retry', and no ERROR
            record is emitted. Per the clean-signal-log rule,
            ``logger.exception`` (ERROR + TB) is reserved for unexpected
            non-HTTP failures so a flaky exchange API does not bury the
            real signal in a flood of stack traces.
        """
        ex = _make_executor()
        call_count = 0

        async def _side_effect() -> None:
            nonlocal call_count
            call_count += 1
            ex.running = False
            raise httpx.ConnectTimeout("connect timeout")

        ex._reconcile_with_exchange = AsyncMock(side_effect=_side_effect)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            with (
                patch("snapper.messaging.executors.base.asyncio.sleep", new_callable=AsyncMock),
                caplog.at_level("DEBUG"),
            ):
                await ex._reconciliation_handler()
        finally:
            logger.remove(sink_id)
        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any(
            "transient HTTP error" in r.message and "will retry" in r.message
            for r in warning_records
        ), f"expected transient-WARNING, got {[r.message for r in warning_records]}"
        assert (
            not error_records
        ), f"httpx errors must not log ERROR+TB, got {[r.message for r in error_records]}"

    @pytest.mark.asyncio
    async def test_recon_handler_runs_cycle_then_stops(self) -> None:
        """Reconciliation handler runs one cycle when running then stops.

        Given: executor with running=True that flips to False after first cycle,
        When: _reconciliation_handler runs,
        Then: _reconcile_with_exchange is called once, exception is caught.
        """
        ex = _make_executor()
        call_count = 0

        async def _side_effect() -> None:
            nonlocal call_count
            call_count += 1
            ex.running = False
            raise RuntimeError("test error")

        ex._reconcile_with_exchange = AsyncMock(side_effect=_side_effect)

        with patch("snapper.messaging.executors.base.asyncio.sleep", new_callable=AsyncMock):
            await ex._reconciliation_handler()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_recon_balance_within_threshold_no_warning(self) -> None:
        """Balance within threshold does not trigger warning.

        Given: exchange balance where free+used == total (within threshold),
        When: _reconcile_with_exchange runs,
        Then: no warning logged for the balanced currency.
        """
        ex = _make_executor()
        ex.settings.recon_balance_threshold = 1.0
        bal = AccountBalance(currency="USD", free=100.0, used=50.0, total=150.0)
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={"USD": bal})
        ex._process_execution = AsyncMock()

        with patch.object(base_module.logger, "warning") as mock_warn:
            await ex._reconcile_with_exchange()

        assert not any("balance mismatch" in str(c) for c in mock_warn.call_args_list)

    @pytest.mark.asyncio
    async def test_recon_exchange_api_failure_propagates(self) -> None:
        """Exchange API failure in _reconcile_with_exchange propagates.

        Given: exchange_client.get_orders raises RuntimeError,
        When: _reconcile_with_exchange is called,
        Then: the exception propagates (caught by _reconciliation_handler).
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(side_effect=RuntimeError("API down"))

        with pytest.raises(RuntimeError, match="API down"):
            await ex._reconcile_with_exchange()

    @pytest.mark.asyncio
    async def test_recon_skips_pending_without_exchange_order_id(self) -> None:
        """Pending orders without exchange_order_id are skipped.

        Given: pending order with exchange_order_id=None,
        When: _reconcile_with_exchange runs,
        Then: no corrective action taken.
        """
        ex = _make_executor()
        pending = _make_pending()
        pending.exchange_order_id = None
        ex.pending_orders["cid-1"] = pending
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_fill_gap_skips_market_order_without_price(self) -> None:
        """Fill gap with no price anywhere (snapshot + fills) is skipped with error log.

        Given: exchange shows fill gap, order price is None, and the venue
            fills VWAP lookup also yields nothing,
        When: _reconcile_with_exchange runs,
        Then: no corrective fill processed, error logged — the documented
            fail-safe skip.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(return_value=None)
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_fill_gap_resolves_price_from_venue_fill_vwap(self) -> None:
        """A priceless market-order gap heals via the venue fills VWAP.

        Given: exchange shows a fill gap with price=None but the venue's
            per-order fills lookup returns a VWAP,
        When: _reconcile_with_exchange runs,
        Then: the corrective fill is emitted at the venue VWAP instead of
            being skipped.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(return_value=(101.5, 5.0))
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex.exchange_client.get_order_fill_vwap.assert_awaited_once_with("ex-1")
        ex._process_execution.assert_called_once()
        corrective = ex._process_execution.call_args.args[0]
        assert corrective.last_price == 101.5

    @pytest.mark.asyncio
    async def test_recon_fill_gap_refuses_partial_page_vwap(self) -> None:
        """A VWAP covering only part of the order's fills is never applied.

        Given: a priceless fill gap (order filled=5.0) whose venue fills page
            covers only 3.0 of quantity,
        When: _reconcile_with_exchange runs,
        Then: the corrective fill is skipped — a partial-page average must
            never price the whole gap (it feeds PnL and cash projections).
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(return_value=(101.5, 3.0))
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_fill_gap_vwap_coverage_tolerates_float_rounding(self) -> None:
        """Coverage a few ulps short of the filled qty still heals the gap.

        Given: a priceless fill gap (filled=5.0) whose fills-page coverage is
            4.9999999 (venue decimal rounding + float summation shortfall,
            within the relative 1e-6 tolerance),
        When: _reconcile_with_exchange runs,
        Then: the VWAP is trusted and the corrective fill is emitted — a
            legitimately full page is never refused as partial over float
            noise.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(return_value=(101.5, 4.9999999))
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_called_once()
        corrective = ex._process_execution.call_args.args[0]
        assert corrective.last_price == 101.5

    @pytest.mark.asyncio
    async def test_recon_fill_gap_vwap_lookup_failure_falls_back_to_skip(self) -> None:
        """A failing venue fills lookup degrades to the fail-safe skip.

        Given: a priceless fill gap whose VWAP lookup raises (venue hiccup),
        When: _reconcile_with_exchange runs,
        Then: the cycle survives and the corrective fill is skipped — a
            transient fills-endpoint error never crashes reconciliation.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex.exchange_client.get_order_fill_vwap = AsyncMock(side_effect=RuntimeError("boom"))
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex._process_execution.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_no_exchange_client_is_noop(self) -> None:
        """Reconciliation with no exchange client is a silent no-op.

        Given: exchange_client is None,
        When: _reconcile_with_exchange runs,
        Then: no error, no action.
        """
        ex = _make_executor()
        ex.exchange_client = None

        await ex._reconcile_with_exchange()


class TestAmbiguousReconResolution:
    """Recon-loop resolution of parked ambiguous entries."""

    @pytest.mark.asyncio
    async def test_parked_ambiguous_entry_gets_verification_round(self) -> None:
        """A parked ambiguous entry is no longer silently skipped.

        Given: A pending entry without exchange id, flagged ambiguous,
        When: One reconciliation cycle runs,
        Then: The verification routine is invoked for it.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        pending.submit_ambiguous = True
        pending.unknown_published = True
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock(return_value=True)
        await ex._reconcile_with_exchange()
        ex._verify_ambiguous_submit.assert_awaited_once_with(pending.request, pending)

    @pytest.mark.asyncio
    async def test_plain_no_id_entry_is_still_skipped(self) -> None:
        """A non-ambiguous entry without exchange id stays untouched.

        Given: A pending entry without exchange id and no ambiguity,
        When: One reconciliation cycle runs,
        Then: No verification is attempted (pre-ACK orders are simply
            not reconciled, as before).
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock()
        await ex._reconcile_with_exchange()
        ex._verify_ambiguous_submit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolved_entry_retries_unconfirmed_unknown_publish(self) -> None:
        """An undelivered UNKNOWN publish is retried each cycle.

        Given: A parked entry whose UNKNOWN publish never confirmed,
        When: The recon resolver fails to verify again,
        Then: One publish retry happens and a confirmed send sets the
            flag so the engine guard holds.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        pending.submit_ambiguous = True
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock(return_value=False)
        ex._publish_order_status = AsyncMock(return_value=True)
        await ex._reconcile_with_exchange()
        ex._publish_order_status.assert_awaited_once_with(pending.request, "unknown")
        assert pending.unknown_published is True

    @pytest.mark.asyncio
    async def test_resolved_entry_skips_unknown_retouch(self) -> None:
        """A resolved entry never re-publishes UNKNOWN.

        Given: A parked entry the resolver settles this cycle,
        When: The cycle completes,
        Then: No UNKNOWN publish retry happens.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        pending.submit_ambiguous = True
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock(return_value=True)
        ex._publish_order_status = AsyncMock()
        await ex._reconcile_with_exchange()
        ex._publish_order_status.assert_not_awaited()

    def _accept_event(self, exchange_order_id: str = "ex-1") -> dict[str, object]:
        """Build the queued order_accepted event params."""
        return {
            "event_type": "order_accepted",
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
            "exchange_order_id": exchange_order_id,
            "client_order_id": "cid-1",
            "side": "buy",
            "strategy_tag": None,
        }

    @pytest.mark.asyncio
    async def test_accept_event_pending_is_healed(self) -> None:
        """A failed durable order_accepted write heals through recon.

        Given: A queued unhealed accept event for a live pending entry,
        When: One reconciliation cycle runs and the write succeeds,
        Then: The order_accepted venue event is recorded, the queue
            entry is removed, and the pending flag clears.
        """
        ex = _make_executor()
        snapshot = _make_order_snapshot(order_id="ex-1")
        ex.exchange_client.get_orders = AsyncMock(return_value=[snapshot])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="ex-1")
        pending.accept_event_pending = True
        ex.pending_orders["cid-1"] = pending
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        ex._reconcile_fill_gap = AsyncMock()
        await ex._reconcile_with_exchange()
        event = ex._record_venue_event.await_args.args[0]
        assert event["event_type"] == "order_accepted"
        assert event["exchange_order_id"] == "ex-1"
        assert "cid-1" not in ex._unhealed_accept_events
        assert pending.accept_event_pending is False

    @pytest.mark.asyncio
    async def test_accept_event_retry_failure_keeps_queue(self) -> None:
        """A still-failing durable write keeps the event queued.

        Given: A queued accept event whose write raises again,
        When: One reconciliation cycle runs,
        Then: The queue entry and flag stay set and the cycle continues
            normally.
        """
        ex = _make_executor()
        snapshot = _make_order_snapshot(order_id="ex-1")
        ex.exchange_client.get_orders = AsyncMock(return_value=[snapshot])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="ex-1")
        pending.accept_event_pending = True
        ex.pending_orders["cid-1"] = pending
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        ex._reconcile_fill_gap = AsyncMock()
        await ex._reconcile_with_exchange()
        assert "cid-1" in ex._unhealed_accept_events
        assert pending.accept_event_pending is True
        ex._reconcile_fill_gap.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_accept_event_heals_after_terminal_pop(self) -> None:
        """The retry survives the pending entry's terminal cleanup.

        Given: A queued accept event whose order already filled and
            whose pending entry is GONE (terminal pop before the write
            ever stuck),
        When: One reconciliation cycle runs,
        Then: The durable order_accepted event is still written and the
            queue entry removed — terminal cleanup cannot lose the
            durable acceptance record.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        await ex._reconcile_with_exchange()
        event = ex._record_venue_event.await_args.args[0]
        assert event["event_type"] == "order_accepted"
        assert "cid-1" not in ex._unhealed_accept_events

    @pytest.mark.asyncio
    async def test_unresolved_with_confirmed_unknown_does_not_republish(self) -> None:
        """An already-confirmed UNKNOWN publish is never repeated.

        Given: A parked unresolved entry whose UNKNOWN already
            confirmed,
        When: One reconciliation cycle runs,
        Then: No publish retry happens (the engine already holds).
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        pending.submit_ambiguous = True
        pending.unknown_published = True
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock(return_value=False)
        ex._publish_order_status = AsyncMock()
        await ex._reconcile_with_exchange()
        ex._publish_order_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_unknown_retouch_keeps_flag_unset(self) -> None:
        """A failed publish retry leaves the flag unset for next cycle.

        Given: A parked unresolved entry with an unconfirmed UNKNOWN
            and a publisher that fails again,
        When: One reconciliation cycle runs,
        Then: unknown_published stays False so the retry repeats.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        pending = _make_pending(exchange_order_id="")
        pending.exchange_order_id = None
        pending.submit_ambiguous = True
        ex.pending_orders["cid-1"] = pending
        ex._verify_ambiguous_submit = AsyncMock(return_value=False)
        ex._publish_order_status = AsyncMock(return_value=False)
        await ex._reconcile_with_exchange()
        assert pending.unknown_published is False

    @pytest.mark.asyncio
    async def test_retry_accept_event_with_unknown_key_is_noop(self) -> None:
        """A retry for an already-healed key is a harmless no-op.

        Given: No queued accept event for the client id (healed or
            removed concurrently),
        When: _retry_accept_event runs for that id,
        Then: Nothing is written and nothing raises.
        """
        ex = _make_executor()
        ex._record_venue_event = AsyncMock()
        await ex._retry_accept_event("ghost-cid")
        ex._record_venue_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_accept_event_heal_skips_insert_when_already_durable(self) -> None:
        """A committed-despite-reported-failure write heals without a duplicate.

        Given: A queued accept event whose original insert actually
            committed (timeout-after-commit race) so the durable probe
            finds the order_accepted row,
        When: _retry_accept_event runs,
        Then: No second insert happens, the queue entry is removed, and
            the pending flag clears.
        """
        ex = _make_executor()
        ex.repository = MagicMock(spec=SQLAlchemyRepository)
        ex.repository.has_venue_event = AsyncMock(return_value=True)
        pending = _make_pending(exchange_order_id="ex-1")
        pending.accept_event_pending = True
        ex.pending_orders["cid-1"] = pending
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        await ex._retry_accept_event("cid-1")
        ex._record_venue_event.assert_not_awaited()
        ex.repository.has_venue_event.assert_awaited_once_with("cid-1", "order_accepted")
        assert "cid-1" not in ex._unhealed_accept_events
        assert pending.accept_event_pending is False

    @pytest.mark.asyncio
    async def test_accept_event_heal_inserts_when_probe_negative(self) -> None:
        """A negative durable probe proceeds with the insert retry.

        Given: A queued accept event and a probe finding no durable row,
        When: _retry_accept_event runs and the insert succeeds,
        Then: The event is written and the queue entry removed.
        """
        ex = _make_executor()
        ex.repository = MagicMock(spec=SQLAlchemyRepository)
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        await ex._retry_accept_event("cid-1")
        ex._record_venue_event.assert_awaited_once()
        assert "cid-1" not in ex._unhealed_accept_events

    @pytest.mark.asyncio
    async def test_accept_event_heal_probe_failure_keeps_queue(self) -> None:
        """A failing durable probe defers the heal to the next cycle.

        Given: A queued accept event and a probe that raises (DB down),
        When: _retry_accept_event runs,
        Then: Nothing is written and the queue entry stays for retry.
        """
        ex = _make_executor()
        ex.repository = MagicMock(spec=SQLAlchemyRepository)
        ex.repository.has_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        await ex._retry_accept_event("cid-1")
        ex._record_venue_event.assert_not_awaited()
        assert "cid-1" in ex._unhealed_accept_events

    @pytest.mark.asyncio
    async def test_accept_event_heals_even_when_venue_is_down(self) -> None:
        """The DB-only heal runs before (and despite) a dead venue.

        Given: A queued accept event and get_orders raising (venue
            unreachable — exactly the outage that produced the failed
            write),
        When: One reconciliation cycle runs,
        Then: The durable order_accepted write still happens before the
            venue call aborts the rest of the cycle.
        """
        ex = _make_executor()
        ex.exchange_client.get_orders = AsyncMock(side_effect=RuntimeError("venue down"))
        ex._unhealed_accept_events["cid-1"] = self._accept_event()
        ex._record_venue_event = AsyncMock()
        with pytest.raises(RuntimeError, match="venue down"):
            await ex._reconcile_with_exchange()
        assert "cid-1" not in ex._unhealed_accept_events
        ex._record_venue_event.assert_awaited_once()


def _make_cmd_row(
    cid: str = "cid-1",
    *,
    public_id: str = "cmd-1",
    status: str = "dispatched",
    wallet: str = "wallet-1",
    created_at: datetime | None = None,
    shard_key: str = "kraken.BTC-USD.live",
) -> dict[str, Any]:
    """Build a TradeCommandRow-shaped dict for Phase E sweep tests."""
    now = created_at or datetime.now(UTC)
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 4,
        "command_type": "create",
        "shard_key": shard_key,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "strat-1",
        "client_order_id": cid,
        "venue_client_id": cid,
        "idempotency_key": None,
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.0,
        "price": 100.0,
        "leverage": None,
        "reduce_only": False,
        "status": status,
        "attempt_count": 1,
        "last_error": None,
        "created_at": now,
        "dispatched_at": now,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": "corr-1",
        "wallet_public_id": wallet,
        "operator_public_id": None,
        "user_public_id": None,
        "source_surface": None,
        "plan_public_id": None,
    }


def _make_sweep_executor() -> Any:
    """Build an executor wired for Phase E sweep tests."""
    ex = _make_executor()
    ex.wallet_public_id = "wallet-1"
    ex.settings.trade_command_dispatch_ttl_s = 30.0
    ex.repository = MagicMock(spec=SQLAlchemyRepository)
    ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
    ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[])
    ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(return_value=None)
    ex.repository.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
    ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=True)
    ex.exchange_client._log_order_to_db = AsyncMock(return_value=(7, "ord-pub-1"))
    ex._adopt_found_order = AsyncMock()
    ex._publish_order_status = AsyncMock(return_value=True)
    ex._record_venue_event = AsyncMock()
    return ex


class TestGhostOrderAdoption:
    """Reverse sweep over the open-orders snapshot (#145 Phase E §1c)."""

    @pytest.mark.asyncio
    async def test_own_ghost_order_is_adopted(self) -> None:
        """An open venue order with our wallet's command is adopted.

        Given: a snapshot order absent from pending_orders whose strict
            lookup returns this wallet's dispatched create command,
        When: the ghost sweep runs,
        Then: a pending entry is rebuilt FROM THE COMMAND ROW (tag from
            the shard key) and _adopt_found_order receives the snapshot.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row(shard_key="kraken.BTC-USD.paper.momo")
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=cmd)
        snapshot = _make_order_snapshot(order_id="ex-9")
        adopted: set[str] = set()
        await ex._adopt_ghost_orders([snapshot], adopted, set())
        assert adopted == {"cid-1"}
        assert "cid-1" in ex.pending_orders
        order = ex.pending_orders["cid-1"].request
        assert order.strategy_tag == "momo"
        assert order.signaled_at == cmd["created_at"]
        ex._adopt_found_order.assert_awaited_once()
        assert ex._adopt_found_order.await_args.args[2] is snapshot

    @pytest.mark.asyncio
    async def test_foreign_order_warned_once_never_touched(self) -> None:
        """An order with no command row is foreign — warn once, skip after.

        Given: a snapshot order whose strict lookup returns None,
        When: the ghost sweep runs twice,
        Then: no adoption happens and the second pass skips the lookup.
        """
        ex = _make_sweep_executor()
        snapshot = _make_order_snapshot()
        await ex._adopt_ghost_orders([snapshot], set(), set())
        await ex._adopt_ghost_orders([snapshot], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert ex.repository.get_active_create_command_by_client_order_id.await_count == 1
        assert "cid-1" in ex._ghost_foreign_warned

    @pytest.mark.asyncio
    async def test_other_wallet_command_is_skipped_silently(self) -> None:
        """Another wallet's command belongs to its own executor.

        Given: the strict lookup returning a command of wallet-2,
        When: the ghost sweep runs,
        Then: no adoption and the cid is NOT marked foreign (the owning
            executor adopts it).
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(wallet="wallet-2")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex._ghost_foreign_warned

    @pytest.mark.asyncio
    async def test_terminal_command_with_open_order_not_adopted(self) -> None:
        """A durably-terminal command never re-enters live accounting.

        Given: the strict lookup returning a CANCELLED command while the
            venue order is OPEN,
        When: the ghost sweep runs,
        Then: the order is left for the operator (loud warning, no adopt).
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(status="cancelled")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_adoption_cap_defers_excess(self) -> None:
        """At most five adoptions per cycle.

        Given: six own-wallet ghost orders,
        When: the ghost sweep runs,
        Then: five adopt and the sixth waits for the next cycle.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            side_effect=lambda cid, _ex: _make_cmd_row(cid, public_id=f"cmd-{cid}")
        )
        snapshots = [
            _make_order_snapshot(order_id=f"ex-{i}", client_order_id=f"cid-{i}") for i in range(6)
        ]
        adopted: set[str] = set()
        await ex._adopt_ghost_orders(snapshots, adopted, set())
        assert len(adopted) == 5

    @pytest.mark.asyncio
    async def test_lookup_failure_retries_next_cycle(self) -> None:
        """A failing strict lookup skips the order without state changes.

        Given: the strict lookup raising (fail-closed multi-match or DB),
        When: the ghost sweep runs,
        Then: nothing is adopted or marked and the next cycle retries.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            side_effect=RuntimeError("multiple active")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex._ghost_foreign_warned

    @pytest.mark.asyncio
    async def test_pending_adopted_and_idless_snapshots_skipped(self) -> None:
        """Pending entries, cycle-adopted cids and id-less orders skip.

        Given: a snapshot trio — one cid already pending, one already in
            the cycle's adopted set, one with no client id,
        When: the ghost sweep runs,
        Then: no lookups happen at all.
        """
        ex = _make_sweep_executor()
        ex.pending_orders["cid-pending"] = _make_pending()
        snapshots = [
            _make_order_snapshot(client_order_id="cid-pending"),
            _make_order_snapshot(client_order_id="cid-adopted"),
            _make_order_snapshot(client_order_id=None),
        ]
        await ex._adopt_ghost_orders(snapshots, {"cid-adopted"}, set())
        ex.repository.get_active_create_command_by_client_order_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_concurrent_pending_recheck_blocks_double_adopt(self) -> None:
        """The recheck right before adoption wins over a stale candidate.

        Given: a lookup whose await window races a pending-entry insert
            for the same cid (the in-flight order handler landed it),
        When: the ghost sweep runs,
        Then: no second adoption happens.
        """
        ex = _make_sweep_executor()

        async def _lookup_and_race(cid: str, _exchange: str) -> dict[str, Any]:
            ex.pending_orders[cid] = _make_pending()
            return _make_cmd_row(cid)

        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            side_effect=_lookup_and_race
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_plain_repository_disables_sweep(self) -> None:
        """A non-SQL repository (paper/test) disables the sweep cleanly.

        Given: an executor with a plain MagicMock repository,
        When: the ghost sweep runs,
        Then: nothing happens.
        """
        ex = _make_executor()
        ex._adopt_found_order = AsyncMock()
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()


class TestDispatchedVerification:
    """Durable-plane verification sweep (#145 Phase E §1d)."""

    @pytest.mark.asyncio
    async def test_found_command_is_adopted(self) -> None:
        """A venue-found unresolved command adopts via the command row.

        Given: one unresolved dispatched command and a venue lookup
            returning a snapshot,
        When: the verification sweep runs,
        Then: a pending entry is created and _adopt_found_order runs;
            the absence counter clears.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[cmd])
        snapshot = _make_order_snapshot(order_id="ex-7")
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=snapshot)
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        assert "cid-1" in ex.pending_orders
        ex._adopt_found_order.assert_awaited_once()
        assert "cid-1" not in ex._dispatched_absence_counts

    @pytest.mark.asyncio
    async def test_single_absence_only_counts(self) -> None:
        """One authoritative absence is not enough to reject.

        Given: a venue lookup returning None once,
        When: the verification sweep runs,
        Then: the absence counter is 1 and nothing publishes.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        await ex._verify_unresolved_dispatched(set())
        assert ex._dispatched_absence_counts["cid-1"] == 1
        ex._publish_order_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_double_absence_rejects_publish_first(self) -> None:
        """Two consecutive absences reject with publish-before-record.

        Given: a young command verified absent for the second time,
        When: the verification sweep runs,
        Then: REJECTED publishes, the order_rejected row records AFTER,
            and the counter clears.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        ex._publish_order_status.assert_awaited_once()
        event = ex._record_venue_event.await_args.args[0]
        assert event["event_type"] == "order_rejected"
        assert "cid-1" not in ex._dispatched_absence_counts

    @pytest.mark.asyncio
    async def test_failed_publish_writes_no_terminal_event(self) -> None:
        """A failed REJECTED publish never records a terminal row.

        Given: the second absence with a publisher returning False,
        When: the verification sweep runs,
        Then: NO venue event is written and the absence state survives
            so the next cycle retries the release (a premature terminal
            row would strand the engine guard forever).
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        ex._publish_order_status = AsyncMock(return_value=False)
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        ex._record_venue_event.assert_not_awaited()
        assert ex._dispatched_absence_counts["cid-1"] == 2

    @pytest.mark.asyncio
    async def test_old_command_gets_warn_only(self) -> None:
        """Absence stops being authoritative past the age bound.

        Given: a command older than an hour verified absent twice,
        When: the verification sweep runs,
        Then: no publish and no terminal event — operator escalation only.
        """
        ex = _make_sweep_executor()
        old_cmd = _make_cmd_row(created_at=datetime.now(UTC) - timedelta(seconds=7200))
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[old_cmd])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        ex._publish_order_status.assert_not_awaited()
        ex._record_venue_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_record_failure_keeps_state_for_retry(self) -> None:
        """A failed durable write after a confirmed publish stays retryable.

        Given: the second absence where the publish confirms but the
            order_rejected write raises,
        When: the verification sweep runs,
        Then: the counter survives so the sweep re-verifies (duplicate
            REJECTED publishes are engine-idempotent).
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        assert ex._dispatched_absence_counts["cid-1"] == 2

    @pytest.mark.asyncio
    async def test_unsupported_venue_logs_once_and_skips(self) -> None:
        """A venue without client-id lookup disables the sweep quietly.

        Given: find_order_by_client_id raising NotImplementedError,
        When: the verification sweep runs twice,
        Then: nothing publishes and the one-time flag is set.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(side_effect=NotImplementedError())
        await ex._verify_unresolved_dispatched(set())
        await ex._verify_unresolved_dispatched(set())
        assert ex._verify_unsupported_logged is True
        ex._publish_order_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lookup_error_leaves_counter_untouched(self) -> None:
        """A transient lookup failure neither counts nor resets absence.

        Given: a lookup raising a network error,
        When: the verification sweep runs,
        Then: the existing absence counter value is preserved.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            side_effect=RuntimeError("venue down")
        )
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        assert ex._dispatched_absence_counts["cid-1"] == 1

    @pytest.mark.asyncio
    async def test_fairness_cap_and_rotation(self) -> None:
        """At most three verifications per cycle with rotating start.

        Given: four unresolved commands and a venue answering absence,
        When: one verification sweep runs,
        Then: exactly three lookups happen and the rotation offset moves.
        """
        ex = _make_sweep_executor()
        cmds = [_make_cmd_row(f"cid-{i}", public_id=f"cmd-{i}") for i in range(4)]
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=cmds)
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        await ex._verify_unresolved_dispatched(set())
        assert ex.exchange_client.find_order_by_client_id.await_count == 3
        assert ex._dispatched_rotation_offset == 3

    @pytest.mark.asyncio
    async def test_resolved_cids_drop_stale_counters(self) -> None:
        """Counters of cids that left the candidate set are dropped.

        Given: an absence counter for a cid that no longer appears in
            the unresolved query (evidence landed elsewhere),
        When: the verification sweep runs,
        Then: the stale counter is removed.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[])
        ex._dispatched_absence_counts["cid-gone"] = 1
        await ex._verify_unresolved_dispatched(set())
        assert "cid-gone" not in ex._dispatched_absence_counts

    @pytest.mark.asyncio
    async def test_pending_and_adopted_cids_excluded(self) -> None:
        """In-memory-tracked and cycle-adopted cids are not re-verified.

        Given: unresolved rows whose cids are pending or just adopted,
        When: the verification sweep runs,
        Then: no venue lookups happen.
        """
        ex = _make_sweep_executor()
        ex.pending_orders["cid-pending"] = _make_pending()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(
            return_value=[
                _make_cmd_row("cid-pending"),
                _make_cmd_row("cid-adopted", public_id="cmd-2"),
            ]
        )
        await ex._verify_unresolved_dispatched({"cid-adopted"})
        ex.exchange_client.find_order_by_client_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_query_failure_retries_next_cycle(self) -> None:
        """A failing unresolved query aborts the sweep cleanly.

        Given: the repository query raising,
        When: the verification sweep runs,
        Then: no lookups happen and nothing raises.
        """
        ex = _make_sweep_executor()
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        await ex._verify_unresolved_dispatched(set())
        ex.exchange_client.find_order_by_client_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_found_with_concurrent_pending_recheck_skips(self) -> None:
        """The pre-adoption recheck blocks a racing pending insert.

        Given: a FOUND verification whose lookup await raced a pending
            insert for the same cid,
        When: the per-command verification runs,
        Then: no adoption happens.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row()

        async def _find_and_race(cid: str, _instrument: str) -> Any:
            ex.pending_orders[cid] = _make_pending()
            return _make_order_snapshot()

        ex.exchange_client.find_order_by_client_id = AsyncMock(side_effect=_find_and_race)
        await ex._verify_one_dispatched_command(cmd)
        ex._adopt_found_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_exchange_client_is_noop(self) -> None:
        """A client-less executor skips per-command verification.

        Given: exchange_client is None,
        When: the per-command verification runs directly,
        Then: nothing happens.
        """
        ex = _make_sweep_executor()
        ex.exchange_client = None
        await ex._verify_one_dispatched_command(_make_cmd_row())
        ex._publish_order_status.assert_not_awaited()


class TestGhostAdoptionHardening:
    """Round-2 review fixes: false-reject heal, stale snapshot, watermarks."""

    @pytest.mark.asyncio
    async def test_rejected_command_found_open_is_healed(self) -> None:
        """A falsely-rejected command found OPEN adopts and restores.

        Given: the strict lookup returning a REJECTED command while the
            venue shows the order OPEN (false absence rejection),
        When: the ghost sweep runs,
        Then: the order is adopted AND the durable row is CAS'd back to
            ACCEPTED with the venue order id.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row(status="rejected")
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=cmd)
        snapshot = _make_order_snapshot(order_id="ex-9")
        await ex._adopt_ghost_orders([snapshot], set(), set())
        ex._adopt_found_order.assert_awaited_once()
        kwargs = ex.repository.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["expected_status"] == "rejected"
        assert kwargs["new_status"] == "accepted"
        assert kwargs["exchange_order_id"] == "ex-9"
        assert kwargs["clear_terminal_at"] is True
        assert "cmd-1" not in ex._pending_rejected_restores

    @pytest.mark.asyncio
    async def test_non_rejected_terminal_still_refused(self) -> None:
        """Only REJECTED has the healing valve; others stay refused.

        Given: a CANCELLED command with an OPEN venue order,
        When: the ghost sweep runs,
        Then: no adoption and no restore CAS.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(status="cancelled")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pending_at_snapshot_cid_is_skipped(self) -> None:
        """A cid pending at snapshot time never adopts from that snapshot.

        Given: an order present in the (stale) snapshot whose pending
            entry was popped mid-cycle by a live fill,
        When: the ghost sweep runs with the snapshot-time pending set,
        Then: no lookup and no adoption — the next cycle's fresh
            snapshot decides.
        """
        ex = _make_sweep_executor()
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), {"cid-1"})
        ex.repository.get_active_create_command_by_client_order_id.assert_not_awaited()
        ex._adopt_found_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_adoption_seeds_watermarks_from_durable_fills(self) -> None:
        """Durable fill history seeds the adopted entry's watermarks.

        Given: a ghost order whose cid has a durable max-cum fill row,
        When: the ghost sweep adopts it,
        Then: both cumulative watermarks start at the durable maximum so
            the fill-gap pass cannot re-emit the whole history.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(
            return_value={"cum_fill_size": 0.6}
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        pending = ex.pending_orders["cid-1"]
        assert pending.last_seen_cum_qty == 0.6
        assert pending.last_recorded_cum_qty == 0.6

    @pytest.mark.asyncio
    async def test_watermark_seed_failure_defers_adoption(self) -> None:
        """A failed seed read defers the adoption — FAIL-CLOSED.

        Given: the max-cum read raising,
        When: the ghost sweep runs,
        Then: NO adoption happens this cycle (an untrusted zero
            watermark could double-book already-published fills) and
            the next cycle retries.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        assert "cid-1" not in ex.pending_orders
        ex._adopt_found_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatched_found_seed_failure_defers_adoption(self) -> None:
        """The dispatched FOUND path is equally fail-closed on seeding.

        Given: a venue-found unresolved command whose max-cum read raises,
        When: the per-command verification runs,
        Then: no adoption and the absence counter survives untouched.
        """
        ex = _make_sweep_executor()
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=_make_order_snapshot())
        ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_one_dispatched_command(_make_cmd_row())
        assert "cid-1" not in ex.pending_orders
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex._dispatched_absence_counts

    @pytest.mark.asyncio
    async def test_found_with_seed_failure_resets_absence_streak(self) -> None:
        """A live observation breaks the absence streak even on seed failure.

        Given: an absence count of 1, then a FOUND verification whose
            watermark seed fails (adoption deferred), then a fresh
            absence,
        When: the verifications run in sequence,
        Then: the fresh absence counts as 1 — never 2 — so no REJECT can
            fire right after the order was observed live.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row()
        ex._dispatched_absence_counts["cid-1"] = 1
        ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=_make_order_snapshot())
        await ex._verify_one_dispatched_command(cmd)
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        await ex._verify_one_dispatched_command(cmd)
        assert ex._dispatched_absence_counts["cid-1"] == 1
        ex._publish_order_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_failure_queues_and_recon_retries(self) -> None:
        """A failed restore CAS survives in the retry queue.

        Given: a rejected-found-open adoption whose restore CAS raises
            inline,
        When: the next recon cycle's DB-only sweep runs with a healthy
            repository,
        Then: the restore is retried and the queue entry clears.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row(status="rejected")
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=cmd)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot(order_id="ex-9")], set(), set())
        assert "cmd-1" in ex._pending_rejected_restores
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=True)
        await ex._retry_rejected_restore("cmd-1")
        assert "cmd-1" not in ex._pending_rejected_restores
        kwargs = ex.repository.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["exchange_order_id"] == "ex-9"
        assert kwargs["clear_terminal_at"] is True

    @pytest.mark.asyncio
    async def test_restore_lost_cas_still_rejected_keeps_queue(self) -> None:
        """A lost CAS with the row STILL rejected keeps retrying.

        Given: a queued restore whose CAS returns False while the
            current status re-reads as rejected,
        When: the retry runs,
        Then: the queue entry survives for the next cycle.
        """
        ex = _make_sweep_executor()
        ex._pending_rejected_restores["cmd-1"] = _make_cmd_row(status="rejected")
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="rejected")
        await ex._retry_rejected_restore("cmd-1")
        assert "cmd-1" in ex._pending_rejected_restores

    @pytest.mark.asyncio
    async def test_restore_lost_cas_row_moved_clears_queue(self) -> None:
        """A lost CAS whose row moved on is verified done.

        Given: a queued restore whose CAS returns False and the current
            status re-reads as accepted (another writer fixed it),
        When: the retry runs,
        Then: the queue entry clears.
        """
        ex = _make_sweep_executor()
        ex._pending_rejected_restores["cmd-1"] = _make_cmd_row(status="rejected")
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="accepted")
        await ex._retry_rejected_restore("cmd-1")
        assert "cmd-1" not in ex._pending_rejected_restores

    @pytest.mark.asyncio
    async def test_restore_retry_unknown_key_is_noop(self) -> None:
        """A retry for an already-cleared restore key is harmless.

        Given: no queued restore for the public id,
        When: the retry runs,
        Then: nothing is written.
        """
        ex = _make_sweep_executor()
        await ex._retry_rejected_restore("ghost-cmd")
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_foreign_warned_set_is_lru_bounded(self) -> None:
        """The warned-foreign cid set cannot grow unbounded.

        Given: more foreign cids than the LRU bound,
        When: each is marked warned,
        Then: the set size stays at the bound and the oldest fall out.
        """
        ex = _make_sweep_executor()
        for i in range(600):
            ex._mark_foreign_order_warned(f"cid-{i}")
        assert len(ex._ghost_foreign_warned) == 512
        assert "cid-0" not in ex._ghost_foreign_warned
        assert "cid-599" in ex._ghost_foreign_warned


class TestSweepTtlGating:
    """The verification sweep derives its bounds from the dispatch TTL."""

    @pytest.mark.asyncio
    async def test_disabled_ttl_blocks_absence_reject(self) -> None:
        """With the TTL disabled, absence may never auto-REJECT.

        Given: trade_command_dispatch_ttl_s = 0 (frames never expire) and
            a command verified absent for the second time,
        When: the verification sweep runs,
        Then: WARN-only — no publish, no terminal event (a late frame
            could still legally arrive and submit).
        """
        ex = _make_sweep_executor()
        ex.settings.trade_command_dispatch_ttl_s = 0.0
        ex.repository.get_unresolved_dispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        ex._dispatched_absence_counts["cid-1"] = 1
        await ex._verify_unresolved_dispatched(set())
        ex._publish_order_status.assert_not_awaited()
        ex._record_venue_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cutoff_scales_with_configured_ttl(self) -> None:
        """A TTL above the floor pushes the sweep cutoff out to 2x TTL.

        Given: trade_command_dispatch_ttl_s = 120,
        When: the verification sweep queries its candidates,
        Then: the cutoff is at least 240 seconds in the past — a frame
            still legitimately deliverable under the TTL can never be
            absence-verified.
        """
        ex = _make_sweep_executor()
        ex.settings.trade_command_dispatch_ttl_s = 120.0
        await ex._verify_unresolved_dispatched(set())
        cutoff = ex.repository.get_unresolved_dispatched_commands.await_args.args[2]
        assert (datetime.now(UTC) - cutoff).total_seconds() >= 239.0


class TestAdoptedOrderRowRepair:
    """Adoption repairs the durable orders row when it is missing."""

    @pytest.mark.asyncio
    async def test_missing_row_is_repaired_on_adoption(self) -> None:
        """A ghost adoption without an active orders row writes one.

        Given: no active orders row for the adopted cid,
        When: the ghost sweep adopts,
        Then: _log_order_to_db runs and the pending entry carries the
            repaired db ids (restart recovery re-arms from that row).
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex.exchange_client._log_order_to_db.assert_awaited_once()
        pending = ex.pending_orders["cid-1"]
        assert pending.db_order_id == 7
        assert pending.order_public_id == "ord-pub-1"

    @pytest.mark.asyncio
    async def test_existing_row_is_not_duplicated(self) -> None:
        """An already-present orders row is left alone.

        Given: an active orders row exists for the cid,
        When: the ghost sweep adopts,
        Then: no second insert happens.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex.exchange_client._log_order_to_db.assert_not_awaited()
        ex._adopt_found_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_repair_failure_never_blocks_adoption(self) -> None:
        """A failing row repair is best-effort — adoption proceeds.

        Given: the existence probe raising,
        When: the ghost sweep adopts,
        Then: the adoption completes with no db ids.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_exchange_order_id_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_awaited_once()
        assert ex.pending_orders["cid-1"].db_order_id is None


class TestReconstructionFailureIsolation:
    """A malformed command row cannot wedge the recon cycle."""

    @pytest.mark.asyncio
    async def test_ghost_adoption_skips_unreconstructable_command(self) -> None:
        """A vocabulary-mismatched row skips adoption, not the cycle.

        Given: a command row whose order_type is an exchange-wire value
            the dispatch schema rejects (the pre-existing stop-order
            pipeline bug),
        When: the ghost sweep runs,
        Then: the order is skipped with an ERROR log and no exception
            escapes to abort the rest of the cycle.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(public_id="cmd-stop")
        )
        ex.repository.get_max_cumulative_fill_venue_event = AsyncMock(return_value=None)
        bad = _make_cmd_row(public_id="cmd-stop")
        bad["order_type"] = "stop-loss"
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=bad)
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_dispatched_verification_skips_unreconstructable_command(self) -> None:
        """The verification sweep is equally isolated per command.

        Given: an unresolved dispatched row with a vocabulary-mismatched
            order_type and a venue answering FOUND,
        When: the per-command verification runs,
        Then: it returns without adopting and without raising.
        """
        ex = _make_sweep_executor()
        bad = _make_cmd_row(public_id="cmd-stop")
        bad["order_type"] = "stop-loss"
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=_make_order_snapshot())
        await ex._verify_one_dispatched_command(bad)
        ex._adopt_found_order.assert_not_awaited()


class TestPhaseECoverageEdges:
    """Edge branches of the E2 helpers."""

    @pytest.mark.asyncio
    async def test_recon_cycle_drives_queued_restores(self) -> None:
        """The recon cycle replays the rejected-restore queue.

        Given: a queued restore and a healthy repository,
        When: one reconciliation cycle runs,
        Then: the restore CAS is attempted before venue work.
        """
        ex = _make_sweep_executor()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        ex._pending_rejected_restores["cmd-1"] = _make_cmd_row(status="rejected")
        await ex._reconcile_with_exchange()
        ex.repository.advance_trade_command_lifecycle.assert_awaited_once()
        assert "cmd-1" not in ex._pending_rejected_restores

    @pytest.mark.asyncio
    async def test_row_repair_without_exchange_client_is_noop(self) -> None:
        """Row repair needs a venue client for the log seam.

        Given: an executor whose exchange_client is None,
        When: _repair_adopted_order_row runs directly,
        Then: nothing is probed or written.
        """
        ex = _make_sweep_executor()
        cmd = _make_cmd_row()
        order = order_request_from_command(cmd)
        pending = PendingOrderState(request=order)
        ex.exchange_client = None
        await ex._repair_adopted_order_row(order, _make_order_snapshot(), pending)
        ex.repository.get_exchange_order_id_for_client_order_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_row_repair_logging_miss_leaves_ids_unset(self) -> None:
        """A None from the order-log seam keeps the entry id-less.

        Given: _log_order_to_db returning None (repository off or write
            failed inside the never-raises seam),
        When: the ghost sweep adopts,
        Then: adoption proceeds with db ids unset.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.exchange_client._log_order_to_db = AsyncMock(return_value=None)
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        assert ex.pending_orders["cid-1"].db_order_id is None
        ex._adopt_found_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_watermark_seed_with_plain_repository_is_trusted(self) -> None:
        """A non-SQL repository trusts the zero watermarks (paper/tests).

        Given: an executor with a plain MagicMock repository,
        When: _seed_adoption_watermarks runs directly,
        Then: it returns True without any read.
        """
        ex = _make_executor()
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        assert await ex._seed_adoption_watermarks(pending, "cid-1") is True
