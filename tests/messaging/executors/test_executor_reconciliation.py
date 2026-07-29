"""Tests for venue reconciliation in executor."""

import asyncio
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
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderEventEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderFillSummary
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.errors import CircuitBreakerOpenError
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import _LIVE_TRADING_UNAVAILABLE
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
    ex.exchange_client.get_order_fill_summary = AsyncMock(return_value=None)
    ex.exchange_client.supports_fill_summary = False
    ex.repository = MagicMock()
    return ex


def _enable_live_trading(executor: Any) -> None:
    """Wire the live-trading interlock to report ``enabled``.

    The interlock reads ``live_trading_mode`` fresh per non-paper submit
    and fails closed to ``halted`` when no settings service is wired, so
    submit-path tests that must reach the venue call point the settings
    service at an ``enabled`` fresh read.
    """
    executor._settings_service = SimpleNamespace(
        get_setting_fresh=AsyncMock(return_value="enabled")
    )


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
        pending = _make_pending(cum_qty=0.0)
        ex.pending_orders["cid-1"] = pending
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        filled_snap = _make_order_snapshot(
            filled=1.0, price=100.0, status=ExchangeOrderStatusEnum.CLOSED
        )
        ex.exchange_client.get_order = AsyncMock(return_value=filled_snap)
        ex.exchange_client.get_balance = AsyncMock(return_value={})

        async def _commit_fill(execution: Any) -> None:
            if execution.cum_qty is not None:
                pending.last_seen_cum_qty = execution.cum_qty

        ex._process_execution = AsyncMock(side_effect=_commit_fill)

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
        ex.exchange_client.get_order_fill_summary = AsyncMock(return_value=None)
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
        ex.exchange_client.get_order_fill_summary = AsyncMock(return_value=None)
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
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(vwap=101.5, covered_qty=5.0)
        )
        ex._process_execution = AsyncMock()

        await ex._reconcile_with_exchange()

        ex.exchange_client.get_order_fill_summary.assert_awaited_once_with("ex-1")
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
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(vwap=101.5, covered_qty=3.0)
        )
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
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(vwap=101.5, covered_qty=4.9999999)
        )
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
        ex.exchange_client.get_order_fill_summary = AsyncMock(side_effect=RuntimeError("boom"))
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
        ex._verify_ambiguous_submit.assert_awaited_once_with(pending.request, pending, None)

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
    ex.repository.get_fill_venue_events_for_order = AsyncMock(return_value=[])
    ex.repository.get_executions_for_order = AsyncMock(return_value=[])
    ex.repository.get_order_identity_for_client_order_id = AsyncMock(return_value=None)
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
    async def test_missing_client_order_id_candidate_raises(self) -> None:
        """A ghost-adoption candidate must carry its client order id.

        Given: A snapshot that reaches adoption despite lacking client_order_id,
        When: The ghost sweep processes it,
        Then: A RuntimeError replaces the prior assert-only invariant.
        """
        ex = _make_sweep_executor()
        ex._should_skip_ghost_snapshot = MagicMock(return_value=False)
        snapshot = _make_order_snapshot(client_order_id=None)
        with pytest.raises(RuntimeError, match="client_order_id"):
            await ex._adopt_ghost_orders([snapshot], set(), set())
        ex.repository.get_active_create_command_by_client_order_id.assert_not_awaited()

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
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
            return_value=[
                {"cum_fill_size": 0.4, "fee": 0.01, "exec_id": "e-1", "fee_asset": "EUR"},
                {"cum_fill_size": 0.6, "fee": 0.02, "exec_id": "e-2", "fee_asset": "EUR"},
            ]
        )
        ex.repository.get_executions_for_order = AsyncMock(
            return_value=[
                {"size": 0.4, "fee": 0.01, "fee_asset": "EUR"},
                {"size": 0.2, "fee": 0.02, "fee_asset": "EUR"},
            ]
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        pending = ex.pending_orders["cid-1"]
        assert pending.last_seen_cum_qty == pytest.approx(0.6)
        assert pending.last_recorded_cum_qty == 0.6
        assert pending.last_recorded_fee == {"EUR": pytest.approx(0.03)}
        assert pending.last_published_fee == {"EUR": pytest.approx(0.03)}

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
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
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
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
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
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
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
    async def test_dispatched_history_witness_preserves_requested_size(self) -> None:
        """A fill-derived history witness cannot shrink the durable order.

        Given: a dispatched command requesting 100 units and a restart
            lookup returning a history witness that observed only 10,
        When: unresolved-DISPATCHED recovery repairs the missing order
            row,
        Then: the durable insert receives the requested size of 100,
            while the history witness itself remains a 10-unit
            observation.
        """
        ex = _make_sweep_executor()
        command = _make_cmd_row()
        command["quantity"] = 100.0
        history_witness = _make_order_snapshot(filled=10.0)
        history_witness.amount = 10.0
        history_witness.amount_decimal = "10"
        history_witness.amount_is_order_size = False
        durable_sizes: list[float] = []

        async def _persist_repaired_order(
            _request: object, snapshot: ExchangeOrderSnapshot
        ) -> tuple[int, str]:
            durable_sizes.append(snapshot.amount)
            return 7, "ord-pub-1"

        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=history_witness)
        ex.exchange_client._log_order_to_db = AsyncMock(side_effect=_persist_repaired_order)
        await ex._verify_one_dispatched_command(command)
        assert durable_sizes == [100.0]
        assert history_witness.amount == 10.0

    @pytest.mark.asyncio
    async def test_dispatched_venue_snapshot_keeps_authoritative_size(self) -> None:
        """A genuine venue snapshot remains authoritative during repair.

        Given: a dispatched command and a venue snapshot with its true
            order size,
        When: unresolved-DISPATCHED recovery repairs the missing row,
        Then: the snapshot reaches persistence unchanged.
        """
        ex = _make_sweep_executor()
        snapshot = _make_order_snapshot()
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=snapshot)
        await ex._verify_one_dispatched_command(_make_cmd_row())
        persisted = ex.exchange_client._log_order_to_db.await_args.args[1]
        assert persisted is snapshot
        assert persisted.amount == 1.0

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
        ex.repository.get_order_identity_for_client_order_id = AsyncMock(
            return_value=(5, "ord-pub-9", "ex-1")
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex.exchange_client._log_order_to_db.assert_not_awaited()
        pending = ex.pending_orders["cid-1"]
        assert pending.db_order_id == 5
        assert pending.order_public_id == "ord-pub-9"
        ex._adopt_found_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_repair_failure_defers_adoption(self) -> None:
        """A failed row repair defers the adoption — FAIL-CLOSED.

        Given: the identity probe raising (no order_public_id available
            for the dual-plane executions read),
        When: the ghost sweep runs,
        Then: no adoption happens this cycle and the next cycle retries.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_order_identity_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        ex.exchange_client._log_order_to_db = AsyncMock(return_value=None)
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        ex._adopt_found_order.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders


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
        ex.repository.get_fill_venue_events_for_order = AsyncMock(return_value=[])
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
    async def test_row_repair_logging_miss_defers_adoption(self) -> None:
        """A None from the order-log seam defers the adoption.

        Given: _log_order_to_db returning None (write failed inside the
            never-raises seam) with no existing row identity,
        When: the ghost sweep runs,
        Then: no adoption — without an order_public_id the dual-plane
            seeding cannot trust the executions plane.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.exchange_client._log_order_to_db = AsyncMock(return_value=None)
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        assert "cid-1" not in ex.pending_orders
        ex._adopt_found_order.assert_not_awaited()

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


class TestCorrectiveFeeFidelity:
    """Venue-true fees and timestamps on corrective fills (#145 P2-5)."""

    def _snapshot_with_fee(
        self, fee: float | None, fee_currency: str | None, price: float | None = 100.0
    ) -> ExchangeOrderSnapshot:
        """Build a half-filled snapshot carrying order-level commission."""
        return ExchangeOrderSnapshot(
            id="ex-1",
            client_order_id="cid-1",
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=price,
            status=ExchangeOrderStatusEnum.OPEN,
            filled=0.5,
            remaining=0.5,
            timestamp=1750000000.0,
            fee=fee,
            fee_currency=fee_currency,
        )

    @pytest.mark.asyncio
    async def test_snapshot_commission_rides_cum_fee(self) -> None:
        """An order-level commission passes through as cumulative fee.

        Given: a fill gap on a snapshot carrying fee 0.2 EUR,
        When: the corrective is built,
        Then: it carries cum_fee/cum_fee_currency (the watermark
            attributes the unattributed remainder) and the snapshot's
            venue timestamp — never a fabricated zero fee.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        pending = _make_pending()
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, self._snapshot_with_fee(0.2, "EUR"))
        corrective = ex._process_execution.await_args.args[0]
        assert corrective.cum_fee == 0.2
        assert corrective.cum_fee_currency == "EUR"
        assert corrective.fees is None
        assert corrective.timestamp == datetime.fromtimestamp(1750000000.0, tz=UTC)

    @pytest.mark.asyncio
    async def test_summary_fee_total_rides_cum_fee(self) -> None:
        """Fills-summary totals pass through as the CUMULATIVE fee.

        Given: a priceless snapshot whose fills summary covers the whole
            filled quantity with fee_total 0.6 USD,
        When: the corrective is built,
        Then: cum_fee carries the cumulative total — the dual fee
            watermark attributes only the unattributed remainder, so
            successive gap corrections can never overlap fee fractions
            and per-fill live fees already charged are never re-charged.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(
                vwap=101.0, covered_qty=0.5, fee_total=0.6, fee_currency="USD"
            )
        )
        pending = _make_pending(cum_qty=0.25)
        await ex._reconcile_fill_gap(
            "kraken", "ex-1", pending, self._snapshot_with_fee(None, None, price=None)
        )
        corrective = ex._process_execution.await_args.args[0]
        assert corrective.fees is None
        assert corrective.cum_fee == pytest.approx(0.6)
        assert corrective.cum_fee_currency == "USD"

    @pytest.mark.asyncio
    async def test_no_fee_source_stays_feeless(self) -> None:
        """Without any venue fee source the corrective stays honest-empty.

        Given: a priced snapshot with no commission data and no summary,
        When: the corrective is built,
        Then: cum_fee and fees are both None.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        pending = _make_pending()
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, self._snapshot_with_fee(None, None))
        corrective = ex._process_execution.await_args.args[0]
        assert corrective.cum_fee is None
        assert corrective.fees is None

    def test_corrective_fees_prefer_snapshot_over_summary(self) -> None:
        """The snapshot's own commission outranks the fills summary.

        Given: a snapshot carrying fee 0.2 EUR and a summary with a USD
            total,
        When: _corrective_fees runs directly,
        Then: the snapshot's cumulative wins.
        """
        result = ExchangeExecutorService._corrective_fees(
            self._snapshot_with_fee(0.2, "EUR"),
            OrderFillSummary(vwap=1.0, covered_qty=0.5, fee_total=0.1, fee_currency="USD"),
        )
        assert result == (0.2, "EUR")


class TestFeeWatermark:
    """Cumulative-commission deltas via the executor fee watermark."""

    def _execution(self, cum_fee: float | None, cum_qty: float = 0.5) -> ExecutionUpdate:
        """Build a cum-carrying execution with optional cumulative fee.

        Carries a positive ``average_price`` so the durable fill row is
        economically sound — the writer bulwark (S5.4 P0-4) aborts a real
        fill with a zero/non-finite price, which these fee-watermark
        tests do not intend to exercise.
        """
        return ExecutionUpdate(
            order_id="ex-1",
            exec_type="trade",
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.PARTIALLY_FILLED,
            timestamp=datetime.now(UTC),
            cum_qty=cum_qty,
            average_price=100.0,
            exec_id=f"wal-ex-1-c{int(cum_qty * 1e8)}",
            cum_fee=cum_fee,
            cum_fee_currency="EUR" if cum_fee is not None else None,
        )

    @pytest.mark.asyncio
    async def test_tracked_fee_delta_anchors_on_watermark(self) -> None:
        """A tracked order's fee is the delta from the recorded watermark.

        Given: a pending entry with last_recorded_fee 0.02 and an
            execution carrying cum_fee 0.05,
        When: the execution data is built,
        Then: the published fee is 0.03 in the cumulative currency.
        """
        ex = _make_sweep_executor()
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        pending.exchange_order_id = "ex-1"
        pending.last_published_fee = {"EUR": 0.02}
        ex.pending_orders["cid-1"] = pending
        _topic, fill = ex._build_execution_data(
            self._execution(0.05), "ex-1", pending.request, "kraken"
        )
        assert fill.fee == pytest.approx(0.03)
        assert fill.fee_asset == "EUR"

    @pytest.mark.asyncio
    async def test_regressed_cumulative_fee_publishes_signed_rebate(self) -> None:
        """A cumulative below the watermark publishes a SIGNED rebate.

        Given: a published EUR anchor of 0.05 and an execution whose
            venue cumulative dropped to 0.02 (maker rebate adjustment),
        When: the execution data is built,
        Then: the fee is -0.03 — a nonnegative clamp would silently
            swallow rebates.
        """
        ex = _make_sweep_executor()
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        pending.exchange_order_id = "ex-1"
        pending.last_published_fee = {"EUR": 0.05}
        ex.pending_orders["cid-1"] = pending
        _topic, fill = ex._build_execution_data(
            self._execution(0.02), "ex-1", pending.request, "kraken"
        )
        assert fill.fee == pytest.approx(-0.03)

    @pytest.mark.asyncio
    async def test_untracked_cum_fee_attributes_fully(self) -> None:
        """An untracked order has no watermark — the full cum_fee applies.

        Given: no pending entry for the order,
        When: the execution data is built with cum_fee 0.05,
        Then: the fee is 0.05 (best-effort full attribution).
        """
        ex = _make_sweep_executor()
        order = order_request_from_command(_make_cmd_row())
        _topic, fill = ex._build_execution_data(self._execution(0.05), "ex-1", order, "kraken")
        assert fill.fee == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_book_advances_fee_watermark_after_record(self) -> None:
        """The durable write advances the fee watermark.

        Given: a tracked pending entry and a cum-fee execution whose
            durable write and publish succeed,
        When: the fill is booked,
        Then: last_recorded_fee equals the cumulative fee.
        """
        ex = _make_sweep_executor()
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        pending.exchange_order_id = "ex-1"
        ex.pending_orders["cid-1"] = pending
        ex._record_venue_event = AsyncMock()
        ex._publish_execution = AsyncMock(return_value=True)
        order = pending.request
        await ex._book_correlated_fill(self._execution(0.05), "ex-1", "cid-1", order, "kraken")
        assert pending.last_recorded_fee == {"EUR": pytest.approx(0.05)}
        assert pending.last_published_fee == {"EUR": pytest.approx(0.05)}
        event = ex._record_venue_event.await_args.args[0]
        assert event["fee"] == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_publish_failure_keeps_published_fee_anchor(self) -> None:
        """A failed publish must not advance the PUBLISHED fee watermark.

        Given: a first cum-fee frame whose durable write succeeds but
            whose publish fails, then a second frame with a higher
            cumulative,
        When: both are booked,
        Then: the second frame's PUBLISHED fee absorbs the unpublished
            slice (anchored on last_published_fee), while its DURABLE
            row fee carries only the durable delta.
        """
        ex = _make_sweep_executor()
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        pending.exchange_order_id = "ex-1"
        ex.pending_orders["cid-1"] = pending
        ex._record_venue_event = AsyncMock()
        ex._publish_execution = AsyncMock(return_value=False)
        order = pending.request
        await ex._book_correlated_fill(
            self._execution(0.05, cum_qty=0.5), "ex-1", "cid-1", order, "kraken"
        )
        assert pending.last_recorded_fee == {"EUR": pytest.approx(0.05)}
        assert pending.last_published_fee == {}
        ex._publish_execution = AsyncMock(return_value=True)
        await ex._book_correlated_fill(
            self._execution(0.10, cum_qty=1.0), "ex-1", "cid-1", order, "kraken"
        )
        published_fill = ex._publish_execution.await_args.args[1]
        assert published_fill.fee == pytest.approx(0.10)
        second_event = ex._record_venue_event.await_args.args[0]
        assert second_event["fee"] == pytest.approx(0.05)
        assert pending.last_published_fee == {"EUR": pytest.approx(0.10)}


class TestBreakerOpenDisposition:
    """Distinct, redispatch-safe handling of breaker-refused submits."""

    def _order_executor(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build a sweep executor whose submit raises breaker-open."""
        ex = _make_sweep_executor()
        _enable_live_trading(ex)
        ex.repository.has_order_submit_evidence = AsyncMock(return_value=False)
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="failed")
        ex._execute_live_order = AsyncMock(side_effect=CircuitBreakerOpenError("open"))
        ex.settings.trade_command_dispatch_ttl_s = 0.0
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        return ex

    @pytest.mark.asyncio
    async def test_happy_path_records_fails_and_publishes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The full disposition runs record -> CAS FAILED -> REJECTED.

        Given: a submit refused by the open breaker,
        When: _process_order runs,
        Then: an order_breaker_open event records with status failed, the
            command CAS-es to FAILED, REJECTED publishes with the
            circuit_breaker_open reason, and the pending entry pops.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        events = [c.args[0] for c in ex._record_venue_event.await_args_list]
        breaker_events = [e for e in events if e["event_type"] == "order_breaker_open"]
        assert len(breaker_events) == 1
        assert breaker_events[0]["status"] == "failed"
        cas_kwargs = ex.repository.advance_trade_command_lifecycle.await_args_list[0].kwargs
        assert cas_kwargs["public_id"] == "cmd-1"
        assert cas_kwargs["new_status"] == "failed"
        assert cas_kwargs["last_error"] == "circuit_breaker_open"
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls[0].kwargs["reason"] == "circuit_breaker_open"
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_event_write_failure_parks_without_publish(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed durable event write parks the entry — intent stays held.

        Given: the order_breaker_open write raising,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks with
            breaker_open_pending.
        """
        ex = self._order_executor(monkeypatch)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls == []
        assert ex.pending_orders["cid-1"].breaker_open_pending is True

    @pytest.mark.asyncio
    async def test_lost_cas_with_live_row_parks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-terminal row after all CAS attempts parks the entry.

        Given: every FAILED CAS losing while the row reads dispatched,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="dispatched")
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls == []
        assert ex.pending_orders["cid-1"].breaker_open_pending is True

    @pytest.mark.asyncio
    async def test_already_terminal_row_counts_as_done(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A row already FAILED (earlier attempt / fold) completes the CAS step.

        Given: all CAS attempts losing while the current status reads failed,
        When: _process_order runs,
        Then: the disposition completes and the entry pops.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_terminal_row_short_circuits_cas(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A row the fold already terminalized needs no CAS attempts.

        Given: the strict lookup returning a FAILED command row,
        When: _process_order hits breaker-open,
        Then: no CAS is attempted and the disposition completes.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(status="failed")
        )
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_missing_command_row_counts_as_done(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No durable command row means nothing to terminalize.

        Given: the strict lookup returning None (manual/paper flow),
        When: _process_order hits breaker-open,
        Then: the disposition completes without any CAS.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_publish_failure_parks_for_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failed REJECTED publish parks the entry for the recon retry.

        Given: a publisher returning False for the REJECTED,
        When: _process_order runs,
        Then: the entry parks with breaker_open_pending.
        """
        ex = self._order_executor(monkeypatch)

        async def _publish(order: Any, status: str, *args: Any, **kwargs: Any) -> bool:
            return status != OrderEventEnum.REJECTED

        ex._publish_order_status = AsyncMock(side_effect=_publish)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert ex.pending_orders["cid-1"].breaker_open_pending is True

    @pytest.mark.asyncio
    async def test_recon_retry_heals_parked_disposition(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The recon sweep reruns the disposition probe-guarded.

        Given: a parked breaker_open_pending entry whose event already
            committed (probe True),
        When: the retry runs,
        Then: no duplicate event writes, the CAS and publish complete,
            and the entry pops.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.breaker_open_pending = True
        ex.pending_orders["cid-1"] = pending
        ex.repository.has_venue_event = AsyncMock(return_value=True)
        await ex._retry_breaker_open("cid-1")
        ex._record_venue_event.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_retry_skips_unflagged_entries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The retry only touches parked breaker entries.

        Given: a normal pending entry without the flag,
        When: the retry runs,
        Then: nothing happens.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        ex.pending_orders["cid-1"] = PendingOrderState(request=order)
        await ex._retry_breaker_open("cid-1")
        assert "cid-1" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_plain_repository_skips_cas(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without a SQL repository the CAS step is a pass-through.

        Given: a paper/test executor with a plain MagicMock repository,
        When: _process_order hits breaker-open,
        Then: the disposition still completes (publish + pop).
        """
        ex = self._order_executor(monkeypatch)
        ex.repository = MagicMock()
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_cas_error_parks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A DB error during the FAILED CAS parks the entry.

        Given: advance_trade_command_lifecycle raising,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert ex.pending_orders["cid-1"].breaker_open_pending is True


class TestBreakerOpenEdges:
    """Residual coverage edges of the breaker disposition and seeding."""

    @pytest.mark.asyncio
    async def test_incomplete_disposition_without_pending_parks_fresh_entry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed disposition with no pending entry parks a FRESH one.

        Given: a breaker-open resume invoked while no pending entry
            exists (the dup-guard replay path after an executor crash)
            and the disposition failing,
        When: _handle_breaker_open_submit runs directly,
        Then: a pending entry is created and parked — the command row
            may already be terminal FAILED, so without a retry vehicle
            the REJECTED publish would never happen and the engine
            intent would wait on its timeout valve.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        await ex._handle_breaker_open_submit(order)
        assert ex.pending_orders["cid-1"].breaker_open_pending is True

    @pytest.mark.asyncio
    async def test_retry_keeps_parked_entry_on_repeat_failure(self) -> None:
        """A still-failing retry leaves the entry parked.

        Given: a parked breaker entry whose event write keeps raising,
        When: the retry runs,
        Then: the entry stays parked with the flag set.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.breaker_open_pending = True
        ex.pending_orders["cid-1"] = pending
        await ex._retry_breaker_open("cid-1")
        assert ex.pending_orders["cid-1"].breaker_open_pending is True

    @pytest.mark.asyncio
    async def test_recon_cycle_drives_parked_breaker_retries(self) -> None:
        """The recon cycle replays parked breaker dispositions.

        Given: a parked breaker entry whose durable event already
            committed and a healthy repository,
        When: one reconciliation cycle runs,
        Then: the disposition completes and the entry pops.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=True)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="failed")
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.breaker_open_pending = True
        ex.pending_orders["cid-1"] = pending
        await ex._reconcile_with_exchange()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_watermark_seed_ignores_cum_less_rows(self) -> None:
        """Seed rows without a cumulative neither break nor contribute.

        Given: durable fill rows mixing a cum-less row and a redelivered
            duplicate exec id between cum rows,
        When: the ghost sweep adopts,
        Then: the max cum seeds correctly and the fee sum counts the
            duplicated exec id exactly once.
        """
        ex = _make_sweep_executor()
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
            return_value=[
                {"cum_fill_size": 0.6, "fee": 0.02, "exec_id": "e-1", "fee_asset": "EUR"},
                {"cum_fill_size": None, "fee": None, "exec_id": None, "fee_asset": None},
                {"cum_fill_size": 0.4, "fee": 0.01, "exec_id": "e-2", "fee_asset": "EUR"},
                {"cum_fill_size": 0.6, "fee": 0.02, "exec_id": "e-1", "fee_asset": "EUR"},
            ]
        )
        await ex._adopt_ghost_orders([_make_order_snapshot()], set(), set())
        pending = ex.pending_orders["cid-1"]
        assert pending.last_recorded_cum_qty == 0.6
        assert pending.last_recorded_fee == {"EUR": pytest.approx(0.03)}


class TestInterlockBlockedDisposition:
    """Distinct, redispatch-safe handling of interlock-blocked submits."""

    def _order_executor(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build a sweep executor whose submit is blocked by the interlock."""
        ex = _make_sweep_executor()
        ex._settings_service = SimpleNamespace(get_setting_fresh=AsyncMock(return_value="halted"))
        ex.repository.has_order_submit_evidence = AsyncMock(return_value=False)
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row()
        )
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="failed")
        ex.settings.trade_command_dispatch_ttl_s = 0.0
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        return ex

    @pytest.mark.asyncio
    async def test_happy_path_records_fails_and_publishes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The full disposition runs record -> CAS FAILED -> REJECTED.

        Given: a submit blocked by the halted interlock,
        When: _process_order runs,
        Then: an order_interlock_blocked event records with status failed,
            the command CAS-es to FAILED, REJECTED publishes with the
            live_trading_halted reason, and the pending entry pops.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        events = [c.args[0] for c in ex._record_venue_event.await_args_list]
        interlock_events = [e for e in events if e["event_type"] == "order_interlock_blocked"]
        assert len(interlock_events) == 1
        assert interlock_events[0]["status"] == "failed"
        cas_kwargs = ex.repository.advance_trade_command_lifecycle.await_args_list[0].kwargs
        assert cas_kwargs["public_id"] == "cmd-1"
        assert cas_kwargs["new_status"] == "failed"
        assert cas_kwargs["last_error"] == "order_interlock_blocked"
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls[0].kwargs["reason"] == "live_trading_halted"
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_reduce_only_mode_uses_reduce_only_reason(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A reduce_only interlock publishes the reduce-only wire reason.

        Given: a submit blocked while the mode reads reduce_only,
        When: _process_order runs,
        Then: REJECTED publishes with the live_trading_reduce_only_unavailable
            reason while the durable event keeps the canonical error.
        """
        ex = self._order_executor(monkeypatch)
        ex._settings_service = SimpleNamespace(
            get_setting_fresh=AsyncMock(return_value="reduce_only")
        )
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls[0].kwargs["reason"] == "live_trading_reduce_only_unavailable"
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_event_write_failure_parks_without_publish(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed durable event write parks the entry — intent stays held.

        Given: the order_interlock_blocked write raising,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks with
            interlock_blocked_pending.
        """
        ex = self._order_executor(monkeypatch)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls == []
        assert ex.pending_orders["cid-1"].interlock_blocked_pending is True

    @pytest.mark.asyncio
    async def test_lost_cas_with_live_row_parks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-terminal row after all CAS attempts parks the entry.

        Given: every FAILED CAS losing while the row reads dispatched,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="dispatched")
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        reject_calls = [
            c
            for c in ex._publish_order_status.await_args_list
            if c.args[1] == OrderEventEnum.REJECTED
        ]
        assert reject_calls == []
        assert ex.pending_orders["cid-1"].interlock_blocked_pending is True

    @pytest.mark.asyncio
    async def test_already_terminal_row_counts_as_done(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A row already FAILED (earlier attempt / fold) completes the CAS step.

        Given: all CAS attempts losing while the current status reads failed,
        When: _process_order runs,
        Then: the disposition completes and the entry pops.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_terminal_row_short_circuits_cas(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A row the fold already terminalized needs no CAS attempts.

        Given: the strict lookup returning a FAILED command row,
        When: _process_order hits the interlock,
        Then: no CAS is attempted and the disposition completes.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(
            return_value=_make_cmd_row(status="failed")
        )
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_missing_command_row_counts_as_done(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No durable command row means nothing to terminalize.

        Given: the strict lookup returning None (manual/paper flow),
        When: _process_order hits the interlock,
        Then: the disposition completes without any CAS.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        ex.repository.advance_trade_command_lifecycle.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_publish_failure_parks_for_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failed REJECTED publish parks the entry for the recon retry.

        Given: a publisher returning False for the REJECTED,
        When: _process_order runs,
        Then: the entry parks with interlock_blocked_pending.
        """
        ex = self._order_executor(monkeypatch)

        async def _publish(order: Any, status: str, *args: Any, **kwargs: Any) -> bool:
            return status != OrderEventEnum.REJECTED

        ex._publish_order_status = AsyncMock(side_effect=_publish)
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert ex.pending_orders["cid-1"].interlock_blocked_pending is True

    @pytest.mark.asyncio
    async def test_recon_retry_heals_parked_disposition(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The recon sweep reruns the disposition probe-guarded.

        Given: a parked interlock_blocked_pending entry whose event already
            committed (probe True),
        When: the retry runs,
        Then: no duplicate event writes, the CAS and publish complete,
            and the entry pops.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.interlock_blocked_pending = True
        ex.pending_orders["cid-1"] = pending
        ex.repository.has_venue_event = AsyncMock(return_value=True)
        await ex._retry_interlock_blocked("cid-1")
        ex._record_venue_event.assert_not_awaited()
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_retry_skips_unflagged_entries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The retry only touches parked interlock entries.

        Given: a normal pending entry without the flag,
        When: the retry runs,
        Then: nothing happens.
        """
        ex = self._order_executor(monkeypatch)
        order = order_request_from_command(_make_cmd_row())
        ex.pending_orders["cid-1"] = PendingOrderState(request=order)
        await ex._retry_interlock_blocked("cid-1")
        assert "cid-1" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_plain_repository_skips_cas(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without a SQL repository the CAS step is a pass-through.

        Given: a paper/test executor with a plain MagicMock repository,
        When: _process_order hits the interlock,
        Then: the disposition still completes (publish + pop).
        """
        ex = self._order_executor(monkeypatch)
        ex.repository = MagicMock()
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert "cid-1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_cas_error_parks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A DB error during the FAILED CAS parks the entry.

        Given: advance_trade_command_lifecycle raising,
        When: _process_order runs,
        Then: no REJECTED publishes and the entry parks.
        """
        ex = self._order_executor(monkeypatch)
        ex.repository.advance_trade_command_lifecycle = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        order = order_request_from_command(_make_cmd_row())
        await ex._process_order(order)
        assert ex.pending_orders["cid-1"].interlock_blocked_pending is True


class TestInterlockBlockedEdges:
    """Residual coverage edges of the interlock disposition and seeding."""

    @pytest.mark.asyncio
    async def test_incomplete_disposition_without_pending_parks_fresh_entry(self) -> None:
        """A failed disposition with no pending entry parks a FRESH one.

        Given: an interlock resume invoked while no pending entry exists
            (the interlock gate runs before the submit entry is created)
            and the disposition failing,
        When: _handle_interlock_blocked_submit runs directly,
        Then: a pending entry is created, parked, and stamped with the
            block reason so the recon retry carries the wire reason.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        await ex._handle_interlock_blocked_submit(order, "live_trading_halted")
        parked = ex.pending_orders["cid-1"]
        assert parked.interlock_blocked_pending is True
        assert parked.interlock_blocked_reason == "live_trading_halted"

    @pytest.mark.asyncio
    async def test_incomplete_disposition_reuses_existing_pending(self) -> None:
        """A failed disposition keeps and flags the existing pending entry.

        Given: an interlock resume invoked while a pending entry already
            exists and the disposition failing,
        When: _handle_interlock_blocked_submit runs directly,
        Then: the SAME entry object is retained, flagged, and stamped with
            the reduce-only reason.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        existing = PendingOrderState(request=order)
        ex.pending_orders["cid-1"] = existing
        await ex._handle_interlock_blocked_submit(order, "live_trading_reduce_only_unavailable")
        assert ex.pending_orders["cid-1"] is existing
        assert existing.interlock_blocked_pending is True
        assert existing.interlock_blocked_reason == "live_trading_reduce_only_unavailable"

    @pytest.mark.asyncio
    async def test_retry_keeps_parked_entry_on_repeat_failure(self) -> None:
        """A still-failing retry leaves the entry parked.

        Given: a parked interlock entry whose event write keeps raising,
        When: the retry runs,
        Then: the entry stays parked with the flag set.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.interlock_blocked_pending = True
        ex.pending_orders["cid-1"] = pending
        await ex._retry_interlock_blocked("cid-1")
        assert ex.pending_orders["cid-1"].interlock_blocked_pending is True

    @pytest.mark.asyncio
    async def test_recon_cycle_drives_parked_interlock_retries(self) -> None:
        """The recon cycle replays parked interlock dispositions.

        Given: a parked interlock entry whose durable event already
            committed and a healthy repository,
        When: one reconciliation cycle runs,
        Then: the disposition completes and the entry pops.
        """
        ex = _make_sweep_executor()
        ex.repository.has_venue_event = AsyncMock(return_value=True)
        ex.repository.get_current_trade_command_status = AsyncMock(return_value="failed")
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        order = order_request_from_command(_make_cmd_row())
        pending = PendingOrderState(request=order)
        pending.interlock_blocked_pending = True
        ex.pending_orders["cid-1"] = pending
        await ex._reconcile_with_exchange()
        assert "cid-1" not in ex.pending_orders


class TestLiveTradingModeRead:
    """Fail-closed fresh read of live_trading_mode for the interlock."""

    def _executor(self) -> Any:
        """Build a minimal executor for direct mode reads."""
        return _make_executor()

    @pytest.mark.asyncio
    async def test_no_service_fails_closed_to_unavailable(self) -> None:
        """A missing settings service collapses to the UNAVAILABLE sentinel.

        Given: an executor whose settings service was never wired,
        When: _read_live_trading_mode runs,
        Then: it returns the blocking UNAVAILABLE sentinel without touching
            any read.
        """
        ex = self._executor()
        ex._settings_service = None
        assert await ex._read_live_trading_mode() == _LIVE_TRADING_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_read_exception_fails_closed_to_unavailable(self) -> None:
        """A raising fresh read collapses to the UNAVAILABLE sentinel.

        Given: get_setting_fresh raising a query/decrypt error,
        When: _read_live_trading_mode runs,
        Then: the except branch returns the blocking UNAVAILABLE sentinel.
        """
        ex = self._executor()
        ex._settings_service = SimpleNamespace(
            get_setting_fresh=AsyncMock(side_effect=RuntimeError("db down"))
        )
        assert await ex._read_live_trading_mode() == _LIVE_TRADING_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_invalid_value_fails_closed_to_unavailable(self) -> None:
        """A stored value outside the three modes collapses to UNAVAILABLE.

        Given: get_setting_fresh returning an unrecognized string,
        When: _read_live_trading_mode runs,
        Then: it returns the blocking UNAVAILABLE sentinel rather than the
            arbitrary value.
        """
        ex = self._executor()
        ex._settings_service = SimpleNamespace(get_setting_fresh=AsyncMock(return_value="bogus"))
        assert await ex._read_live_trading_mode() == _LIVE_TRADING_UNAVAILABLE

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["halted", "reduce_only", "enabled"])
    async def test_valid_value_passes_through(self, mode: str) -> None:
        """Each of the three valid modes returns unchanged.

        Given: get_setting_fresh returning a recognized mode,
        When: _read_live_trading_mode runs,
        Then: it returns that exact mode.
        """
        ex = self._executor()
        ex._settings_service = SimpleNamespace(get_setting_fresh=AsyncMock(return_value=mode))
        assert await ex._read_live_trading_mode() == mode

    @pytest.mark.asyncio
    async def test_slow_read_trips_timeout_and_fails_closed_to_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A read slower than the time-box fails closed to UNAVAILABLE.

        Given: a fresh read that sleeps beyond the (shrunk) timeout,
        When: _read_live_trading_mode runs,
        Then: asyncio.timeout cancels it and the except branch returns the
            blocking UNAVAILABLE sentinel so a wedged database cannot starve
            the handler.
        """
        ex = self._executor()

        async def _slow(_key: str) -> str:
            await asyncio.sleep(1.0)
            return "enabled"

        ex._settings_service = SimpleNamespace(get_setting_fresh=_slow)
        monkeypatch.setattr(base_module, "_LIVE_TRADING_MODE_READ_TIMEOUT_S", 0.02)
        assert await ex._read_live_trading_mode() == _LIVE_TRADING_UNAVAILABLE


class TestLiveTradingInterlockGate:
    """Venue-scoped gating of submits on the live-trading interlock."""

    def _executor(self) -> Any:
        """Build a sweep executor with the disposition and mode read spied."""
        ex = _make_sweep_executor()
        ex._handle_interlock_blocked_submit = AsyncMock()
        ex._read_live_trading_mode = AsyncMock(return_value="halted")
        return ex

    @pytest.mark.asyncio
    async def test_paper_venue_always_passes_ignoring_order_mode(self) -> None:
        """A paper venue passes even when order.mode is live.

        Given: a paper executor and an order whose mode is live,
        When: _is_live_trading_interlocked runs with the venue name,
        Then: it returns False, never reads the mode, and never disposes —
            the discriminator is the venue, not the caller-supplied mode.
        """
        ex = self._executor()
        ex._get_exchange_name = lambda: ExchangeEnum.PAPER
        order = order_request_from_command(_make_cmd_row())
        assert order.mode == "live"
        result = await ex._is_live_trading_interlocked(order, ex._get_exchange_name())
        assert result is False
        ex._read_live_trading_mode.assert_not_awaited()
        ex._handle_interlock_blocked_submit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabled_mode_passes(self) -> None:
        """An enabled mode lets a non-paper submit proceed.

        Given: a kraken executor whose mode reads enabled,
        When: _is_live_trading_interlocked runs,
        Then: it returns False without disposing.
        """
        ex = self._executor()
        ex._read_live_trading_mode = AsyncMock(return_value="enabled")
        order = order_request_from_command(_make_cmd_row())
        result = await ex._is_live_trading_interlocked(order, ex._get_exchange_name())
        assert result is False
        ex._handle_interlock_blocked_submit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_halted_mode_blocks_with_halted_reason(self) -> None:
        """A halted mode blocks and disposes with the halted reason.

        Given: a kraken executor whose mode reads halted,
        When: _is_live_trading_interlocked runs,
        Then: it returns True and disposes with the live_trading_halted reason.
        """
        ex = self._executor()
        order = order_request_from_command(_make_cmd_row())
        result = await ex._is_live_trading_interlocked(order, ex._get_exchange_name())
        assert result is True
        ex._handle_interlock_blocked_submit.assert_awaited_once_with(order, "live_trading_halted")

    @pytest.mark.asyncio
    async def test_reduce_only_mode_blocks_with_reduce_only_reason(self) -> None:
        """A reduce_only mode blocks and disposes with the reduce-only reason.

        Given: a kraken executor whose mode reads reduce_only,
        When: _is_live_trading_interlocked runs,
        Then: it returns True and disposes with the reduce-only reason.
        """
        ex = self._executor()
        ex._read_live_trading_mode = AsyncMock(return_value="reduce_only")
        order = order_request_from_command(_make_cmd_row())
        result = await ex._is_live_trading_interlocked(order, ex._get_exchange_name())
        assert result is True
        ex._handle_interlock_blocked_submit.assert_awaited_once_with(
            order, "live_trading_reduce_only_unavailable"
        )

    @pytest.mark.asyncio
    async def test_unavailable_mode_blocks_with_mode_unavailable_reason(self) -> None:
        """An unreadable mode blocks and disposes with the mode-unavailable reason.

        Given: a kraken executor whose mode read collapses to the blocking
            UNAVAILABLE sentinel (no authoritative value — no settings
            service, a timed-out/errored read, or an unrecognized value),
        When: _is_live_trading_interlocked runs,
        Then: it returns True and disposes with the live_trading_mode_unavailable
            reason (the default interlock reason for a non-authoritative read),
            keeping an infrastructure incident distinct from a deliberate halt.
        """
        ex = self._executor()
        ex._read_live_trading_mode = AsyncMock(return_value=_LIVE_TRADING_UNAVAILABLE)
        order = order_request_from_command(_make_cmd_row())
        result = await ex._is_live_trading_interlocked(order, ex._get_exchange_name())
        assert result is True
        ex._handle_interlock_blocked_submit.assert_awaited_once_with(
            order, "live_trading_mode_unavailable"
        )


class TestCorrectiveFeeDeferral:
    """Transient fee-source failures defer instead of freezing fee-less."""

    @pytest.mark.asyncio
    async def test_summary_lookup_failure_defers_priced_corrective(self) -> None:
        """A failed summary lookup defers even when the price is usable.

        Given: a priced snapshot with no commission data and a fills
            summary lookup that raises,
        When: the fill-gap pass runs,
        Then: NO corrective emits — the stable exec id would freeze a
            fee-less emission; the watermark stays put for a retry.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            side_effect=RuntimeError("venue down")
        )
        pending = _make_pending()
        snapshot = _make_order_snapshot(filled=0.5)
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
        ex._process_execution.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_partial_coverage_defers_priced_corrective(self) -> None:
        """A partial fills page defers the fee attribution.

        Given: a priced snapshot with no commission and a summary
            covering only part of the filled quantity,
        When: the fill-gap pass runs,
        Then: NO corrective emits this cycle.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(
                vwap=100.0, covered_qty=0.1, fee_total=0.01, fee_currency="USD"
            )
        )
        pending = _make_pending()
        snapshot = _make_order_snapshot(filled=0.5)
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
        ex._process_execution.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_implemented_source_returning_none_defers(self) -> None:
        """None from an IMPLEMENTED summary source defers, not emits.

        Given: a priced snapshot with no commission on a venue whose
            fills summary IS implemented (supports_fill_summary) but
            returned None (fills page has no usable rows yet),
        When: the fill-gap pass runs,
        Then: NO corrective emits — only a venue with no source at all
            may emit fee-less.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.supports_fill_summary = True
        pending = _make_pending()
        snapshot = _make_order_snapshot(filled=0.5)
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
        ex._process_execution.assert_not_awaited()


class TestDeferredGapHoldsTerminal:
    """A deferred corrective blocks the terminal pop until it lands."""

    @pytest.mark.asyncio
    async def test_disappeared_terminal_held_on_deferred_gap(self) -> None:
        """The disappeared-order terminal waits for the deferred fee.

        Given: a venue-terminal order with an unpublished fill gap whose
            fee source transiently failed (summary raises, no snapshot
            commission, price usable),
        When: the disappeared-order reconciliation runs,
        Then: NO terminal emits and the pending entry survives — popping
            it would orphan the deferred corrective forever.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            side_effect=RuntimeError("venue down")
        )
        pending = _make_pending()
        ex.pending_orders["cid-1"] = pending
        ex.exchange_client.get_order = AsyncMock(
            return_value=_make_order_snapshot(filled=0.5, status=ExchangeOrderStatusEnum.CLOSED)
        )
        await ex._reconcile_disappeared_order("kraken", "ex-1", pending)
        ex._process_execution.assert_not_awaited()
        assert "cid-1" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_disappeared_terminal_held_on_failed_corrective_publish(self) -> None:
        """A swallowed corrective publish failure also holds the terminal.

        Given: a venue-terminal order whose gap corrective records but
            fails to publish (committed watermark does not advance),
        When: the disappeared-order reconciliation runs,
        Then: NO terminal emits and the entry survives for the retry.
        """
        ex = _make_sweep_executor()
        ex._record_venue_event = AsyncMock()
        ex._publish_execution = AsyncMock(return_value=False)
        pending = PendingOrderState(request=order_request_from_command(_make_cmd_row()))
        pending.exchange_order_id = "ex-1"
        ex.pending_orders["cid-1"] = pending
        ex.client_by_exchange["ex-1"] = "cid-1"
        ex.exchange_client.get_order = AsyncMock(
            return_value=_make_order_snapshot(
                filled=0.5, status=ExchangeOrderStatusEnum.CLOSED, price=100.0
            )
        )
        await ex._reconcile_disappeared_order("kraken", "ex-1", pending)
        assert "cid-1" in ex.pending_orders
        published_types = [c.args[1].status for c in ex._publish_execution.await_args_list]
        assert len(published_types) == 1

    @pytest.mark.asyncio
    async def test_fee_deferral_cap_emits_feeless_with_escalation(self) -> None:
        """Past the deferral cap the corrective emits fee-less.

        Given: a priced snapshot whose fee source keeps failing for more
            than the deferral cap,
        When: the fill-gap pass runs cap+1 times,
        Then: the first cap attempts defer, the final attempt emits the
            corrective FEE-LESS (terminal can then project) and clears
            the counter.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(side_effect=RuntimeError("aged out"))
        pending = _make_pending()
        snapshot = _make_order_snapshot(filled=0.5)
        for _ in range(5):
            result = await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
            assert result == "deferred"
        ex._process_execution.assert_not_awaited()

        async def _commit(execution: Any) -> None:
            pending.last_seen_cum_qty = execution.cum_qty

        ex._process_execution = AsyncMock(side_effect=_commit)
        result = await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
        assert result == "emitted"
        corrective = ex._process_execution.await_args.args[0]
        assert corrective.cum_fee is None
        assert corrective.fees is None
        assert "ex-1" not in ex._gap_fee_deferrals

    @pytest.mark.asyncio
    async def test_priced_snapshot_still_gets_summary_fees(self) -> None:
        """A usable snapshot price no longer skips the fee summary.

        Given: a priced snapshot without commission data and a
            coverage-complete fills summary carrying fees,
        When: the fill-gap pass runs,
        Then: the corrective keeps the snapshot price AND carries the
            summary's cumulative fee.
        """
        ex = _make_sweep_executor()
        ex._process_execution = AsyncMock()
        ex.exchange_client.get_order_fill_summary = AsyncMock(
            return_value=OrderFillSummary(
                vwap=99.0, covered_qty=0.5, fee_total=0.6, fee_currency="USD"
            )
        )
        pending = _make_pending()
        snapshot = _make_order_snapshot(filled=0.5, price=100.0)
        await ex._reconcile_fill_gap("kraken", "ex-1", pending, snapshot)
        corrective = ex._process_execution.await_args.args[0]
        assert corrective.last_price == 100.0
        assert corrective.cum_fee == pytest.approx(0.6)
        assert corrective.cum_fee_currency == "USD"
