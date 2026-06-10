"""Tests for venue reconciliation in executor."""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest
from loguru import logger

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
) -> ExchangeOrderSnapshot:
    """Build an ExchangeOrderSnapshot for testing."""
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id="cid-1",
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
        """Disappeared order with non-terminal status (e.g., OPEN) is deferred.

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
    """Recon-loop resolution of parked ambiguous entries (#145 P0-1 slice 4)."""

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
