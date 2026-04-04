"""Tests for Phase 3c: venue reconciliation in executor."""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
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
    status: OrderStatusEnum = OrderStatusEnum.OPEN,
) -> ExchangeOrderSnapshot:
    """Build an ExchangeOrderSnapshot for testing."""
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id="cid-1",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
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
        canceled_snap = _make_order_snapshot(filled=0.0, status=OrderStatusEnum.CANCELED)
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
        filled_snap = _make_order_snapshot(filled=1.0, price=100.0, status=OrderStatusEnum.CLOSED)
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
        """Fill gap with no price (market order) is skipped with error log.

        Given: exchange shows fill gap but order price is None,
        When: _reconcile_with_exchange runs,
        Then: no corrective fill processed, error logged.
        """
        ex = _make_executor()
        ex.pending_orders["cid-1"] = _make_pending(cum_qty=0.0)
        snap = _make_order_snapshot(filled=5.0, price=None)
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
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
