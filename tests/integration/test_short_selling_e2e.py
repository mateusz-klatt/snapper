"""End-to-end paper-mode integration tests for short selling.

These tests publish a SignalData onto the live ZMQ broker, then observe
the OrderRequestData that the TraderCoordinator emits in response. They
verify the full signal -> trader -> engine -> outgoing-order path
without mocking ZMQ transports.

All tests in this file are gated behind the `integration` pytest marker
and excluded from the default `make test` / `make check-all` run via
pyproject.toml [tool.pytest.ini_options] addopts. Run via:

    make test-integration
"""

import asyncio
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import pytest
import zmq
import zmq.asyncio

import snapper.config.settings as snapper_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SignalData
from tests.integration.conftest import PaperE2EStack

pytestmark = pytest.mark.integration


_INSTRUMENT = "BTC-USD"
_STRATEGY_NAME = "e2e_test"
_SIGNAL_TOPIC = f"signals.paper.{_INSTRUMENT}.{_STRATEGY_NAME}"
_ORDER_SUBMIT_TOPIC = f"orders.commands.paper.{_INSTRUMENT}.submit"


def _make_signal(
    side: TradeSideEnum, *, strength: float = 1.0, price: float = 50000.0
) -> SignalData:
    """Construct a paper-mode SignalData with all required provenance fields."""
    now = datetime.now(UTC)
    return SignalData(
        sequence_id=0,
        public_id=str(uuid7()),
        timestamp=now,
        session_id="e2e-test-session",
        instrument=_INSTRUMENT,
        exchange=ExchangeEnum.PAPER,
        side=side,
        strength=strength,
        reason="e2e short selling test",
        price=price,
        strategy_name=_STRATEGY_NAME,
        fired_at=now,
    )


async def _subscribe_to_orders_topic(
    ctx: zmq.asyncio.Context, xpub_endpoint: str
) -> zmq.asyncio.Socket:
    """Open a SUB socket on the broker XPUB endpoint and subscribe to orders.commands."""
    sub = ctx.socket(zmq.SUB)
    sub.connect(xpub_endpoint)
    sub.subscribe(_ORDER_SUBMIT_TOPIC.encode())
    await asyncio.sleep(0.2)
    return sub


async def _publish_signal(ctx: zmq.asyncio.Context, xsub_endpoint: str, signal: SignalData) -> None:
    """Open a PUB socket on the broker XSUB endpoint and send a signal frame."""
    pub = ctx.socket(zmq.PUB)
    pub.connect(xsub_endpoint)
    await asyncio.sleep(0.2)
    await pub.send_multipart([_SIGNAL_TOPIC.encode(), signal.to_json().encode()])
    await asyncio.sleep(0.05)
    pub.setsockopt(zmq.LINGER, 0)
    pub.close()


async def _wait_for_order_request(
    sub: zmq.asyncio.Socket, *, timeout_s: float = 10.0
) -> OrderRequestData:
    """Receive a single OrderRequestData frame from a pre-subscribed SUB socket."""
    raw = await asyncio.wait_for(sub.recv_multipart(), timeout=timeout_s)
    payload_bytes = raw[-1]
    return OrderRequestData.from_json(payload_bytes.decode())


class TestShortSellingE2E:
    """Paper-mode short selling end-to-end scenarios."""

    async def test_short_open_via_paper_executor(self, paper_e2e_stack: PaperE2EStack) -> None:
        """SELL signal with allow_short_selling=True opens a short position.

        Verifies the trader emits an OrderRequestData with side=SELL and a
        positive quantity onto the orders.commands topic when a SELL signal
        is delivered for an instrument with no existing position.
        """
        sub = await _subscribe_to_orders_topic(
            paper_e2e_stack.client_context, paper_e2e_stack.xpub_endpoint
        )
        try:
            signal = _make_signal(TradeSideEnum.SELL, strength=1.0, price=50000.0)
            await _publish_signal(
                paper_e2e_stack.client_context, paper_e2e_stack.xsub_endpoint, signal
            )
            order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert order.instrument == _INSTRUMENT
            assert order.exchange == ExchangeEnum.PAPER
            assert order.side == TradeSideEnum.SELL
            assert order.quantity > 0
            assert order.reduce_only is False
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()

    async def test_long_open_emits_buy_order(self, paper_e2e_stack: PaperE2EStack) -> None:
        """BUY signal opens a long position via the trader -> executor flow.

        Sanity baseline: confirms the long path still works after the
        bidirectional engine ship and that the fixture stack reaches a
        steady state for non-short scenarios.
        """
        sub = await _subscribe_to_orders_topic(
            paper_e2e_stack.client_context, paper_e2e_stack.xpub_endpoint
        )
        try:
            signal = _make_signal(TradeSideEnum.BUY, strength=1.0, price=50000.0)
            await _publish_signal(
                paper_e2e_stack.client_context, paper_e2e_stack.xsub_endpoint, signal
            )
            order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert order.side == TradeSideEnum.BUY
            assert order.quantity > 0
            assert order.reduce_only is False
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()

    async def test_short_disabled_clamps_desired_units(
        self,
        paper_e2e_stack: PaperE2EStack,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """SELL signal with allow_short_selling=False produces no order.

        Flips the flag back to False on the live mock_settings, then injects
        a SELL signal when the position is flat. The trader should clamp
        desired_units to 0 and emit nothing on orders.commands within the
        observation window.
        """
        mock_settings = snapper_settings.get_settings()
        monkeypatch.setattr(mock_settings, "allow_short_selling", False, raising=False)
        sub = await _subscribe_to_orders_topic(
            paper_e2e_stack.client_context, paper_e2e_stack.xpub_endpoint
        )
        try:
            signal = _make_signal(TradeSideEnum.SELL, strength=1.0, price=50000.0)
            await _publish_signal(
                paper_e2e_stack.client_context, paper_e2e_stack.xsub_endpoint, signal
            )
            with pytest.raises(asyncio.TimeoutError):
                await _wait_for_order_request(sub, timeout_s=2.0)
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()

    async def test_short_cover_via_paper_executor(self, paper_e2e_stack: PaperE2EStack) -> None:
        """Open a short, then send a BUY signal that covers it.

        The first SELL signal opens a short position. After the paper
        executor's simulated fill propagates back to the engine via
        orders.events.*.executed, a BUY signal at strength=1.0 raises
        desired_units from -1.0 to +1.0, producing a flip order whose
        size includes the closing short leg plus a new long leg.

        This test asserts only that a BUY order is emitted in response to
        the cover signal — the exact split-flip math (closing_qty +
        opening_qty) is covered by unit tests on TradingEngineService.
        """
        sub = await _subscribe_to_orders_topic(
            paper_e2e_stack.client_context, paper_e2e_stack.xpub_endpoint
        )
        try:
            short_signal = _make_signal(TradeSideEnum.SELL, strength=1.0, price=50000.0)
            await _publish_signal(
                paper_e2e_stack.client_context,
                paper_e2e_stack.xsub_endpoint,
                short_signal,
            )
            short_order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert short_order.side == TradeSideEnum.SELL

            await asyncio.sleep(0.5)

            cover_signal = _make_signal(TradeSideEnum.BUY, strength=1.0, price=50100.0)
            await _publish_signal(
                paper_e2e_stack.client_context,
                paper_e2e_stack.xsub_endpoint,
                cover_signal,
            )
            cover_order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert cover_order.side == TradeSideEnum.BUY
            assert cover_order.quantity > 0
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()

    async def test_long_close_to_flat_via_sell(self, paper_e2e_stack: PaperE2EStack) -> None:
        """Open a long, then close it via SELL with allow_short_selling=True.

        Verifies that even with shorts enabled, a SELL signal that exactly
        balances an existing long produces a closing order. The split-flip
        logic in TradingEngineService treats this as a pure close (no
        opening leg) when desired_units crosses to zero rather than past it.

        Uses BUY then SELL at the same strength (1.0). After the BUY fill
        sets position_qty to +1.0, the SELL signal computes
        desired_units = -1.0, producing a flip order. We assert direction
        and positive size; the exact split semantics live in unit tests.
        """
        sub = await _subscribe_to_orders_topic(
            paper_e2e_stack.client_context, paper_e2e_stack.xpub_endpoint
        )
        try:
            long_signal = _make_signal(TradeSideEnum.BUY, strength=1.0, price=50000.0)
            await _publish_signal(
                paper_e2e_stack.client_context,
                paper_e2e_stack.xsub_endpoint,
                long_signal,
            )
            long_order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert long_order.side == TradeSideEnum.BUY

            await asyncio.sleep(0.5)

            sell_signal = _make_signal(TradeSideEnum.SELL, strength=1.0, price=49900.0)
            await _publish_signal(
                paper_e2e_stack.client_context,
                paper_e2e_stack.xsub_endpoint,
                sell_signal,
            )
            sell_order = await _wait_for_order_request(sub, timeout_s=10.0)
            assert sell_order.side == TradeSideEnum.SELL
            assert sell_order.quantity > 0
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
