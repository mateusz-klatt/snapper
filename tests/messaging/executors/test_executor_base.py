"""Tests for ExchangeExecutorService base class."""

import asyncio
import contextlib
import json
import time as time_module
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from types import TracebackType
from typing import Any
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch
from unittest.mock import patch as mock_patch

import pytest
import zmq

import snapper.messaging.executors.base as base_module
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.kraken import KrakenOrderExecutor
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.messages import MessageParseError

TEST_DB_URL = "sqlite:///:memory:"


def zmq_socket_stub(
    track_calls: list[tuple[int, int]] | None = None, **kwargs: Any
) -> SimpleNamespace:
    """Create a stub ZMQ socket for testing."""

    def _setsockopt(opt: int, val: int) -> None:
        if track_calls is not None:
            track_calls.append((opt, val))

    return SimpleNamespace(setsockopt=_setsockopt, **kwargs)


class DummyExecutor(ExchangeExecutorService[Any]):
    """Test stub for ExchangeExecutorService."""

    def __init__(self, client: Any) -> None:
        """Initialize the instance."""
        super().__init__()
        self._client = client
        self.settings = cast(
            Any,
            SimpleNamespace(
                db_url=TEST_DB_URL,
                zmq_broker_xpub="xpub",
                zmq_broker_xsub="xsub",
                master_password=None,
            ),
        )

    def _create_exchange_client(self) -> Any:
        return self._client

    def _get_exchange_name(self) -> Literal["paper"]:
        return "paper"


@pytest.mark.asyncio
async def test_start_with_websocket_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test executor start with websocket-enabled client.

    Given: An executor with a websocket-supporting exchange client,
    When: start() is called,
    Then: The executor runs with order, execution, and heartbeat handlers.
    """

    class WebsocketClient:
        supports_websocket_executions = True

        def set_tracker(self, tracker: Any) -> None:
            """No-op tracker injection for tests."""

        async def __aenter__(self) -> WebsocketClient:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

    client = WebsocketClient()
    executor = DummyExecutor(client)
    cast(Any, executor)._order_handler = lambda: asyncio.sleep(0)
    cast(Any, executor)._execution_handler = lambda: asyncio.sleep(0)
    cast(Any, executor)._heartbeat_loop = lambda: asyncio.sleep(0)
    monkeypatch.setattr(base_module, "get_repository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(
        base_module,
        "get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(base_module, "get_settings_with_service", lambda _svc: executor.settings)

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None,
                close=lambda: None,
                term=lambda: None,
                setsockopt=lambda *_: None,
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

    monkeypatch.setattr("snapper.messaging.executors.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        base_module,
        "ValidatedSubscriber",
        lambda sock: SimpleNamespace(close=sock.close, subscribe=lambda *_: None),
    )
    monkeypatch.setattr(
        base_module,
        "ValidatedPublisher",
        lambda sock: SimpleNamespace(close=sock.close),
    )
    monkeypatch.setattr(asyncio, "gather", AsyncMock(return_value=None))
    await executor.start()


@pytest.mark.asyncio
async def test_start_without_websocket_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test executor start without websocket support.

    Given: An executor with a non-websocket exchange client,
    When: start() is called,
    Then: The executor runs order and heartbeat handlers without execution handler.
    """

    class NoWebsocketClient:
        supports_websocket_executions = False

        def set_tracker(self, tracker: Any) -> None:
            """No-op tracker injection for tests."""

        async def __aenter__(self) -> NoWebsocketClient:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

    client = NoWebsocketClient()
    executor = DummyExecutor(client)
    cast(Any, executor)._order_handler = lambda: asyncio.sleep(0)
    cast(Any, executor)._heartbeat_loop = lambda: asyncio.sleep(0)
    monkeypatch.setattr(base_module, "get_repository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(
        base_module,
        "get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(base_module, "get_settings_with_service", lambda _svc: executor.settings)

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None,
                close=lambda: None,
                term=lambda: None,
                setsockopt=lambda *_: None,
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

    monkeypatch.setattr("snapper.messaging.executors.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        base_module,
        "ValidatedSubscriber",
        lambda sock: SimpleNamespace(close=sock.close, subscribe=lambda *_: None),
    )
    monkeypatch.setattr(
        base_module,
        "ValidatedPublisher",
        lambda sock: SimpleNamespace(close=sock.close),
    )
    monkeypatch.setattr(asyncio, "gather", AsyncMock(return_value=None))
    await executor.start()


class MergedDummyClient(SimpleNamespace):
    """Test stub for exchange client with order methods."""

    async def create_order(self, request: Any) -> SimpleNamespace:
        """Create a test order."""
        return SimpleNamespace(id="ex123", db_order_id=None, db_order_public_id=None)

    async def subscribe_executions(self) -> AsyncIterator[Any]:
        """Subscribe to execution updates."""
        if False:
            yield None


class MergedDummyExecutor(ExchangeExecutorService[Any]):
    """Test stub for merged exchange executor."""

    def _create_exchange_client(self) -> MergedDummyClient:
        return MergedDummyClient()

    def _get_exchange_name(self) -> str:
        return "kraken"


class ZondaDummyExecutor(ExchangeExecutorService[Any]):
    """Test stub for Zonda-specific execution handling."""

    def _create_exchange_client(self) -> MergedDummyClient:
        return MergedDummyClient()

    def _get_exchange_name(self) -> str:
        return "zonda"


def make_order(**overrides: Any) -> OrderRequestData:
    """Create an OrderRequestData with optional overrides."""
    return OrderRequestData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        type="order_request",
        exchange="paper",
        instrument=overrides.get("instrument", "BTC-USD"),
        side=overrides.get("side", "buy"),
        order_type=overrides.get("order_type", "limit"),
        price=overrides.get("price", 1.0),
        quantity=overrides.get("quantity", 1.0),
        strategy_id=overrides.get("strategy_id", "s1"),
        client_order_id=overrides.get("client_order_id", "c1"),
        mode=overrides.get("mode", "paper"),
        signaled_at=overrides.get("signaled_at", datetime.now(tz=UTC)),
    )


@pytest.mark.asyncio
async def test_process_order_rejects_non_tradeable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order rejection when instrument is not tradeable.

    Given: An executor where is_tradeable returns False,
    When: _process_order is called,
    Then: Order is rejected before reaching _execute_live_order.
    """
    ex: Any = MergedDummyExecutor()
    ex.publisher = SimpleNamespace(send_multipart=AsyncMock())
    ex.running = True
    ex._publish_order_status = AsyncMock()
    ex._execute_live_order = AsyncMock()
    monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: False)
    order = make_order(instrument="NONTRADEABLE-USD")
    await ex._process_order(order)
    ex._execute_live_order.assert_not_awaited()
    ex._publish_order_status.assert_awaited_once()
    rejected_call = ex._publish_order_status.call_args
    assert rejected_call[0][1] == "rejected"


@pytest.mark.asyncio
async def test_process_order_rejects_on_execute_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order rejection when execution fails.

    Given: A running executor with mocked execute that raises error,
    When: _process_order is called,
    Then: Order status 'rejected' is published (no fill for syntactic rejection).
    """
    ex: Any = MergedDummyExecutor()
    ex.publisher = SimpleNamespace(send_multipart=AsyncMock())
    ex.running = True
    ex._publish_execution = AsyncMock()
    ex._publish_order_status = AsyncMock()
    ex._execute_live_order = AsyncMock(side_effect=RuntimeError("fail"))
    monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
    order = make_order()
    await ex._process_order(order)
    ex._publish_execution.assert_not_awaited()
    ex._publish_order_status.assert_awaited()


@pytest.mark.asyncio
async def test_execute_live_order_tracks_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that live order execution tracks pending orders.

    Given: An executor with exchange client that returns order ID,
    When: _execute_live_order is called,
    Then: The order is added to pending_orders dict.
    """
    ex: Any = MergedDummyExecutor()
    ex.exchange_client = MergedDummyClient()
    order = make_order()
    ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
    order_id = await ex._execute_live_order(order)
    assert order_id == "ex123"
    pending = ex.pending_orders[order.client_order_id]
    assert pending.exchange_order_id == "ex123"


@pytest.mark.asyncio
async def test_process_execution_unknown_order_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that unknown order execution logs warning.

    Given: An executor with no pending orders,
    When: _process_execution receives unknown order ID,
    Then: The execution is ignored and pending_orders remains empty.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.publisher = SimpleNamespace(send_multipart=AsyncMock())
    execution = SimpleNamespace(
        order_id="unknown",
        exec_type="trade",
        order_status=None,
        cum_qty=1.0,
        average_price=1.0,
        fee_usd_equiv=0.0,
    )
    await ex._process_execution(execution)
    assert not ex.pending_orders


@pytest.mark.asyncio
async def test_publish_order_status_skips_when_not_running() -> None:
    """Test that order status is not published when not running.

    Given: An executor that is not running,
    When: _publish_order_status is called,
    Then: No message is sent via publisher.
    """
    ex: Any = MergedDummyExecutor()
    ex.msg_publisher = AsyncMock()
    ex.running = False
    await ex._publish_order_status(make_order(), "submitted")
    ex.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_when_not_running() -> None:
    """Test stop is no-op when executor not running.

    Given: An executor that is not running,
    When: stop() is called,
    Then: Running state remains False.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = False
    await ex.stop()
    assert not ex.running


@pytest.mark.asyncio
async def test_stop_with_none_subscriber(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test stop handles None subscriber gracefully.

    Given: A running executor with subscriber set to None,
    When: stop() is called,
    Then: Executor stops without error.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.subscriber = None
    ex.publisher = zmq_socket_stub(close=lambda: None)
    ex.context = SimpleNamespace(term=lambda: None)
    await ex.stop()
    assert not ex.running


@pytest.mark.asyncio
async def test_stop_with_none_publisher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test stop handles None publisher gracefully.

    Given: A running executor with publisher set to None,
    When: stop() is called,
    Then: Executor stops without error.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.subscriber = zmq_socket_stub(close=lambda: None)
    ex.publisher = None
    ex.context = SimpleNamespace(term=lambda: None)
    await ex.stop()
    assert not ex.running


@pytest.mark.asyncio
async def test_stop_with_none_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test stop handles None context gracefully.

    Given: A running executor with context set to None,
    When: stop() is called,
    Then: Executor stops without error.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.subscriber = zmq_socket_stub(close=lambda: None)
    ex.publisher = zmq_socket_stub(close=lambda: None)
    ex.context = None
    await ex.stop()
    assert not ex.running


@pytest.mark.asyncio
async def test_handle_symbol_alias_update_invalid_json() -> None:
    """Test symbol alias update handles invalid JSON.

    Given: An executor instance,
    When: _handle_symbol_alias_update receives invalid JSON,
    Then: The method handles the error gracefully.
    """
    ex: Any = MergedDummyExecutor()
    ex._handle_symbol_alias_update("{bad json")


@pytest.mark.asyncio
async def test_order_handler_unexpected_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler ignores unexpected topics.

    Given: A running executor with subscriber returning unexpected topic,
    When: _order_handler processes messages,
    Then: The unexpected topic is ignored.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class OneShotSubscriber:
        def __init__(self) -> None:
            self.close = lambda: None

        async def recv_multipart(self) -> tuple[str, bytes]:
            ex.running = False
            return ("unexpected.topic", b"{}")

    ex.subscriber = OneShotSubscriber()
    task = asyncio.create_task(ex._order_handler())
    await task


@pytest.mark.asyncio
async def test_order_handler_malformed_topic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test order handler ignores malformed command topics.

    Given: A running executor with subscriber returning malformed topic (less than 5 segments),
    When: _order_handler processes messages,
    Then: The topic is ignored with warning logged.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class OneShotSubscriber:
        """Subscriber returning malformed topic."""

        def __init__(self) -> None:
            self.close = lambda: None

        async def recv_multipart(self) -> tuple[str, bytes]:
            ex.running = False
            return ("orders.commands.kraken.BTC-USD", b"{}")

    ex.subscriber = OneShotSubscriber()
    task = asyncio.create_task(ex._order_handler())
    await task


@pytest.mark.asyncio
async def test_order_handler_ignores_unknown_command_topic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test order handler ignores unknown command topics.

    Given: A running executor with subscriber returning orders.commands.kraken.BTC-USD.unknown,
    When: _order_handler processes messages,
    Then: The topic is ignored (logged at debug level).
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class OneShotSubscriber:
        """Subscriber returning unknown command topic."""

        def __init__(self) -> None:
            self.close = lambda: None

        async def recv_multipart(self) -> tuple[str, bytes]:
            ex.running = False
            return ("orders.commands.kraken.BTC-USD.unknown", b"{}")

    ex.subscriber = OneShotSubscriber()
    task = asyncio.create_task(ex._order_handler())
    await task


@pytest.mark.asyncio
async def test_order_handler_settings_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler processes settings messages.

    Given: A running executor with subscriber returning settings topic,
    When: _order_handler receives system.settings message,
    Then: Settings update handler is called.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class OneShotSubscriber:
        def __init__(self) -> None:
            self.close = lambda: None

        async def recv_multipart(self) -> tuple[str, bytes]:
            ex.running = False
            return (
                "system.settings",
                b'{"type":"setting_changed","session_id":"","sequence_id":0,"key":"foo","value":"bar"}',
            )

    ex.subscriber = OneShotSubscriber()
    handle_mock = MagicMock()
    ex._handle_settings_update = handle_mock
    task = asyncio.create_task(ex._order_handler())
    await task
    handle_mock.assert_called_once()


@pytest.mark.asyncio
async def test_order_handler_wrong_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler skips orders for different exchange.

    Given: A running executor configured for one exchange,
    When: Order for different exchange is received,
    Then: The order is not processed.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    order = cast(Any, make_order())
    order.exchange = "other"

    async def recv_multipart() -> tuple[str, bytes]:
        ex.running = False
        return ("orders.commands.kraken.BTC-USD.submit", b"{}")

    ex.subscriber = SimpleNamespace(
        recv_multipart=AsyncMock(side_effect=recv_multipart),
        close=lambda: None,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.base.parse_message",
        lambda _payload: order,
    )
    ex._process_order = AsyncMock()
    await ex._order_handler()
    ex._process_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_order_handler_non_order_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler ignores non-order messages.

    Given: A running executor with subscriber,
    When: Non-order message type is received,
    Then: The message is ignored.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    async def recv_multipart() -> tuple[str, bytes]:
        ex.running = False
        return ("orders.commands.kraken.BTC-USD.submit", b"{}")

    ex.subscriber = SimpleNamespace(
        recv_multipart=AsyncMock(side_effect=recv_multipart),
        close=lambda: None,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.base.parse_message",
        lambda _payload: SimpleNamespace(type="other", session_id="", sequence_id=0),
    )
    await ex._order_handler()


@pytest.mark.asyncio
async def test_order_handler_logs_error_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler logs errors and continues.

    Given: A running executor with subscriber that raises error,
    When: _order_handler encounters an exception,
    Then: Error is logged and handler stops.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    async def recv_multipart() -> tuple[str, bytes]:
        raise ValueError("boom")

    ex.subscriber = SimpleNamespace(
        recv_multipart=AsyncMock(side_effect=recv_multipart),
        close=lambda: None,
    )

    class DummyLogger:
        def error(self, *_args: Any, **_kwargs: Any) -> None:
            ex.running = False

    monkeypatch.setattr("snapper.messaging.executors.base.logger", DummyLogger())
    await ex._order_handler()


@pytest.mark.asyncio
async def test_order_handler_error_when_not_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test order handler suppresses errors when not running.

    Given: An executor that becomes not running after error,
    When: Error occurs during recv_multipart,
    Then: Error is not logged when executor is stopping.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    call_count = 0

    async def recv_multipart() -> tuple[str, bytes]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            ex.running = False
            raise ValueError("exception after shutdown")
        return ("", b"")

    ex.subscriber = SimpleNamespace(
        recv_multipart=AsyncMock(side_effect=recv_multipart),
    )
    logged_errors: list[str] = []

    class DummyLogger:
        def error(self, msg: str, *_args: Any, **_kwargs: Any) -> None:
            logged_errors.append(msg)

    monkeypatch.setattr("snapper.messaging.executors.base.logger", DummyLogger())
    await ex._order_handler()
    assert logged_errors == []


@pytest.mark.asyncio
async def test_execution_handler_exchange_client_none() -> None:
    """Test execution handler returns when client is None.

    Given: A running executor with no exchange client,
    When: _execution_handler is called,
    Then: Handler returns immediately.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.exchange_client = None
    await ex._execution_handler()


@pytest.mark.asyncio
async def test_execution_handler_not_implemented() -> None:
    """Test execution handler handles NotImplementedError.

    Given: A running executor with client that raises NotImplementedError,
    When: subscribe_executions raises NotImplementedError,
    Then: Handler returns gracefully.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class Client:
        supports_websocket_executions = True

        def subscribe_executions(self) -> AsyncIterator[Any]:
            async def _gen() -> AsyncIterator[Any]:
                raise NotImplementedError
                yield None

            return _gen()

    ex.exchange_client = Client()
    await ex._execution_handler()


@pytest.mark.asyncio
async def test_execution_handler_runtime_error_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution handler logs runtime errors.

    Given: A running executor with client that raises RuntimeError,
    When: subscribe_executions raises RuntimeError,
    Then: Error is logged and handler returns.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True

    class Client:
        supports_websocket_executions = True

        def subscribe_executions(self) -> AsyncIterator[Any]:
            async def _gen() -> AsyncIterator[Any]:
                for _ in []:
                    yield
                raise RuntimeError("ws fail")

            return _gen()

    ex.exchange_client = Client()
    await ex._execution_handler()


@pytest.mark.asyncio
async def test_process_execution_default_filled_and_removes_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution processing removes pending orders on fill.

    Given: An executor with a pending order and client_by_exchange mapping,
    When: Execution update with default filled status is received,
    Then: Fill is published and order is removed from pending.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    order = make_order()
    ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
    ex.client_by_exchange["ex1"] = order.client_order_id
    ex._publish_execution = AsyncMock()
    execution = SimpleNamespace(
        order_id="ex1",
        exec_type="trade",
        exec_id=None,
        order_status=None,
        cum_qty=None,
        average_price=None,
        fee_usd_equiv=None,
        fees=None,
        last_qty=None,
        last_price=None,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        side=SimpleNamespace(value="buy"),
        trade_id=None,
    )
    await ex._process_execution(execution)
    assert order.client_order_id not in ex.pending_orders
    ex._publish_execution.assert_awaited()


@pytest.mark.asyncio
async def test_process_execution_none_exchange_order_id() -> None:
    """Test execution with None exchange_order_id drops with warning.

    Given: An executor with pending orders,
    When: Execution update with order_id=None is processed,
    Then: No fill is published (cannot correlate without exchange_order_id).
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    order = make_order()
    ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
    published_fills: list[Any] = []

    async def track_publish_execution(topic: str, fill: Any) -> None:
        published_fills.append(fill)

    ex._publish_execution = track_publish_execution
    execution = SimpleNamespace(
        order_id=None,
        exec_type="filled",
        exec_id="trade-123",
        order_status=None,
        cum_qty=1.0,
        average_price=50000.0,
        fee_usd_equiv=0.5,
    )
    await ex._process_execution(execution)
    assert len(published_fills) == 0


@pytest.mark.asyncio
async def test_process_execution_orphaned_client_by_exchange() -> None:
    """Test execution with orphaned client_by_exchange mapping drops with warning.

    Given: An executor with client_by_exchange mapping but no matching pending order,
    When: Execution update is processed,
    Then: No fill is published (cannot correlate without pending order).
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.client_by_exchange["ex1"] = "orphaned_client_id"
    published_fills: list[Any] = []

    async def track_publish_execution(topic: str, fill: Any) -> None:
        published_fills.append(fill)

    ex._publish_execution = track_publish_execution
    execution = SimpleNamespace(
        order_id="ex1",
        exec_type="filled",
        exec_id="trade-123",
        order_status=None,
        cum_qty=1.0,
        average_price=50000.0,
        fee_usd_equiv=0.5,
    )
    await ex._process_execution(execution)
    assert len(published_fills) == 0


@pytest.mark.asyncio
async def test_process_execution_duplicate_fill_idempotent() -> None:
    """Test duplicate filled execution is idempotent (no crash, no double publish).

    Given: An executor that already processed and removed an order,
    When: Duplicate filled execution arrives for the same exchange_order_id,
    Then: No crash, no fill published (maps already empty).
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    published_fills: list[Any] = []

    async def track_publish_execution(topic: str, fill: Any) -> None:
        published_fills.append(fill)

    ex._publish_execution = track_publish_execution
    execution = SimpleNamespace(
        order_id="already_filled_ex1",
        exec_type="filled",
        exec_id="trade-dup",
        order_status=None,
        cum_qty=1.0,
        average_price=50000.0,
        fee_usd_equiv=0.5,
    )
    await ex._process_execution(execution)
    assert len(published_fills) == 0
    assert "already_filled_ex1" not in ex.client_by_exchange


@pytest.mark.asyncio
async def test_process_execution_orphan_buffering_and_replay() -> None:
    """Test orphan execution buffering and replay on ACK.

    Given: An execution arrives before ACK (mapping not yet established),
    When: ACK arrives and mapping is created,
    Then: Buffered orphan is replayed and processed.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    order = make_order()
    ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
    published_fills: list[Any] = []

    async def track_publish_execution(topic: str, fill: Any) -> None:
        published_fills.append(fill)

    ex._publish_execution = track_publish_execution
    execution = SimpleNamespace(
        order_id="fast_exchange_id",
        exec_type="filled",
        exec_id="trade-fast",
        order_status=None,
        cum_qty=1.0,
        average_price=50000.0,
        fee_usd_equiv=0.5,
    )
    await ex._process_execution(execution)
    assert len(published_fills) == 0
    assert "fast_exchange_id" in ex.orphaned_executions
    ex.client_by_exchange["fast_exchange_id"] = order.client_order_id
    ex._try_process_orphaned("fast_exchange_id", order.client_order_id)
    await asyncio.sleep(0.01)
    assert "fast_exchange_id" not in ex.orphaned_executions


@pytest.mark.asyncio
async def test_orphan_ttl_expiry_increments_drop_count() -> None:
    """Test orphan executions expire and increment drop counter.

    Given: An orphaned execution buffered past TTL,
    When: _cleanup_expired_orphans is called,
    Then: Orphan is removed and drop counter incremented.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.orphan_ttl_seconds = 0.1
    ex.orphaned_executions["old_ex"] = (SimpleNamespace(order_id="old_ex"), time_module.monotonic())
    initial_count = ex.orphan_drop_count
    with mock_patch("time.monotonic", return_value=time_module.monotonic() + 1.0):
        ex._cleanup_expired_orphans()
    assert "old_ex" not in ex.orphaned_executions
    assert ex.orphan_drop_count == initial_count + 1


@pytest.mark.asyncio
async def test_orphan_duplicate_updates_timestamp_without_relogging() -> None:
    """Test duplicate orphan updates refresh timestamp but do not re-log.

    Given: An orphan execution already buffered,
    When: Another execution with the same order_id arrives,
    Then: Timestamp is refreshed but the entry is not re-logged (already_buffered check).
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.orphan_ttl_seconds = 5.0
    initial_time = time_module.monotonic()
    ex.orphaned_executions["dup_ex"] = (SimpleNamespace(order_id="dup_ex"), initial_time)
    new_execution = SimpleNamespace(
        order_id="dup_ex",
        exec_type="trade",
        order_status=None,
        cum_qty=2.0,
        average_price=100.0,
        fee_usd_equiv=0.0,
        exec_id="trade-dup",
    )
    await ex._process_execution(new_execution)
    assert "dup_ex" in ex.orphaned_executions
    stored_execution, stored_time = ex.orphaned_executions["dup_ex"]
    assert stored_execution.cum_qty == pytest.approx(2.0)
    assert stored_time >= initial_time


@pytest.mark.asyncio
async def test_process_execution_exception_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution processing keeps order on publish error.

    Given: An executor with pending order, mapping, and failing _publish_execution,
    When: Execution update is processed,
    Then: Order remains in pending_orders after exception.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    order = make_order()
    ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
    ex.client_by_exchange["ex1"] = order.client_order_id
    ex._publish_execution = AsyncMock(side_effect=RuntimeError("fail"))
    execution = SimpleNamespace(
        order_id="ex1",
        exec_type="trade",
        order_status=None,
        cum_qty=1.0,
        average_price=1.0,
        fee_usd_equiv=0.0,
    )
    await ex._process_execution(execution)
    assert order.client_order_id in ex.pending_orders


@pytest.mark.asyncio
async def test_publish_heartbeat_skips_when_not_running() -> None:
    """Test heartbeat is not published when not running.

    Given: An executor that is not running,
    When: _publish_heartbeat is called,
    Then: No message is sent.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = False
    ex.msg_publisher = SimpleNamespace(send=AsyncMock())
    msg = SimpleNamespace(to_json=lambda: "{}", component="c")
    await ex._publish_heartbeat("heartbeat.test", cast(Any, msg))
    ex.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_loop_handles_publish_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test heartbeat loop handles publish errors.

    Given: A running executor with failing _publish_heartbeat,
    When: Heartbeat loop runs,
    Then: Error is handled and loop continues until stopped.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.settings = SimpleNamespace(
        zmq_heartbeat_interval_ms=0,
        zmq_broker_xsub="xsub",
        zmq_broker_xpub="xpub",
    )
    ex._publish_heartbeat = AsyncMock(side_effect=RuntimeError("send fail"))
    task = asyncio.create_task(ex._heartbeat_loop())
    await asyncio.sleep(0.02)
    ex.running = False
    await task


def test_stop_closes_resources() -> None:
    """Test stop properly closes all resources.

    Given: A running executor with subscriber, publisher, and context,
    When: stop() is called,
    Then: All resources are closed and LINGER is set to 0.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    closed = {"sub": False, "pub": False, "ctx": False}
    sub_setsockopt: list[tuple[int, int]] = []
    pub_setsockopt: list[tuple[int, int]] = []
    ex.subscriber = zmq_socket_stub(
        track_calls=sub_setsockopt, close=lambda: closed.__setitem__("sub", True)
    )
    ex.publisher = zmq_socket_stub(
        track_calls=pub_setsockopt, close=lambda: closed.__setitem__("pub", True)
    )

    class Ctx:
        def term(self) -> None:
            closed["ctx"] = True

    ex.context = Ctx()
    asyncio.run(ex.stop())
    assert closed["sub"] and closed["pub"] and closed["ctx"]
    assert sub_setsockopt == [(zmq.LINGER, 0)]
    assert pub_setsockopt == [(zmq.LINGER, 0)]


class TestExecutorNonWebSocketMode:
    """Tests for executor without websocket support."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_without_websocket_support(self, mock_get_settings: MagicMock) -> None:
        """Verify executor starts without websocket support.

        Given: An executor with non-websocket client,
        When: start is called,
        Then: Executor runs with order handler only.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = MagicMock()
        mock_exchange_client.supports_websocket_executions = False
        mock_exchange_client.__aenter__ = AsyncMock(return_value=mock_exchange_client)
        mock_exchange_client.__aexit__ = AsyncMock(return_value=None)
        start_called = asyncio.Event()

        async def mock_order_handler() -> None:
            start_called.set()
            await asyncio.sleep(0.5)

        with (
            patch.object(service, "_order_handler", side_effect=mock_order_handler),
            patch.object(service, "_heartbeat_loop", new=AsyncMock()),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch(
                "snapper.application.services.settings.get_settings_service",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "snapper.config.settings.get_settings_with_service",
                return_value=mock_settings,
            ),
            patch("zmq.asyncio.Context"),
        ):
            task = asyncio.create_task(service_any.start())
            try:
                await asyncio.wait_for(start_called.wait(), timeout=2.0)
                assert service_any.running is True
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task


class TestOrderHandlerEdgeCases:
    """Tests for order handler edge cases."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_wrong_exchange(self, mock_get_settings: MagicMock) -> None:
        """Verify order handler ignores wrong exchange.

        Given: An order for different exchange,
        When: Order handler processes it,
        Then: Order is skipped.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv_multipart() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "orders.commands.binance.BTC-USD.submit",
                    (
                        b'{"type":"order_request","session_id":"","sequence_id":0,'
                        b'"strategy_id":"test","exchange":"binance",'
                        b'"instrument":"BTC-USD","mode":"paper","side":"buy","order_type":"market",'
                        b'"quantity":0.1,"client_order_id":"test123"}'
                    ),
                )
            service_any.running = False
            await asyncio.sleep(0.1)
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv_multipart
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        assert recv_count >= 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_non_order_message(self, mock_get_settings: MagicMock) -> None:
        """Verify order handler handles non-order messages.

        Given: A non-order message type,
        When: Order handler processes it,
        Then: Message is skipped without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv_multipart() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "orders.commands.kraken.BTC-USD.submit",
                    b'{"type":"heartbeat","session_id":"","sequence_id":0,"timestamp":"2024-01-01T00:00:00Z"}',
                )
            service_any.running = False
            await asyncio.sleep(0.1)
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv_multipart
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        assert recv_count >= 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_unexpected_topic(self, mock_get_settings: MagicMock) -> None:
        """Verify order handler ignores unexpected topics.

        Given: Subscriber receiving messages with unknown topic,
        When: Order handler processes the message,
        Then: Handler continues without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv_multipart() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "unknown.topic",
                    b'{"type":"order_request","session_id":"","sequence_id":0}',
                )
            service_any.running = False
            await asyncio.sleep(0.1)
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv_multipart
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        assert recv_count >= 1


class TestExecutionHandler:
    """Tests for execution handler functionality."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_not_implemented(self, mock_get_settings: MagicMock) -> None:
        """Verify execution handler handles NotImplementedError.

        Given: Exchange client with unimplemented subscribe_executions,
        When: Execution handler runs,
        Then: NotImplementedError is caught gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_exchange_client = AsyncMock()

        async def raise_not_implemented() -> Any:
            for _ in []:
                yield
            raise NotImplementedError("subscribe_executions not supported")

        mock_exchange_client.subscribe_executions = raise_not_implemented
        service_any.exchange_client = mock_exchange_client
        await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_error(self, mock_get_settings: MagicMock) -> None:
        """Verify execution handler handles connection errors.

        Given: Exchange client that raises error,
        When: Execution handler runs,
        Then: Error is handled gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_exchange_client = AsyncMock()

        async def raise_error() -> Any:
            for _ in []:
                yield
            raise RuntimeError("Connection lost")

        mock_exchange_client.subscribe_executions = raise_error
        service_any.exchange_client = mock_exchange_client
        await service_any._execution_handler()


class TestExecuteLiveOrderErrors:
    """Tests for live order execution errors."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    def _create_order(self, **overrides: Any) -> OrderRequestData:
        base: dict[str, Any] = {
            "strategy_id": "test_strategy",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.1,
            "client_order_id": "test_order_123",
        }
        base.update(overrides)
        return OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            **base,
        )

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_exception(self, mock_get_settings: MagicMock) -> None:
        """Verify live order execution handles API exception.

        Given: Exchange client that raises exception,
        When: _execute_live_order is called,
        Then: Returns None without crashing.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = AsyncMock()
        mock_exchange_client.create_order = AsyncMock(side_effect=Exception("API error"))
        service_any.exchange_client = mock_exchange_client
        order = self._create_order()
        result = await service_any._execute_live_order(order)
        assert result is None

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_no_order_id(self, mock_get_settings: MagicMock) -> None:
        """Verify live order execution handles null order id.

        Given: Exchange client that returns None,
        When: _execute_live_order is called,
        Then: Returns None.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = AsyncMock()
        mock_exchange_client.create_order = AsyncMock(return_value=None)
        service_any.exchange_client = mock_exchange_client
        order = self._create_order()
        result = await service_any._execute_live_order(order)
        assert result is None


class TestProcessOrder:
    """Tests for order processing functionality."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    def _create_order(self, **overrides: Any) -> OrderRequestData:
        base: dict[str, Any] = {
            "strategy_id": "test_strategy",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.1,
            "client_order_id": "test_order_123",
        }
        base.update(overrides)
        return OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            **base,
        )

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_order_exception_publishes_rejection(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify order processing publishes rejection on exception.

        Given: Order with failing execute_live_order call,
        When: Order is processed,
        Then: Order status 'rejected' is published (no fill for syntactic rejection).
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        published_statuses: list[tuple[Any, str]] = []

        async def track_publish_execution(topic: str, fill: Any) -> None:
            """Intentionally empty async stub for testing."""
            pass

        async def track_publish_order_status(order: Any, status: str) -> None:
            published_statuses.append((order, status))

        service_any._publish_execution = track_publish_execution
        service_any._publish_order_status = track_publish_order_status
        service_any._execute_live_order = AsyncMock(side_effect=Exception("Order execution failed"))
        order = self._create_order()
        await service_any._process_order(order)
        assert any(status == "rejected" for _, status in published_statuses)


class TestProcessExecution:
    """Tests for execution processing functionality."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_unknown_order(self, mock_get_settings: MagicMock) -> None:
        """Verify execution processing handles unknown order.

        Given: Execution for order not in pending orders,
        When: _process_execution is called,
        Then: Handles gracefully without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.pending_orders = {}
        execution = ExecutionUpdate(
            order_id="unknown_order_123",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.MARKET,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
        )
        await service_any._process_execution(execution)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_cancelled_cleans_maps_no_fill(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify cancelled execution cleans maps but publishes no fill.

        Given: Pending order with cancellation execution update,
        When: Execution is processed,
        Then: No fill is published but maps are cleaned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="order_123",
        )
        service_any.pending_orders = {
            order.client_order_id: base_module.PendingOrderState(request=order)
        }
        service_any.client_by_exchange = {"exchange_order_456": order.client_order_id}
        published_fills: list[Any] = []

        async def track_publish_execution(topic: str, fill: Any) -> None:
            published_fills.append(fill)

        service_any._publish_execution = track_publish_execution
        execution = ExecutionUpdate(
            order_id="exchange_order_456",
            exec_type="canceled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.MARKET,
            order_status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
        )
        await service_any._process_execution(execution)
        assert len(published_fills) == 0
        assert order.client_order_id not in service_any.pending_orders
        assert "exchange_order_456" not in service_any.client_by_exchange

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_expired_cleans_maps_no_fill(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify expired execution cleans maps but publishes no fill.

        Given: Pending order with expired execution update,
        When: Execution is processed,
        Then: No fill is published but maps are cleaned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=0.1,
            price=50000.0,
            client_order_id="order_expired_123",
        )
        service_any.pending_orders = {
            order.client_order_id: base_module.PendingOrderState(request=order)
        }
        service_any.client_by_exchange = {"exchange_order_789": order.client_order_id}
        published_fills: list[Any] = []

        async def track_publish_execution(topic: str, fill: Any) -> None:
            published_fills.append(fill)

        service_any._publish_execution = track_publish_execution
        execution = ExecutionUpdate(
            order_id="exchange_order_789",
            exec_type="expired",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
        )
        await service_any._process_execution(execution)
        assert len(published_fills) == 0
        assert order.client_order_id not in service_any.pending_orders
        assert "exchange_order_789" not in service_any.client_by_exchange

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_partial(self, mock_get_settings: MagicMock) -> None:
        """Verify partial execution is processed correctly.

        Given: Pending order with partial fill execution update,
        When: Execution is processed,
        Then: Partial fill is published and order remains pending.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=1.0,
            price=50000.0,
            client_order_id="order_123",
        )
        service_any.pending_orders = {
            order.client_order_id: base_module.PendingOrderState(request=order)
        }
        service_any.client_by_exchange = {"exchange_order_456": order.client_order_id}
        published_fills: list[Any] = []

        async def track_publish_execution(topic: str, fill: Any) -> None:
            published_fills.append(fill)

        service_any._publish_execution = track_publish_execution
        execution = ExecutionUpdate(
            order_id="exchange_order_456",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50100.0,
        )
        await service_any._process_execution(execution)
        assert len(published_fills) == 1
        assert published_fills[0].status == "partial"
        assert order.client_order_id in service_any.pending_orders


class TestHeartbeat:
    """Tests for heartbeat functionality."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_heartbeat_no_publisher(self, mock_get_settings: MagicMock) -> None:
        """Verify heartbeat publishing handles missing publisher.

        Given: Service with no publisher configured,
        When: Heartbeat is published,
        Then: No error is raised.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.msg_publisher = None
        service_any.running = True
        hb = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="test",
            sequence=1,
            status="healthy",
            lag_ms=0,
        )
        await service_any._publish_heartbeat("heartbeat.test", hb)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_heartbeat_error_handling(self, mock_get_settings: MagicMock) -> None:
        """Verify heartbeat publishing handles send errors.

        Given: Publisher that raises exception on send,
        When: Heartbeat is published,
        Then: Error is caught and handled gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock(side_effect=Exception("Send failed"))
        service_any.msg_publisher = mock_msg_publisher
        hb = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="test",
            sequence=1,
            status="healthy",
            lag_ms=0,
        )
        await service_any._publish_heartbeat("heartbeat.test", hb)


class TestSymbolAliasUpdate:
    """Tests for symbol alias update handling."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_symbol_alias_update_error(self, mock_get_settings: MagicMock) -> None:
        """Verify symbol alias update handles invalid JSON.

        Given: Invalid JSON payload,
        When: Symbol alias update is handled,
        Then: Error is caught gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._handle_symbol_alias_update("not valid json {{{")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_symbol_alias_update_success(self, mock_get_settings: MagicMock) -> None:
        """Verify symbol alias update triggers cache invalidation.

        Given: Valid symbol alias update payload,
        When: Update is handled,
        Then: Cache invalidation is triggered on mapper service.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        payload = json.dumps(
            {
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
                "event": "symbol_aliases_updated",
                "action": "clear_cache",
            }
        )
        with patch(
            "snapper.messaging.executors.base.SymbolMapperService.get_instance"
        ) as mock_mapper:
            mock_instance = MagicMock()
            mock_mapper.return_value = mock_instance
            service_any._handle_symbol_alias_update(payload)
            mock_instance.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)


class TestSettingsUpdate:
    """Tests for settings update handling."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_settings_update_error(self, mock_get_settings: MagicMock) -> None:
        """Verify settings update handles invalid JSON.

        Given: Invalid JSON payload,
        When: Settings update is handled,
        Then: Error is caught gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._handle_settings_update("not valid json {{{")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_settings_update_success(self, mock_get_settings: MagicMock) -> None:
        """Verify settings update updates cache correctly.

        Given: Valid settings update payload,
        When: Update is handled,
        Then: Settings cache is updated with new value.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        envelope = SettingChangedData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            key="test_key",
            value="test_value",
            category="test",
        )
        payload = envelope.to_json()
        with patch("snapper.messaging.executors.base.SettingsService.get_instance") as mock_service:
            mock_instance = MagicMock()
            mock_instance._parse_value.return_value = "test_value"
            mock_instance._cache = {}
            mock_service.return_value = mock_instance
            service_any._handle_settings_update(payload)
            mock_instance._parse_value.assert_called_once_with("test_value")
            assert mock_instance._cache["test_key"] == "test_value"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_settings_update_no_instance(self, mock_get_settings: MagicMock) -> None:
        """Verify settings update handles missing service instance.

        Given: Settings service returning None for get_instance,
        When: Update is handled,
        Then: No error is raised.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        envelope = SettingChangedData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            key="test_key",
            value="test_value",
            category="test",
        )
        payload = envelope.to_json()
        with patch("snapper.messaging.executors.base.SettingsService.get_instance") as mock_service:
            mock_service.return_value = None
            service_any._handle_settings_update(payload)


class TestPublishOrderStatus:
    """Tests for order status publishing."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_order_status_no_publisher(self, mock_get_settings: MagicMock) -> None:
        """Verify order status publishing handles missing publisher.

        Given: Service with no publisher configured,
        When: Order status is published,
        Then: No error is raised.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.msg_publisher = None
        service_any.running = True
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="order_123",
        )
        await service_any._publish_order_status(order, "submitted")


class TestExecutionHandlerEdgeCases:
    """Tests for execution handler edge cases."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_websocket_unsupported(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify execution handler handles unsupported websocket.

        Given: Exchange client without websocket execution support,
        When: Execution handler runs,
        Then: Handler returns without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_client = MagicMock()
        mock_client.supports_websocket_executions = False
        service_any.exchange_client = mock_client
        await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_not_implemented(self, mock_get_settings: MagicMock) -> None:
        """Verify execution handler catches NotImplementedError.

        Given: Exchange client with NotImplementedError in subscribe_executions,
        When: Execution handler runs,
        Then: Error is caught gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_client = MagicMock()
        mock_client.supports_websocket_executions = True

        async def raising_generator() -> Any:
            for _ in []:
                yield
            raise NotImplementedError("subscribe_executions not supported")

        mock_client.subscribe_executions = raising_generator
        service_any.exchange_client = mock_client
        await service_any._execution_handler()


class MockExchangeClientNoAenter:
    """Test stub for exchange client without context manager."""

    supports_websocket_executions = False

    async def get_balance(self) -> dict[str, float]:
        """Get balance stub.

        Returns:
            Test balance dictionary.
        """
        return {"USD": 10000.0}

    async def create_order(self, request: Any) -> str:
        """Create order stub.

        Args:
            request: Order request.

        Returns:
            Test order ID.
        """
        return "order_123"


class MockExchangeClientWithAenter:
    """Test stub for exchange client with context manager."""

    supports_websocket_executions = True

    def __init__(self) -> None:
        """Initialize client stub."""
        self._entered = False

    def set_tracker(self, tracker: Any) -> None:
        """No-op tracker injection for tests."""

    async def __aenter__(self) -> MockExchangeClientWithAenter:
        """Enter async context manager.

        Returns:
            Self reference.
        """
        self._entered = True
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager.

        Args:
            exc_type: Exception type if raised.
            exc_val: Exception value if raised.
            exc_tb: Exception traceback if raised.
        """
        self._entered = False

    async def get_balance(self) -> dict[str, float]:
        """Get balance stub.

        Returns:
            Test balance dictionary.
        """
        return {"USD": 10000.0}

    async def create_order(self, request: Any) -> str:
        """Create order stub.

        Args:
            request: Order request.

        Returns:
            Test order ID.
        """
        return "order_123"

    async def subscribe_executions(self) -> Any:
        """Subscribe to executions stub.

        Yields:
            Nothing (stub generator).
        """
        if False:
            yield


class ConcreteTestExecutor(ExchangeExecutorService[ExchangeClientBase]):
    """Test implementation of ExchangeExecutorService."""

    def __init__(self, exchange_client: Any) -> None:
        """Initialize test executor.

        Args:
            exchange_client: Mock exchange client to use.
        """
        super().__init__()
        self._mock_client = exchange_client

    def _create_exchange_client(self) -> ExchangeClientBase:
        """Create exchange client.

        Returns:
            The mock client provided during initialization.
        """
        return cast(ExchangeClientBase, self._mock_client)

    def _get_exchange_name(self) -> Literal["kraken", "zonda", "walutomat", "paper"]:
        """Get exchange name.

        Returns:
            Exchange name string.
        """
        return "kraken"


class TestStartWithAsyncContextManager:
    """Tests for start with async context manager."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_with_aenter_client_cancelled(self, mock_get_settings: MagicMock) -> None:
        """Verify start handles CancelledError with context manager.

        Given: Exchange client with async context manager,
        When: Order handler raises CancelledError,
        Then: Context manager is properly exited.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_client = MockExchangeClientWithAenter()
        service = ConcreteTestExecutor(mock_client)
        call_count = 0

        async def mock_order_handler() -> None:
            nonlocal call_count
            call_count += 1
            raise asyncio.CancelledError()

        with (
            patch.object(service, "_order_handler", side_effect=mock_order_handler),
            patch.object(service, "_heartbeat_loop", new=AsyncMock()),
            patch.object(service, "_execution_handler", new=AsyncMock()),
            patch(
                "snapper.messaging.executors.base.get_repository",
                return_value=MagicMock(),
            ),
            patch(
                "snapper.messaging.executors.base.get_settings_service",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "snapper.messaging.executors.base.get_settings_with_service",
                return_value=mock_settings,
            ),
            patch("zmq.asyncio.Context"),
            pytest.raises(asyncio.CancelledError),
        ):
            await service.start()
        assert call_count == 1
        assert mock_client._entered is False


class TestOrderHandlerWrongExchange:
    """Tests for order handler with wrong exchange."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_wrong_exchange_logs_warning(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify order handler warns on wrong exchange.

        Given: Order destined for different exchange,
        When: Order handler processes it,
        Then: Order is ignored with warning.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_client = MockExchangeClientNoAenter()
        service = ConcreteTestExecutor(mock_client)
        service_any = cast(Any, service)
        service_any.running = True
        recv_count = 0

        async def mock_recv_multipart() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "orders.commands.kraken.BTC-USD.submit",
                    (
                        b'{"type":"order_request","session_id":"","sequence_id":0,'
                        b'"strategy_id":"test","exchange":"binance",'
                        b'"instrument":"BTC-USD","mode":"paper","side":"buy","order_type":"market",'
                        b'"quantity":0.1,"client_order_id":"test123"}'
                    ),
                )
            service_any.running = False
            await asyncio.sleep(0.01)
            return ("", b"")

        mock_subscriber = AsyncMock()
        mock_subscriber.recv_multipart = mock_recv_multipart
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        assert recv_count >= 1


class TestExecutionHandlerNotImplementedError:
    """Tests for execution handler NotImplementedError."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_catches_not_implemented(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify execution handler catches NotImplementedError.

        Given: Exchange not supporting execution streaming,
        When: Execution handler runs,
        Then: NotImplementedError is caught without crash.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_client = MockExchangeClientNoAenter()
        service = ConcreteTestExecutor(mock_client)
        service_any = cast(Any, service)
        service_any.running = True

        async def subscribe_raises_not_implemented() -> Any:
            for _ in []:
                yield
            raise NotImplementedError("Exchange doesn't support execution streaming")

        mock_exchange = MagicMock()
        mock_exchange.supports_websocket_executions = True
        mock_exchange.subscribe_executions = subscribe_raises_not_implemented
        service_any.exchange_client = mock_exchange
        await service_any._execution_handler()


class TestExecutionHandlerGeneralException:
    """Tests for execution handler general exceptions."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_logs_error_when_running(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify execution handler logs error when running.

        Given: Service running with failing execution subscription,
        When: RuntimeError is raised,
        Then: Error is logged.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_client = MockExchangeClientNoAenter()
        service = ConcreteTestExecutor(mock_client)
        service_any = cast(Any, service)
        service_any.running = True

        async def subscribe_raises_runtime_error() -> Any:
            for _ in []:
                yield
            raise RuntimeError("Connection lost")

        mock_exchange = MagicMock()
        mock_exchange.supports_websocket_executions = True
        mock_exchange.subscribe_executions = subscribe_raises_runtime_error
        service_any.exchange_client = mock_exchange
        await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_silent_when_not_running(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify execution handler silent when not running.

        Given: Service not running,
        When: RuntimeError is raised,
        Then: Error is silently ignored.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_client = MockExchangeClientNoAenter()
        service = ConcreteTestExecutor(mock_client)
        service_any = cast(Any, service)
        service_any.running = False

        async def subscribe_raises_runtime_error() -> Any:
            for _ in []:
                yield
            raise RuntimeError("Connection lost")

        mock_exchange = MagicMock()
        mock_exchange.supports_websocket_executions = True
        mock_exchange.subscribe_executions = subscribe_raises_runtime_error
        service_any.exchange_client = mock_exchange
        await service_any._execution_handler()


class DummyExecutorSimple(ExchangeExecutorService[Any]):
    """Simple test stub for exchange executor."""

    def __init__(self) -> None:
        """Initialize simple executor stub."""
        super().__init__()

    def _create_exchange_client(self) -> Any:
        """Create simple exchange client.

        Returns:
            SimpleNamespace stub.
        """
        return SimpleNamespace()

    def _get_exchange_name(self) -> Literal["paper"]:
        """Get exchange name.

        Returns:
            Paper exchange name.
        """
        return "paper"


class SocketStub:
    """Test stub for ZMQ socket."""

    def __init__(self) -> None:
        """Initialize socket stub."""
        self.closed = False

    def close(self) -> None:
        """Close the socket."""
        self.closed = True


class ContextStub:
    """Test stub for ZMQ context."""

    def __init__(self) -> None:
        """Initialize context stub."""
        self.terminated = False

    def term(self) -> None:
        """Terminate the context."""
        self.terminated = True


class SubscriberStub:
    """Test stub for ZMQ subscriber."""

    def __init__(self, topic: str, payload: bytes) -> None:
        """Initialize subscriber stub.

        Args:
            topic: Topic to return.
            payload: Payload to return.
        """
        self._topic = topic
        self._payload = payload
        self.calls = 0

    async def recv_multipart(self) -> tuple[str, bytes]:
        """Receive multipart message stub.

        Returns:
            Topic and payload tuple.
        """
        self.calls += 1
        return self._topic, self._payload


@pytest.mark.asyncio
async def test_stop_returns_when_not_running() -> None:
    """Test stop returns immediately when not running.

    Given: An executor that is not running,
    When: stop() is called,
    Then: Running state remains False.
    """
    executor = DummyExecutorSimple()
    await executor.stop()
    assert executor.running is False


@pytest.mark.asyncio
async def test_stop_closes_resources_simple() -> None:
    """Test stop closes all resources properly.

    Given: A running executor with sockets and context,
    When: stop() is called,
    Then: All sockets are closed and context is terminated.
    """
    executor = DummyExecutorSimple()
    executor.running = True
    sub_socket = SocketStub()
    pub_socket = SocketStub()
    ctx = ContextStub()
    executor.subscriber = cast(Any, zmq_socket_stub(close=sub_socket.close))
    executor.publisher = cast(Any, zmq_socket_stub(close=pub_socket.close))
    executor.context = cast(Any, ctx)
    await executor.stop()
    assert sub_socket.closed is True
    assert pub_socket.closed is True
    assert ctx.terminated is True
    assert executor.running is False


@pytest.mark.asyncio
async def test_order_handler_processes_order_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler processes orders correctly.

    Given: A running executor with subscriber containing order message,
    When: _order_handler processes the message,
    Then: Order is passed to _process_order.
    """
    executor = DummyExecutorSimple()
    executor.running = True
    order = OrderRequestData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        strategy_id="s1",
        exchange="paper",
        instrument="BTC-USD",
        mode="paper",
        side="buy",
        quantity=1.0,
        client_order_id="c1",
        order_type="market",
    )
    payload = order.to_json().encode("utf-8")
    subscriber = SubscriberStub(
        f"orders.commands.{executor._get_exchange_name()}.BTC-USD.submit",
        payload,
    )
    executor.subscriber = cast(Any, subscriber)
    processed: list[OrderRequestData] = []

    async def fake_process(order_msg: OrderRequestData) -> None:
        processed.append(order_msg)
        executor.running = False

    monkeypatch.setattr(executor, "_process_order", fake_process)
    monkeypatch.setattr("snapper.messaging.executors.base.parse_message", lambda data: order)
    await asyncio.wait_for(executor._order_handler(), timeout=0.1)
    assert processed == [order]
    assert subscriber.calls == 1


@pytest.mark.asyncio
async def test_order_handler_logs_error_and_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test order handler logs errors and exits.

    Given: A running executor with failing subscriber,
    When: recv_multipart raises error,
    Then: Error is logged.
    """
    executor = DummyExecutorSimple()
    executor.running = True

    class FailingSubscriber:
        async def recv_multipart(self) -> tuple[str, bytes]:
            raise RuntimeError("boom")

    executor.subscriber = cast(Any, FailingSubscriber())
    errors: list[str] = []

    def fake_error(message: str) -> None:
        errors.append(message)
        executor.running = False

    monkeypatch.setattr("snapper.messaging.executors.base.logger.error", fake_error)
    await asyncio.wait_for(executor._order_handler(), timeout=0.1)
    assert errors


@pytest.mark.asyncio
async def test_execution_handler_skips_when_ws_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution handler skips when websocket unsupported.

    Given: An executor with client not supporting websocket executions,
    When: _execution_handler is called,
    Then: Warning is logged.
    """
    executor = DummyExecutorSimple()
    executor.exchange_client = SimpleNamespace(supports_websocket_executions=False)
    warnings: list[str] = []
    monkeypatch.setattr(
        "snapper.messaging.executors.base.logger.warning", lambda message: warnings.append(message)
    )
    await executor._execution_handler()
    assert warnings


@pytest.mark.asyncio
async def test_execution_handler_breaks_when_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution handler breaks loop when not running.

    Given: An executor that is not running,
    When: Execution messages are yielded,
    Then: Messages are not processed.
    """
    executor = DummyExecutorSimple()
    executor.running = False

    async def generator() -> Any:
        yield "msg"

    class Client:
        supports_websocket_executions = True

        def __init__(self) -> None:
            self.calls = 0

        async def subscribe_executions(self) -> Any:
            async for item in generator():
                self.calls += 1
                yield item

    client = Client()
    executor.exchange_client = client
    called: list[str] = []

    async def fail_if_called(message: Any) -> None:
        called.append("processed")

    monkeypatch.setattr(executor, "_process_execution", fail_if_called)
    await executor._execution_handler()
    assert client.calls == 1
    assert called == []


@pytest.mark.asyncio
async def test_execution_handler_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution handler processes messages.

    Given: A running executor with websocket client,
    When: Execution message is yielded,
    Then: Message is passed to _process_execution.
    """
    executor = DummyExecutorSimple()
    executor.running = True

    class Client:
        supports_websocket_executions = True

        def __init__(self) -> None:
            self.calls = 0

        async def subscribe_executions(self) -> Any:
            self.calls += 1
            yield "msg"

    client = Client()
    executor.exchange_client = client
    processed: list[Any] = []

    async def record_execution(message: Any) -> None:
        processed.append(message)
        executor.running = False

    monkeypatch.setattr(executor, "_process_execution", record_execution)
    await executor._execution_handler()
    assert processed == ["msg"]
    assert client.calls == 1


@pytest.mark.asyncio
async def test_execution_handler_logs_error_when_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution handler logs errors when running.

    Given: A running executor with client raising error,
    When: subscribe_executions raises ValueError,
    Then: Error is logged.
    """
    executor = DummyExecutorSimple()
    executor.running = True

    class Client:
        supports_websocket_executions = True

        def subscribe_executions(self) -> Any:
            class FailingGen:
                def __aiter__(self) -> Any:
                    return self

                async def __anext__(self) -> Any:
                    raise ValueError("ws error")

            return FailingGen()

    executor.exchange_client = Client()
    errors: list[str] = []
    monkeypatch.setattr(
        "snapper.messaging.executors.base.logger.error",
        lambda message: errors.append(message),
    )
    await executor._execution_handler()
    assert errors


class TestExecutorCoverage:
    """Tests for executor coverage scenarios."""

    def _create_mock_settings(self) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    def _create_order(self, **overrides: Any) -> OrderRequestData:
        base_payload: dict[str, Any] = {
            "strategy_id": "test_strategy",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "paper",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.1,
            "client_order_id": "test_order_123",
        }
        base_payload.update(overrides)
        return OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            **base_payload,
        )

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_success_paper(self, mock_get_settings: MagicMock) -> None:
        """Verify live order execution success in paper mode.

        Given: Valid order request in paper mode,
        When: Order is executed,
        Then: Exchange order ID is returned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = AsyncMock()
        service_any.exchange_client = mock_exchange_client
        order = self._create_order(mode="paper")
        exchange_order = ExchangeOrderSnapshot(
            id="exchange_123",
            client_order_id="test_order_123",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=0.1,
            price=None,
            status=OrderStatusEnum.OPEN,
            filled=0.0,
            remaining=0.1,
            timestamp=datetime.now(UTC).timestamp(),
        )
        mock_exchange_client.create_order = AsyncMock(return_value=exchange_order)
        result = await service_any._execute_live_order(order)
        mock_exchange_client.create_order.assert_awaited_once()
        assert result == "exchange_123"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_execution(self, mock_get_settings: MagicMock) -> None:
        """Verify fill message is published successfully.

        Given: Service running with publisher configured,
        When: Fill message is published,
        Then: Publisher sends multipart message.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_publisher = AsyncMock()
        service_any.msg_publisher = mock_publisher
        service_any.running = True
        fill_msg = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-1",
            exchange_order_id="exchange_123",
            client_order_id="test_order_123",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.1,
            price=50000.0,
            last_size=0.1,
            last_price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        await service_any._publish_execution("orders.events.kraken.BTC-USD.executed", fill_msg)
        mock_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_execution_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify fill publishing skipped when not running.

        Given: Service not running,
        When: Fill message is published,
        Then: No message is sent.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = False
        fill_msg = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-1",
            exchange_order_id="test_order_123",
            client_order_id="test_order_123",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.1,
            price=50000.0,
            last_size=0.1,
            last_price=50000.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        await service_any._publish_execution("orders.events.kraken.BTC-USD.executed", fill_msg)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify get_status returns correct state.

        Given: Initialized executor service,
        When: get_status is called,
        Then: Status dict contains running and broker info.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        status = service.get_status()
        assert "running" in status
        assert status["running"] is False
        assert "broker_xpub" in status

    @pytest.mark.asyncio
    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    async def test_start_initializes_sockets(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """Verify start initializes ZMQ sockets.

        Given: Valid configuration settings,
        When: Service is started,
        Then: SUB and PUB sockets are created.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        mock_settings_service = MagicMock()
        mock_settings_with_db = self._create_mock_settings()
        mock_settings_with_db.kraken_api_key = "test_api_key"
        mock_settings_with_db.kraken_api_secret = "test_api_secret"
        mock_exchange_client = MagicMock()
        mock_exchange_client.__aenter__ = AsyncMock(return_value=mock_exchange_client)
        mock_exchange_client.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(service, "_order_handler", new=AsyncMock(return_value=None)),
            patch.object(service, "_execution_handler", new=AsyncMock(return_value=None)),
            patch.object(service, "_heartbeat_loop", new=AsyncMock(return_value=None)),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch(
                "snapper.application.services.settings.get_settings_service",
                new=AsyncMock(return_value=mock_settings_service),
            ),
            patch(
                "snapper.config.settings.get_settings_with_service",
                return_value=mock_settings_with_db,
            ),
        ):
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            with (
                patch(
                    "snapper.messaging.executors.base.zmq.asyncio.Context",
                    return_value=mock_context,
                ),
                patch(
                    "snapper.application.services.settings.zmq.asyncio.Context",
                    return_value=mock_context,
                ),
            ):
                await service.start()
        assert mock_context.socket.call_count >= 2
        socket_calls = [call[0][0] for call in mock_context.socket.call_args_list]
        assert zmq.SUB in socket_calls
        assert zmq.PUB in socket_calls

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_stop_cleans_up(self, mock_get_settings: MagicMock) -> None:
        """Verify stop cleans up resources.

        Given: Running service with sockets and context,
        When: stop() is called,
        Then: Sockets closed and context terminated.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service.context = MagicMock()
        mock_subscriber = MagicMock()
        service.subscriber = mock_subscriber
        mock_publisher = MagicMock()
        service.publisher = mock_publisher
        service.running = True
        await service.stop()
        assert service.running is False
        mock_subscriber.close.assert_called_once()
        mock_publisher.close.assert_called_once()
        service.context.term.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_order_status(self, mock_get_settings: MagicMock) -> None:
        """Verify order status is published.

        Given: Running service with publisher,
        When: Order status is published,
        Then: Publisher sends multipart message.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = AsyncMock()
        order = self._create_order()
        await service_any._publish_order_status(order, "submitted")
        service_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_success(self, mock_get_settings: MagicMock) -> None:
        """Verify live order execution success.

        Given: Valid order request in live mode,
        When: Order is executed,
        Then: Order added to pending and ID returned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = AsyncMock()
        service_any.exchange_client = mock_exchange_client
        order = self._create_order(mode="live")
        mock_order_result = MagicMock()
        mock_order_result.id = "abc123"
        mock_order_result.db_order_id = 42
        mock_order_result.db_order_public_id = "pub-42"
        mock_exchange_client.create_order = AsyncMock(return_value=mock_order_result)
        service_any.pending_orders[order.client_order_id] = base_module.PendingOrderState(
            request=order
        )
        result = await service_any._execute_live_order(order)
        assert result == "abc123"
        pending = service_any.pending_orders[order.client_order_id]
        assert pending.exchange_order_id == "abc123"
        assert pending.db_order_id == 42
        assert pending.order_public_id == "pub-42"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_error(self, mock_get_settings: MagicMock) -> None:
        """Verify live order execution handles error.

        Given: Exchange client that raises exception,
        When: Order is executed,
        Then: None is returned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = MagicMock()
        service_any.exchange_client = mock_exchange_client
        order = self._create_order(mode="live")
        mock_exchange_client.create_order = AsyncMock(side_effect=RuntimeError("boom"))
        result = await service_any._execute_live_order(order)
        assert result is None

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_order_live_success(self, mock_get_settings: MagicMock) -> None:
        """Verify live order processing success.

        Given: Valid live order request,
        When: Order is processed,
        Then: Submitted and accepted statuses published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._publish_order_status = AsyncMock()
        service_any._publish_execution = AsyncMock()
        service_any._execute_live_order = AsyncMock(return_value="abc123")
        order = self._create_order(mode="live")
        await service_any._process_order(order)
        service_any._publish_order_status.assert_any_await(order, "submitted")
        service_any._publish_order_status.assert_any_await(order, "accepted", "abc123")
        assert service_any._publish_order_status.await_count == 2
        service_any._publish_execution.assert_not_awaited()
        service_any._execute_live_order.assert_awaited_once_with(order)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_order_paper_rejection(self, mock_get_settings: MagicMock) -> None:
        """Verify paper order rejection.

        Given: Paper order with failing execution,
        When: Order is processed,
        Then: Submitted and rejected statuses published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._publish_order_status = AsyncMock()
        service_any._publish_execution = AsyncMock()
        service_any._execute_paper_order = AsyncMock(return_value=None)
        order = self._create_order()
        await service_any._process_order(order)
        service_any._publish_order_status.assert_any_await(order, "submitted")
        service_any._publish_order_status.assert_any_await(order, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_not_implemented(self, mock_get_settings: MagicMock) -> None:
        """Verify execution handler handles NotImplementedError.

        Given: Exchange client with unimplemented method,
        When: Execution handler runs,
        Then: Error is caught gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_exchange_client = MagicMock()
        service_any.exchange_client = mock_exchange_client

        async def execution_stream() -> AsyncIterator[Any]:
            if False:
                yield None
            raise NotImplementedError()

        mock_exchange_client.subscribe_executions = execution_stream
        await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_updates_pending(self, mock_get_settings: MagicMock) -> None:
        """Verify execution processing updates pending orders.

        Given: Pending order with filled execution and client_by_exchange mapping,
        When: Execution is processed,
        Then: Fill published and order removed from pending.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._publish_execution = AsyncMock()
        order = self._create_order(mode="live")
        service_any.pending_orders[order.client_order_id] = base_module.PendingOrderState(
            request=order
        )
        service_any.client_by_exchange = {"ex123": order.client_order_id}
        execution = ExecutionUpdate(
            order_id="ex123",
            exec_type="filled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
            cum_qty=0.1,
            cum_cost=10.0,
            average_price=100.0,
            fee_usd_equiv=0.5,
        )
        await service_any._process_execution(execution)
        service_any._publish_execution.assert_awaited_once()
        assert order.client_order_id not in service_any.pending_orders

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_heartbeat_loop_publishes(self, mock_get_settings: MagicMock) -> None:
        """Verify heartbeat loop publishes heartbeats.

        Given: Running service with configured heartbeat,
        When: Heartbeat loop runs,
        Then: Heartbeat is published with correct topic.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        publish_heartbeat_mock = AsyncMock()

        async def publish_side_effect(topic: str, message: HeartbeatData) -> None:
            assert message.component == "executor.kraken"
            service_any.running = False

        publish_heartbeat_mock.side_effect = publish_side_effect
        service_any._publish_heartbeat = publish_heartbeat_mock
        with patch(
            "asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            mock_sleep.return_value = None
            await service_any._heartbeat_loop()
        publish_heartbeat_mock.assert_awaited_once()
        assert service_any.heartbeat_seq == 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_heartbeat_when_running(self, mock_get_settings: MagicMock) -> None:
        """Verify heartbeat published when service running.

        Given: Running service with publisher,
        When: Heartbeat is published,
        Then: Publisher sends multipart message.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = AsyncMock()
        hb = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="executor.kraken",
            sequence=1,
            status="healthy",
            lag_ms=0,
        )
        await service_any._publish_heartbeat("heartbeat.executor.kraken", hb)
        service_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.symbols.mapper.SymbolMapperService.get_instance")
    async def test_handle_symbol_alias_update(
        self,
        mock_get_instance: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify symbol alias update triggers cache invalidation.

        Given: Symbol alias update payload,
        When: Update is handled,
        Then: Cache invalidation is triggered.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        mock_db_mapper = MagicMock()
        mock_get_instance.return_value = mock_db_mapper
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        payload = json.dumps(
            {
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
                "event": "symbol_aliases_updated",
                "action": "clear_cache",
            }
        )
        service_any._handle_symbol_alias_update(payload)
        mock_db_mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_without_websocket_executions_support(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify start handles client without websocket support.

        Given: Exchange client without websocket execution support,
        When: Service starts,
        Then: Service starts without execution handler.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        mock_settings_service = MagicMock()

        class MockExchangeClient:
            supports_websocket_executions = False

            def set_tracker(self, tracker: Any) -> None:
                """No-op tracker injection for tests."""

            async def __aenter__(self) -> MockExchangeClient:
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> None:
                """No cleanup required on context exit."""
                pass

        mock_exchange_client = MockExchangeClient()
        with (
            patch("snapper.data.repository.get_repository", return_value=MagicMock()),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch.object(service, "_order_handler", new=AsyncMock(return_value=None)) as order_mock,
            patch.object(
                service, "_heartbeat_loop", new=AsyncMock(return_value=None)
            ) as heartbeat_mock,
            patch.object(
                service, "_execution_handler", new=AsyncMock(return_value=None)
            ) as execution_mock,
            patch(
                "snapper.application.services.settings.get_settings_service",
                new=AsyncMock(return_value=mock_settings_service),
            ),
            patch(
                "snapper.config.settings.get_settings_with_service",
                return_value=mock_settings,
            ),
        ):
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            with patch(
                "snapper.messaging.executors.base.zmq.asyncio.Context",
                return_value=mock_context,
            ):
                await service.start()
        assert service.running is True
        order_mock.assert_awaited_once()
        heartbeat_mock.assert_awaited_once()
        execution_mock.assert_not_awaited()
        await service.stop()
        assert service.running is False

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_when_already_running(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify start returns early when already running.

        Given: Service already running,
        When: start() is called,
        Then: No new client is created.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service.running = True
        with (
            patch.object(service, "_create_exchange_client") as create_mock,
            patch(
                "snapper.data.repository.get_repository",
                side_effect=AssertionError("should not fetch repository"),
            ),
        ):
            await service.start()
        create_mock.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_skips_mismatched_exchange(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler skips orders for other exchanges.

        Given: Order for different exchange (zonda vs kraken),
        When: Order handler processes message,
        Then: Order is not processed.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.subscriber = AsyncMock()

        async def fake_recv() -> tuple[str, bytes]:
            service_any.running = False
            return ("orders.commands.kraken.BTC-USD.submit", b"{}")

        service_any.subscriber.recv_multipart = AsyncMock(side_effect=fake_recv)
        wrong_order = self._create_order(exchange="zonda")
        process_mock: AsyncMock = AsyncMock()
        service_any._process_order = process_mock
        with patch(
            "snapper.messaging.executors.base.parse_message",
            return_value=wrong_order,
        ):
            await service_any._order_handler()
        process_mock.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_handles_symbol_alias_update(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler handles symbol alias updates.

        Given: Symbol alias update message,
        When: Order handler receives message,
        Then: Symbol alias update handler is called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.subscriber = AsyncMock()
        service_any._handle_symbol_alias_update = MagicMock()
        payload_bytes = json.dumps(
            {"event": "symbol_aliases_updated", "action": "clear_cache"}
        ).encode("utf-8")

        async def fake_recv() -> tuple[str, bytes]:
            service_any.running = False
            return ("system.symbol_aliases", payload_bytes)

        service_any.subscriber.recv_multipart = AsyncMock(side_effect=fake_recv)
        await service_any._order_handler()
        service_any._handle_symbol_alias_update.assert_called_once_with(
            payload_bytes.decode("utf-8")
        )

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_unexpected_topic(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler ignores unexpected topics.

        Given: Message with unrecognized topic,
        When: Order handler processes message,
        Then: No handlers are called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.subscriber = AsyncMock()
        service_any._process_order = AsyncMock()
        service_any._handle_symbol_alias_update = MagicMock()

        async def fake_recv() -> tuple[str, bytes]:
            service_any.running = False
            return ("orders.kraken.invalid", b"{}")

        service_any.subscriber.recv_multipart = AsyncMock(side_effect=fake_recv)
        with patch(
            "snapper.messaging.executors.base.parse_message",
            side_effect=AssertionError("parse_message should not be called"),
        ):
            await service_any._order_handler()
        service_any._process_order.assert_not_awaited()
        service_any._handle_symbol_alias_update.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_waits_without_socket(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler waits when no socket available.

        Given: Service without subscriber socket,
        When: Order handler runs,
        Then: Handler sleeps and retries.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.subscriber = None

        async def sleep_side_effect(delay: float) -> None:
            assert delay == pytest.approx(0.1)
            service_any.running = False

        with patch(
            "snapper.messaging.executors.base.asyncio.sleep",
            new=AsyncMock(side_effect=sleep_side_effect),
        ) as sleep_mock:
            await service_any._order_handler()
        sleep_mock.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_order_status_handles_error(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order status publishing handles send error.

        Given: Publisher that raises exception,
        When: Order status is published,
        Then: Error is handled gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = AsyncMock()
        service_any.msg_publisher.send.side_effect = RuntimeError("send failed")
        order = self._create_order()
        await service_any._publish_order_status(order, "submitted")
        service_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_execution_handles_error(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify fill publishing handles send error.

        Given: Publisher that raises exception,
        When: Fill is published,
        Then: Error is handled gracefully.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = AsyncMock()
        service_any.msg_publisher.send.side_effect = RuntimeError("fill failed")
        fill_msg = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-1",
            exchange_order_id="one",
            client_order_id="one",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.0,
            price=0.0,
            last_size=0.0,
            last_price=0.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        await service_any._publish_execution("orders.events.kraken.BTC-USD.executed", fill_msg)
        service_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_logs_exception(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify execution handler logs exceptions.

        Given: Exchange client that raises ValueError,
        When: Execution handler runs,
        Then: Exception is logged.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True

        class FailingIterator:
            def __aiter__(self) -> FailingIterator:
                return self

            async def __anext__(self) -> ExecutionUpdate:
                raise ValueError("boom")

        class FailingClient:
            supports_websocket_executions = True

            def subscribe_executions(self) -> FailingIterator:
                return FailingIterator()

        service_any.exchange_client = FailingClient()
        await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_unknown_order(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify execution processing handles unknown orders.

        Given: Execution for non-existent order,
        When: Execution is processed,
        Then: No fill is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="missing",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.MARKET,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.1,
            cum_cost=10.0,
        )
        await service_any._process_execution(execution)
        service_any._publish_execution.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_symbol_alias_update_ignores_unexpected(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify symbol alias update ignores unexpected events.

        Given: Payload with unexpected event type,
        When: Update is handled,
        Then: Mapper is not called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        with patch(
            "snapper.infrastructure.symbols.mapper.SymbolMapperService.get_instance",
            side_effect=AssertionError("should not fetch mapper"),
        ):
            service_any._handle_symbol_alias_update(json.dumps({"event": "other"}))


class TestExecutor:
    """Tests for executor basic functionality."""

    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    def test_initialization(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify executor initializes with default state.

        Given: Valid settings configuration,
        When: Executor is created,
        Then: All attributes initialized to defaults.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        service = KrakenOrderExecutor()
        assert service.running is False
        assert service.heartbeat_seq == 0
        assert service.context is None
        assert service.subscriber is None
        assert service.publisher is None

    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    def test_get_status_initial(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify get_status returns initial state.

        Given: Newly created executor,
        When: get_status is called,
        Then: Status shows not running with broker info.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        service = KrakenOrderExecutor()
        status = service.get_status()
        assert status["running"] is False
        assert status["broker_xsub"] == "tcp://127.0.0.1:7500"
        assert status["broker_xpub"] == "tcp://127.0.0.1:7501"
        assert status["heartbeat_seq"] == 0

    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    def test_get_status_running(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify get_status returns running state.

        Given: Running executor with heartbeats sent,
        When: get_status is called,
        Then: Status shows running with current heartbeat seq.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        service = KrakenOrderExecutor()
        service.running = True
        service.heartbeat_seq = 42
        status = service.get_status()
        assert status["running"] is True
        assert status["heartbeat_seq"] == 42


class TestExecutorWebSocketExecutions:
    """Tests for executor websocket executions."""

    def _create_mock_settings(self, with_credentials: bool = True) -> MagicMock:
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.db_url = TEST_DB_URL
        if with_credentials:
            mock_settings.kraken_api_key = "test_api_key"
            mock_settings.kraken_api_secret = "test_api_secret"
        else:
            mock_settings.kraken_api_key = None
            mock_settings.kraken_api_secret = None
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    async def test_start_subscribes_to_executions(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """Verify start subscribes to executions with websocket client.

        Given: Exchange client with websocket execution support,
        When: Service starts,
        Then: Client context manager is entered.
        """
        mock_settings = self._create_mock_settings(with_credentials=True)
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = MagicMock()
        mock_exchange_client.__aenter__ = AsyncMock(return_value=mock_exchange_client)
        mock_exchange_client.__aexit__ = AsyncMock(return_value=None)
        service = KrakenOrderExecutor()
        mock_settings_service = MagicMock()
        mock_settings_with_db = self._create_mock_settings(with_credentials=True)
        with (
            patch("snapper.messaging.executors.base.zmq.asyncio.Context") as mock_context_class,
            patch.object(service, "_order_handler", new_callable=AsyncMock),
            patch.object(service, "_execution_handler", new_callable=AsyncMock),
            patch.object(service, "_heartbeat_loop", new_callable=AsyncMock),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch(
                "snapper.application.services.settings.get_settings_service",
                new=AsyncMock(return_value=mock_settings_service),
            ),
            patch(
                "snapper.config.settings.get_settings_with_service",
                return_value=mock_settings_with_db,
            ),
        ):
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_context_class.return_value = mock_context
            with patch(
                "snapper.application.services.settings.zmq.asyncio.Context",
                return_value=mock_context,
            ):
                start_task = asyncio.create_task(service.start())
                for _ in range(50):
                    await asyncio.sleep(0.1)
                    if mock_exchange_client.__aenter__.called:
                        break
                start_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await start_task
            mock_exchange_client.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.data.repository.get_repository")
    @patch("snapper.config.settings.get_settings")
    async def test_start_without_credentials_works(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """Verify start works without API credentials.

        Given: Configuration without API credentials,
        When: Service starts,
        Then: Service starts successfully.
        """
        mock_settings = self._create_mock_settings(with_credentials=False)
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = MagicMock()
        mock_exchange_client.__aenter__ = AsyncMock(return_value=mock_exchange_client)
        mock_exchange_client.__aexit__ = AsyncMock(return_value=None)
        service = KrakenOrderExecutor()
        mock_settings_service = MagicMock()
        mock_settings_with_db = self._create_mock_settings(with_credentials=False)
        with (
            patch("snapper.messaging.executors.base.zmq.asyncio.Context") as mock_context_class,
            patch.object(service, "_order_handler", new_callable=AsyncMock),
            patch.object(service, "_execution_handler", new_callable=AsyncMock),
            patch.object(service, "_heartbeat_loop", new_callable=AsyncMock),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch(
                "snapper.application.services.settings.get_settings_service",
                new=AsyncMock(return_value=mock_settings_service),
            ),
            patch(
                "snapper.config.settings.get_settings_with_service",
                return_value=mock_settings_with_db,
            ),
        ):
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_context_class.return_value = mock_context
            with patch(
                "snapper.application.services.settings.zmq.asyncio.Context",
                return_value=mock_context,
            ):
                start_task = asyncio.create_task(service.start())
                await asyncio.sleep(0.1)
                start_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await start_task

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_returns_order_id_not_fill(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify execute_live_order returns order ID.

        Given: Valid order request,
        When: Order is executed on exchange,
        Then: Order ID string returned and order added to pending.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        mock_exchange_client = MagicMock()
        service_any = cast(Any, service)
        service_any.exchange_client = mock_exchange_client
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="test-order-123",
            exchange="kraken",
        )
        mock_order_result = type(
            "ExchangeOrderSnapshot",
            (),
            {"id": "KRAKEN-ORDER-ABC123", "db_order_id": None, "db_order_public_id": None},
        )()
        mock_exchange_client.create_order = AsyncMock(return_value=mock_order_result)
        service.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        result = await service._execute_live_order(order)
        assert isinstance(result, str)
        assert result == "KRAKEN-ORDER-ABC123"
        pending = service.pending_orders[order.client_order_id]
        assert pending.exchange_order_id == "KRAKEN-ORDER-ABC123"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_limit_order(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify limit order execution returns order ID.

        Given: Valid limit order request,
        When: Order is executed,
        Then: Order ID returned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        mock_exchange_client = MagicMock()
        service_any = cast(Any, service)
        service_any.exchange_client = mock_exchange_client
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=0.5,
            price=44000.0,
            client_order_id="test-limit-123",
            exchange="kraken",
        )
        mock_order_result = type(
            "ExchangeOrderSnapshot",
            (),
            {"id": "KRAKEN-LIMIT-XYZ", "db_order_id": None, "db_order_public_id": None},
        )()
        mock_exchange_client.create_order = AsyncMock(return_value=mock_order_result)
        service_any.pending_orders[order.client_order_id] = base_module.PendingOrderState(
            request=order
        )
        result = await service._execute_live_order(order)
        assert result == "KRAKEN-LIMIT-XYZ"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execute_live_order_handles_error(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify live order execution handles exception.

        Given: Exchange client that raises exception,
        When: Order is executed,
        Then: None returned and order not in pending.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        mock_exchange_client = MagicMock()
        service_any = cast(Any, service)
        service_any.exchange_client = mock_exchange_client
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="test-error-123",
            exchange="kraken",
        )
        mock_exchange_client.create_order = AsyncMock(side_effect=Exception("Insufficient balance"))
        result = await service._execute_live_order(order)
        assert result is None
        assert "test-error-123" not in service.pending_orders

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_creates_fill_from_websocket(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify websocket execution creates fill envelope.

        Given: Pending order with filled execution from websocket,
        When: Execution is processed,
        Then: Fill envelope created with correct details.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="test-order-123",
            exchange="kraken",
        )
        service.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        service.client_by_exchange = {"KRAKEN-ORDER-ABC123": order.client_order_id}
        execution = ExecutionUpdate(
            order_id="KRAKEN-ORDER-ABC123",
            exec_type="filled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.MARKET,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
            cum_qty=0.1,
            cum_cost=4512.35,
            average_price=45123.50,
            fee_usd_equiv=4.51,
        )
        with patch.object(service, "_publish_execution", new_callable=AsyncMock) as mock_publish:
            await service._process_execution(execution)
            mock_publish.assert_called_once()
            fill: ExecutionData = mock_publish.call_args[0][1]
            assert isinstance(fill, ExecutionData)
            assert fill.client_order_id == "test-order-123"
            assert fill.exchange_order_id == "KRAKEN-ORDER-ABC123"
            assert fill.instrument == "BTC-USD"
            assert fill.size == pytest.approx(0.1)
            assert fill.price == pytest.approx(45123.50)
            assert fill.last_size == pytest.approx(0.1)
            assert fill.last_price == pytest.approx(45123.50)
            assert fill.fee == pytest.approx(4.51)
            assert fill.fee_asset == "USD"
            assert fill.status == "filled"
            assert order.client_order_id not in service.pending_orders

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_partial_fill(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify partial fill execution creates partial status.

        Given: Pending order with partial fill execution and client_by_exchange mapping,
        When: Execution is processed,
        Then: Partial fill published and order remains pending.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=1.0,
            price=45000.0,
            client_order_id="test-partial-123",
            exchange="kraken",
        )
        service.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        service.client_by_exchange = {"KRAKEN-PARTIAL-XYZ": order.client_order_id}
        execution = ExecutionUpdate(
            order_id="KRAKEN-PARTIAL-XYZ",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.PARTIALLY_FILLED,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            cum_cost=22500.0,
            average_price=45000.0,
            fee_usd_equiv=22.50,
        )
        with patch.object(service, "_publish_execution", new_callable=AsyncMock) as mock_publish:
            await service._process_execution(execution)
            fill: ExecutionData = mock_publish.call_args[0][1]
            assert fill.size == pytest.approx(0.5)
            assert fill.last_size == pytest.approx(0.5)
            assert fill.last_price == pytest.approx(45000.0)
            assert fill.status == "partial"
            assert order.client_order_id in service.pending_orders
            assert service.pending_orders[order.client_order_id].last_seen_cum_qty == pytest.approx(
                0.5
            )

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_cancelled_order_cleans_maps(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify cancelled order cleans maps but publishes no fill.

        Given: Pending order with cancellation execution,
        When: Execution is processed,
        Then: No fill is published but maps are cleaned.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=1.0,
            price=44000.0,
            client_order_id="test-cancel-123",
            exchange="kraken",
        )
        service.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        service.client_by_exchange = {"KRAKEN-CANCEL-ABC": order.client_order_id}
        execution = ExecutionUpdate(
            order_id="KRAKEN-CANCEL-ABC",
            exec_type="canceled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
            cum_qty=0.0,
            cum_cost=0.0,
            average_price=0.0,
        )
        with patch.object(service, "_publish_execution", new_callable=AsyncMock) as mock_publish:
            await service._process_execution(execution)
            mock_publish.assert_not_called()
            assert order.client_order_id not in service.pending_orders
            assert "KRAKEN-CANCEL-ABC" not in service.client_by_exchange

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_execution_unknown_order(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify unknown order execution is ignored.

        Given: Execution for order not in pending orders,
        When: Execution is processed,
        Then: No fill is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        execution = ExecutionUpdate(
            order_id="UNKNOWN-ORDER-123",
            exec_type="filled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
            cum_qty=0.1,
            cum_cost=4500.0,
            average_price=45000.0,
        )
        with patch.object(service, "_publish_execution", new_callable=AsyncMock) as mock_publish:
            await service._process_execution(execution)
            mock_publish.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_order_live_mode_waits_for_websocket(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify live mode order waits for websocket execution.

        Given: Live order request,
        When: Order is processed,
        Then: Only submitted status published, no fill.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service.running = True
        service.publisher = AsyncMock()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="test-live-123",
            exchange="kraken",
        )
        with (
            patch.object(
                service,
                "_execute_live_order",
                new_callable=AsyncMock,
                return_value="KRAKEN-ORDER-XYZ",
            ) as mock_execute_live,
            patch.object(
                service, "_publish_order_status", new_callable=AsyncMock
            ) as mock_publish_status,
            patch.object(
                service, "_publish_execution", new_callable=AsyncMock
            ) as mock_publish_execution,
        ):
            await service._process_order(order)
            mock_execute_live.assert_called_once_with(order)
            assert mock_publish_status.call_args_list[0] == call(order, "submitted")
            mock_publish_execution.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_order_live_mode_rejected(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify live mode order rejection publishes rejected fill.

        Given: Live order that fails execution,
        When: Order is processed,
        Then: Rejected fill published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service.running = True
        service.publisher = AsyncMock()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test_strategy",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="market",
            quantity=0.1,
            client_order_id="test-rejected-123",
            exchange="kraken",
        )
        with (
            patch.object(
                service,
                "_execute_live_order",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch.object(
                service, "_publish_order_status", new_callable=AsyncMock
            ) as mock_publish_status,
            patch.object(
                service, "_publish_execution", new_callable=AsyncMock
            ) as mock_publish_execution,
        ):
            await service._process_order(order)
            mock_publish_execution.assert_not_called()
            assert mock_publish_status.call_args_list[1] == call(order, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_handles_unsupported_client(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify execution handler handles unsupported client.

        Given: Client that raises NotImplementedError,
        When: Execution handler runs,
        Then: Handler handles gracefully.
        """
        mock_settings = self._create_mock_settings(with_credentials=False)
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_exchange_client = MagicMock()
        service_any.exchange_client = mock_exchange_client

        async def mock_subscribe_executions() -> AsyncIterator[Any]:
            for _ in []:
                yield
            raise NotImplementedError("Client does not support execution streaming")

        mock_exchange_client.subscribe_executions = mock_subscribe_executions
        handler_task = asyncio.create_task(service_any._execution_handler())
        await asyncio.sleep(0.1)
        handler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await handler_task


class TestCancelReplaceHandlers:
    """Tests for cancel and replace command handlers."""

    def _create_mock_settings(self) -> MagicMock:
        """Create mock settings for tests."""
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        return mock_settings

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_cancel_command_processes_valid_cancel(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify cancel command handler processes valid cancel requests.

        Given: A valid OrderCancelData for correct exchange,
        When: Cancel command handler processes it,
        Then: _process_cancel is called with the envelope.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_cancel = AsyncMock()
        payload = (
            '{"type":"order_cancel","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"kraken","instrument":"BTC-USD",'
            '"exchange_order_id":"KRAKEN-123","client_order_id":"client_456"}'
        )
        await service_any._handle_cancel_command(payload, "kraken", "BTC-USD")
        service_any._process_cancel.assert_awaited_once()
        cancel_msg = service_any._process_cancel.call_args[0][0]
        assert cancel_msg.exchange == "kraken"
        assert cancel_msg.exchange_order_id == "KRAKEN-123"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_cancel_command_rejects_wrong_exchange(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify cancel command handler rejects wrong exchange.

        Given: An OrderCancelData for different exchange,
        When: Cancel command handler processes it,
        Then: _process_cancel is not called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_cancel = AsyncMock()
        payload = (
            '{"type":"order_cancel","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"paper","instrument":"BTC-USD",'
            '"exchange_order_id":"PAPER-123","client_order_id":"client_456"}'
        )
        await service_any._handle_cancel_command(payload, "kraken", "BTC-USD")
        service_any._process_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.messaging.executors.base.parse_message")
    @patch("snapper.config.settings.get_settings")
    async def test_handle_cancel_command_rejects_wrong_message_type(
        self,
        mock_get_settings: MagicMock,
        mock_parse_message: MagicMock,
    ) -> None:
        """Verify cancel command handler rejects wrong message type.

        Given: A non-cancel message on cancel topic,
        When: Cancel command handler processes it,
        Then: _process_cancel is not called and warning is logged.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_cancel = AsyncMock()
        mock_msg = MagicMock()
        mock_msg.type = "heartbeat"
        mock_parse_message.return_value = mock_msg
        await service_any._handle_cancel_command("{}", "kraken", "BTC-USD")
        service_any._process_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_replace_command_processes_valid_replace(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify replace command handler processes valid replace requests.

        Given: A valid OrderReplaceData for correct exchange,
        When: Replace command handler processes it,
        Then: _process_replace is called with the envelope.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_replace = AsyncMock()
        payload = (
            '{"type":"order_replace","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"kraken","instrument":"BTC-USD",'
            '"exchange_order_id":"KRAKEN-123","client_order_id":"client_456",'
            '"new_quantity":0.5,"new_price":48000.0}'
        )
        await service_any._handle_replace_command(payload, "kraken", "BTC-USD")
        service_any._process_replace.assert_awaited_once()
        replace_msg = service_any._process_replace.call_args[0][0]
        assert replace_msg.new_quantity == pytest.approx(0.5)
        assert replace_msg.new_price == pytest.approx(48000.0)

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_replace_command_rejects_wrong_exchange(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify replace command handler rejects wrong exchange.

        Given: An OrderReplaceData for different exchange,
        When: Replace command handler processes it,
        Then: _process_replace is not called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_replace = AsyncMock()
        payload = (
            '{"type":"order_replace","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"zonda","instrument":"BTC-PLN",'
            '"exchange_order_id":"ZONDA-123","client_order_id":"client_456","new_price":200000.0}'
        )
        await service_any._handle_replace_command(payload, "kraken", "BTC-PLN")
        service_any._process_replace.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.messaging.executors.base.parse_message")
    @patch("snapper.config.settings.get_settings")
    async def test_handle_replace_command_rejects_wrong_message_type(
        self,
        mock_get_settings: MagicMock,
        mock_parse_message: MagicMock,
    ) -> None:
        """Verify replace command handler rejects wrong message type.

        Given: A non-replace message on replace topic,
        When: Replace command handler processes it,
        Then: _process_replace is not called and warning is logged.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_replace = AsyncMock()
        mock_msg = MagicMock()
        mock_msg.type = "heartbeat"
        mock_parse_message.return_value = mock_msg
        await service_any._handle_replace_command("{}", "kraken", "BTC-USD")
        service_any._process_replace.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.messaging.executors.base.parse_message")
    @patch("snapper.config.settings.get_settings")
    async def test_handle_submit_command_handles_invalid_payload(
        self,
        mock_get_settings: MagicMock,
        mock_parse_message: MagicMock,
    ) -> None:
        """Verify submit command handler handles invalid payload gracefully.

        Given: An invalid payload that causes MessageParseError,
        When: Submit command handler processes it,
        Then: Warning is logged and no processing occurs.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_order = AsyncMock()
        mock_parse_message.side_effect = MessageParseError("Invalid JSON")
        await service_any._handle_submit_command("invalid-json", "kraken", "BTC-USD")
        service_any._process_order.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.messaging.executors.base.parse_message")
    @patch("snapper.config.settings.get_settings")
    async def test_handle_cancel_command_handles_invalid_payload(
        self,
        mock_get_settings: MagicMock,
        mock_parse_message: MagicMock,
    ) -> None:
        """Verify cancel command handler handles invalid payload gracefully.

        Given: An invalid payload that causes MessageParseError,
        When: Cancel command handler processes it,
        Then: Warning is logged and no processing occurs.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_cancel = AsyncMock()
        mock_parse_message.side_effect = MessageParseError("Invalid JSON")
        await service_any._handle_cancel_command("invalid-json", "kraken", "BTC-USD")
        service_any._process_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.messaging.executors.base.parse_message")
    @patch("snapper.config.settings.get_settings")
    async def test_handle_replace_command_handles_invalid_payload(
        self,
        mock_get_settings: MagicMock,
        mock_parse_message: MagicMock,
    ) -> None:
        """Verify replace command handler handles invalid payload gracefully.

        Given: An invalid payload that causes MessageParseError,
        When: Replace command handler processes it,
        Then: Warning is logged and no processing occurs.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_replace = AsyncMock()
        mock_parse_message.side_effect = MessageParseError("Invalid JSON")
        await service_any._handle_replace_command("invalid-json", "kraken", "BTC-USD")
        service_any._process_replace.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_submit_command_rejects_wrong_instrument(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify submit command handler rejects instrument mismatch.

        Given: An OrderRequestData with different instrument than topic,
        When: Submit command handler processes it,
        Then: _process_order is not called due to invariant violation.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_order = AsyncMock()
        payload = (
            '{"type":"order_request","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"strategy_id":"test","exchange":"kraken",'
            '"instrument":"ETH-USD","mode":"paper","side":"buy","order_type":"market",'
            '"quantity":1.0,"client_order_id":"test-123"}'
        )
        await service_any._handle_submit_command(payload, "kraken", "BTC-USD")
        service_any._process_order.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_cancel_command_rejects_wrong_instrument(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify cancel command handler rejects instrument mismatch.

        Given: An OrderCancelData with different instrument than topic,
        When: Cancel command handler processes it,
        Then: _process_cancel is not called due to invariant violation.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_cancel = AsyncMock()
        payload = (
            '{"type":"order_cancel","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"kraken","instrument":"ETH-USD",'
            '"exchange_order_id":"KRAKEN-123","client_order_id":"client_456"}'
        )
        await service_any._handle_cancel_command(payload, "kraken", "BTC-USD")
        service_any._process_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_handle_replace_command_rejects_wrong_instrument(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify replace command handler rejects instrument mismatch.

        Given: An OrderReplaceData with different instrument than topic,
        When: Replace command handler processes it,
        Then: _process_replace is not called due to invariant violation.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._process_replace = AsyncMock()
        payload = (
            '{"type":"order_replace","session_id":"","sequence_id":0,'
            '"public_id":"test-pid","timestamp":"2024-01-01T00:00:00Z",'
            '"exchange":"kraken","instrument":"ETH-USD",'
            '"exchange_order_id":"KRAKEN-123","client_order_id":"client_456","new_price":48000.0}'
        )
        await service_any._handle_replace_command(payload, "kraken", "BTC-USD")
        service_any._process_replace.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_cancel_success(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _process_cancel handles successful cancellation.

        Given: A cancel request for existing order,
        When: Exchange client cancels successfully,
        Then: Cancelled event is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._publish_cancel_event = AsyncMock()
        mock_client = AsyncMock()
        mock_result = MagicMock()
        mock_result.status = OrderStatusEnum.CANCELED
        mock_client.cancel_order = AsyncMock(return_value=mock_result)
        service_any.exchange_client = mock_client
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._process_cancel(cancel_envelope)
        service_any._publish_cancel_event.assert_awaited_with(cancel_envelope, "cancelled")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_cancel_success_cleans_maps(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _process_cancel cleans up maps on successful cancellation.

        Given: A cancel request for order in pending_orders with client_by_exchange mapping,
        When: Exchange client cancels successfully,
        Then: Both maps are cleaned up.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._publish_cancel_event = AsyncMock()
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=1.0,
            price=50000.0,
            client_order_id="client_456",
        )
        service_any.pending_orders = {
            order.client_order_id: base_module.PendingOrderState(request=order)
        }
        service_any.client_by_exchange = {"KRAKEN-123": order.client_order_id}
        mock_client = AsyncMock()
        mock_result = MagicMock()
        mock_result.status = OrderStatusEnum.CANCELED
        mock_client.cancel_order = AsyncMock(return_value=mock_result)
        service_any.exchange_client = mock_client
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._process_cancel(cancel_envelope)
        assert order.client_order_id not in service_any.pending_orders
        assert "KRAKEN-123" not in service_any.client_by_exchange
        service_any._publish_cancel_event.assert_awaited_with(cancel_envelope, "cancelled")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_cancel_failure(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _process_cancel handles failed cancellation.

        Given: A cancel request,
        When: Exchange client fails to cancel,
        Then: Rejected event is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._publish_cancel_event = AsyncMock()
        mock_client = AsyncMock()
        mock_result = MagicMock()
        mock_result.status = OrderStatusEnum.OPEN
        mock_client.cancel_order = AsyncMock(return_value=mock_result)
        service_any.exchange_client = mock_client
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._process_cancel(cancel_envelope)
        service_any._publish_cancel_event.assert_awaited_with(cancel_envelope, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_cancel_exception(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _process_cancel handles exceptions.

        Given: A cancel request,
        When: Exchange client raises exception,
        Then: Rejected event is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._publish_cancel_event = AsyncMock()
        mock_client = AsyncMock()
        mock_client.cancel_order = AsyncMock(side_effect=Exception("Network error"))
        service_any.exchange_client = mock_client
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._process_cancel(cancel_envelope)
        service_any._publish_cancel_event.assert_awaited_with(cancel_envelope, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_replace_publishes_rejected(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _process_replace publishes rejected event.

        Given: A replace request,
        When: Replace is not implemented,
        Then: Rejected event is published.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any._publish_replace_event = AsyncMock()
        replace_envelope = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
            new_price=49000.0,
        )
        await service_any._process_replace(replace_envelope)
        service_any._publish_replace_event.assert_awaited_with(replace_envelope, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_cancel_event_success(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_cancel_event publishes to correct topic.

        Given: A cancel envelope and status,
        When: Publishing cancel event,
        Then: Message is published to orders.events.*.*.{status}.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_publisher = AsyncMock()
        service_any.msg_publisher = mock_publisher
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._publish_cancel_event(cancel_envelope, "cancelled")
        mock_publisher.send.assert_awaited_once()
        published_data = mock_publisher.send.call_args[0][1]
        assert published_data.event == "cancelled"
        assert published_data.instrument == "BTC-USD"
        assert published_data.exchange == "kraken"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_cancel_event_no_publisher(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_cancel_event handles missing publisher.

        Given: No publisher available,
        When: Publishing cancel event,
        Then: Returns without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = None
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._publish_cancel_event(cancel_envelope, "cancelled")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_cancel_event_handles_exception(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_cancel_event handles publish exception.

        Given: Publisher that raises exception,
        When: Publishing cancel event,
        Then: Error is logged without crash.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_publisher = AsyncMock()
        mock_publisher.send = AsyncMock(side_effect=Exception("Network error"))
        service_any.msg_publisher = mock_publisher
        cancel_envelope = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._publish_cancel_event(cancel_envelope, "cancelled")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_replace_event_success(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_replace_event publishes to correct topic.

        Given: A replace envelope and status,
        When: Publishing replace event,
        Then: Message is published to orders.events.*.*.{status}.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_publisher = AsyncMock()
        service_any.msg_publisher = mock_publisher
        replace_envelope = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
            new_quantity=0.5,
            new_price=48000.0,
        )
        await service_any._publish_replace_event(replace_envelope, "rejected")
        mock_publisher.send.assert_awaited_once()
        published_data = mock_publisher.send.call_args[0][1]
        assert published_data.event == "rejected"
        assert published_data.instrument == "BTC-USD"
        assert published_data.exchange == "kraken"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_replace_event_no_publisher(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_replace_event handles missing publisher.

        Given: No publisher available,
        When: Publishing replace event,
        Then: Returns without error.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any.msg_publisher = None
        replace_envelope = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._publish_replace_event(replace_envelope, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_replace_event_handles_exception(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _publish_replace_event handles publish exception.

        Given: Publisher that raises exception,
        When: Publishing replace event,
        Then: Error is logged without crash.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        mock_publisher = AsyncMock()
        mock_publisher.send = AsyncMock(side_effect=Exception("Network error"))
        service_any.msg_publisher = mock_publisher
        replace_envelope = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-123",
            client_order_id="client_456",
        )
        await service_any._publish_replace_event(replace_envelope, "rejected")

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_dispatches_cancel_command(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler dispatches cancel commands.

        Given: A message on orders.commands.*.*.cancel topic,
        When: Order handler processes it,
        Then: _handle_cancel_command is called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._handle_cancel_command = AsyncMock()
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "orders.commands.kraken.BTC-USD.cancel",
                    b'{"type":"order_cancel","exchange":"kraken","instrument":"BTC-USD",'
                    b'"exchange_order_id":"K123","client_order_id":"c456"}',
                )
            service_any.running = False
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        service_any._handle_cancel_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_dispatches_replace_command(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler dispatches replace commands.

        Given: A message on orders.commands.*.*.replace topic,
        When: Order handler processes it,
        Then: _handle_replace_command is called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._handle_replace_command = AsyncMock()
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return (
                    "orders.commands.kraken.BTC-USD.replace",
                    b'{"type":"order_replace","exchange":"kraken","instrument":"BTC-USD",'
                    b'"exchange_order_id":"K123","client_order_id":"c456","new_price":50000.0}',
                )
            service_any.running = False
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        service_any._handle_replace_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_order_handler_ignores_unknown_command(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify order handler ignores unknown commands.

        Given: A message on orders.commands.*.*.unknown topic,
        When: Order handler processes it,
        Then: No handler is called.
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        service_any.running = True
        service_any._handle_submit_command = AsyncMock()
        service_any._handle_cancel_command = AsyncMock()
        service_any._handle_replace_command = AsyncMock()
        mock_subscriber = AsyncMock()
        recv_count = 0

        async def mock_recv() -> tuple[str, bytes]:
            nonlocal recv_count
            recv_count += 1
            if recv_count == 1:
                return ("orders.commands.kraken.BTC-USD.unknown", b"{}")
            service_any.running = False
            return ("", b"")

        mock_subscriber.recv_multipart = mock_recv
        service_any.subscriber = mock_subscriber
        await service_any._order_handler()
        service_any._handle_submit_command.assert_not_awaited()
        service_any._handle_cancel_command.assert_not_awaited()
        service_any._handle_replace_command.assert_not_awaited()


class TestDeltaFillSemantics:
    """Tests for Stage A: delta fill fields and order status cleanup."""

    @pytest.mark.asyncio
    async def test_build_execution_data_uses_last_qty_when_available(self) -> None:
        """Verify executor propagates exchange-provided last_qty/last_price.

        Given: ExecutionUpdate with last_qty and last_price from exchange,
        When: _build_execution_data is called,
        Then: last_size and last_price use exchange values directly.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="delta-1")
        execution = ExecutionUpdate(
            order_id="ex-1",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50100.0,
            last_qty=0.5,
            last_price=50100.0,
        )
        _topic, fill = ex._build_execution_data(execution, "ex-1", order, "kraken")
        assert fill.size == pytest.approx(0.5)
        assert fill.price == pytest.approx(50100.0)
        assert fill.last_size == pytest.approx(0.5)
        assert fill.last_price == pytest.approx(50100.0)

    @pytest.mark.asyncio
    async def test_build_execution_data_fallback_delta_from_cumulative(self) -> None:
        """Verify executor computes delta from cumulative when last_qty missing.

        Given: ExecutionUpdate without last_qty/last_price,
        When: _build_execution_data is called with prior cumulative state,
        Then: last_size is computed as cum_qty minus last_seen_cum_qty.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="delta-2")
        ex.pending_orders["delta-2"] = base_module.PendingOrderState(
            request=order, last_seen_cum_qty=0.3
        )
        execution = ExecutionUpdate(
            order_id="ex-2",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50050.0,
        )
        _topic, fill = ex._build_execution_data(execution, "ex-2", order, "kraken")
        assert fill.size == pytest.approx(0.5)
        assert fill.last_size == pytest.approx(0.2)
        assert fill.last_price == pytest.approx(50050.0)
        assert ex.pending_orders["delta-2"].last_seen_cum_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_build_execution_data_two_partials_then_filled(self) -> None:
        """Verify correct delta across a sequence of partial fills.

        Given: Three execution events for the same order (partial, partial, filled),
        When: _build_execution_data is called for each,
        Then: Each event carries the correct incremental delta.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="delta-seq")

        partial_1 = ExecutionUpdate(
            order_id="ex-seq",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.3,
            average_price=50000.0,
            last_qty=0.3,
            last_price=50000.0,
        )
        _topic, fill_1 = ex._build_execution_data(partial_1, "ex-seq", order, "kraken")
        assert fill_1.last_size == pytest.approx(0.3)
        assert fill_1.last_price == pytest.approx(50000.0)
        assert fill_1.status == "partial"

        partial_2 = ExecutionUpdate(
            order_id="ex-seq",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50050.0,
            last_qty=0.2,
            last_price=50100.0,
        )
        _topic, fill_2 = ex._build_execution_data(partial_2, "ex-seq", order, "kraken")
        assert fill_2.size == pytest.approx(0.5)
        assert fill_2.last_size == pytest.approx(0.2)
        assert fill_2.last_price == pytest.approx(50100.0)
        assert fill_2.status == "partial"

        final = ExecutionUpdate(
            order_id="ex-seq",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CLOSED,
            timestamp=datetime.now(UTC),
            cum_qty=0.8,
            average_price=50075.0,
            last_qty=0.3,
            last_price=50150.0,
        )
        _topic, fill_3 = ex._build_execution_data(final, "ex-seq", order, "kraken")
        assert fill_3.size == pytest.approx(0.8)
        assert fill_3.last_size == pytest.approx(0.3)
        assert fill_3.last_price == pytest.approx(50150.0)
        assert fill_3.status == "filled"

    @pytest.mark.asyncio
    async def test_build_execution_data_zonda_delta_partials_accumulate(self) -> None:
        """Verify Zonda trade events are accumulated until requested size is reached.

        Given: Two Zonda trade executions where each payload reports only delta quantity,
        When: _build_execution_data is called for both events,
        Then: The first fill stays partial and the second closes the order.
        """
        ex: Any = ZondaDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="zonda-delta", quantity=0.0001, exchange="zonda")
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)

        first = ExecutionUpdate(
            order_id="zonda-ex-1",
            exec_type="trade",
            symbol="BTC-PLN",
            side=OrderSideEnum.SELL,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
            last_qty=0.00002073,
            last_price=248468.97,
        )
        _topic, fill_1 = ex._build_execution_data(first, "zonda-ex-1", order, "zonda")
        assert fill_1.size == pytest.approx(0.00002073)
        assert fill_1.last_size == pytest.approx(0.00002073)
        assert fill_1.status == "partial"
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == pytest.approx(
            0.00002073
        )

        second = ExecutionUpdate(
            order_id="zonda-ex-1",
            exec_type="trade",
            symbol="BTC-PLN",
            side=OrderSideEnum.SELL,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
            last_qty=0.00007927,
            last_price=248468.97,
        )
        _topic, fill_2 = ex._build_execution_data(second, "zonda-ex-1", order, "zonda")
        assert fill_2.size == pytest.approx(0.0001)
        assert fill_2.last_size == pytest.approx(0.00007927)
        assert fill_2.status == "filled"

    @pytest.mark.asyncio
    async def test_last_seen_cum_qty_cleaned_on_filled(self) -> None:
        """Verify last_seen_cum_qty is removed after final fill.

        Given: Pending order with cumulative tracking state,
        When: Final fill is processed via _process_execution,
        Then: last_seen_cum_qty entry is cleaned up.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="cleanup-1")
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex-cleanup"] = order.client_order_id
        ex.pending_orders[order.client_order_id].last_seen_cum_qty = 0.3
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-cleanup",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CLOSED,
            timestamp=datetime.now(UTC),
            cum_qty=1.0,
            average_price=50000.0,
            last_qty=0.7,
            last_price=50100.0,
        )
        await ex._process_execution(execution)
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_last_seen_cum_qty_cleaned_on_canceled(self) -> None:
        """Verify last_seen_cum_qty is removed on order cancellation.

        Given: Pending order with cumulative tracking state,
        When: Cancellation event is processed,
        Then: last_seen_cum_qty entry is cleaned up.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="cancel-1")
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex-cancel"] = order.client_order_id
        ex.pending_orders[order.client_order_id].last_seen_cum_qty = 0.2
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-cancel",
            exec_type="canceled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
        )
        await ex._process_execution(execution)
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_last_seen_cum_qty_cleaned_on_expired(self) -> None:
        """Verify last_seen_cum_qty is removed on order expiration.

        Given: Pending order with cumulative tracking state,
        When: Expiration event is processed,
        Then: last_seen_cum_qty entry is cleaned up.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="expire-1")
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex-expire"] = order.client_order_id
        ex.pending_orders[order.client_order_id].last_seen_cum_qty = 0.1
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-expire",
            exec_type="expired",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.EXPIRED,
            timestamp=datetime.now(UTC),
        )
        await ex._process_execution(execution)
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_publish_order_status_filled_size_zero_for_submitted(self) -> None:
        """Verify submitted order status publishes filled_size=0.0.

        Given: Running executor with publisher,
        When: Order status is published as 'submitted',
        Then: filled_size is 0.0, not order.quantity.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.msg_publisher = AsyncMock()
        order = make_order(quantity=1.0)
        await ex._publish_order_status(order, "submitted")
        ex.msg_publisher.send.assert_awaited_once()
        published = ex.msg_publisher.send.call_args[0][1]
        assert published.filled_size == 0.0
        assert published.size == 1.0

    @pytest.mark.asyncio
    async def test_publish_order_status_filled_size_zero_for_accepted(self) -> None:
        """Verify accepted order status publishes filled_size=0.0.

        Given: Running executor with publisher,
        When: Order status is published as 'accepted',
        Then: filled_size is 0.0, not order.quantity.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.msg_publisher = AsyncMock()
        order = make_order(quantity=2.0)
        await ex._publish_order_status(order, "accepted", exchange_order_id="ex-abc")
        ex.msg_publisher.send.assert_awaited_once()
        published = ex.msg_publisher.send.call_args[0][1]
        assert published.filled_size == 0.0
        assert published.size == 2.0

    @pytest.mark.asyncio
    async def test_publish_order_status_filled_size_zero_for_rejected(self) -> None:
        """Verify rejected order status publishes filled_size=0.0.

        Given: Running executor with publisher,
        When: Order status is published as 'rejected',
        Then: filled_size is 0.0.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.msg_publisher = AsyncMock()
        order = make_order(quantity=1.5)
        await ex._publish_order_status(order, "rejected")
        ex.msg_publisher.send.assert_awaited_once()
        published = ex.msg_publisher.send.call_args[0][1]
        assert published.filled_size == 0.0


class TestExecutorBasePersistence:
    """Tests for Stage C: DB persistence in executor base."""

    @pytest.mark.asyncio
    async def test_process_execution_logs_to_db_on_fill(self) -> None:
        """Verify _process_execution calls DB logging for fill with DB IDs.

        Given: Pending order with db_order_id and order_public_id set,
        When: Filled execution is processed,
        Then: _log_execution_to_db and _log_order_update_to_db are called.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="db-fill-1")
        pending = base_module.PendingOrderState(
            request=order, db_order_id=42, order_public_id="pub-42"
        )
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-db"] = order.client_order_id
        mock_client = AsyncMock()
        ex.exchange_client = mock_client
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-db",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CLOSED,
            timestamp=datetime.now(UTC),
            cum_qty=1.0,
            average_price=50000.0,
            last_qty=1.0,
            last_price=50000.0,
            fee_usd_equiv=0.5,
        )
        await ex._process_execution(execution)
        mock_client._log_execution_to_db.assert_awaited_once()
        mock_client._log_order_update_to_db.assert_awaited_once()
        call_args = mock_client._log_order_update_to_db.call_args
        assert call_args.kwargs["db_order_id"] == 42
        assert call_args.kwargs["status"] == OrderStatusEnum.CLOSED

    @pytest.mark.asyncio
    async def test_process_execution_partial_logs_open_status(self) -> None:
        """Verify partial fill logs OPEN status (not CLOSED) to DB.

        Given: Pending order with DB IDs,
        When: Partial fill is processed,
        Then: Order status update uses OPEN, order remains pending.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="db-partial-1")
        pending = base_module.PendingOrderState(
            request=order, db_order_id=42, order_public_id="pub-42"
        )
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-partial"] = order.client_order_id
        mock_client = AsyncMock()
        ex.exchange_client = mock_client
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-partial",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50000.0,
            last_qty=0.5,
            last_price=50000.0,
            fee_usd_equiv=0.25,
        )
        await ex._process_execution(execution)
        call_args = mock_client._log_order_update_to_db.call_args
        assert call_args.kwargs["status"] == OrderStatusEnum.OPEN
        assert order.client_order_id in ex.pending_orders

    @pytest.mark.asyncio
    async def test_process_execution_no_db_ids_skips_logging(self) -> None:
        """Verify execution without DB IDs skips DB logging.

        Given: Pending order without db_order_id,
        When: Execution is processed,
        Then: No DB logging calls are made.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="no-db-1")
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex-nodb"] = order.client_order_id
        mock_client = AsyncMock()
        ex.exchange_client = mock_client
        ex._publish_execution = AsyncMock()
        execution = ExecutionUpdate(
            order_id="ex-nodb",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CLOSED,
            timestamp=datetime.now(UTC),
            cum_qty=1.0,
            average_price=50000.0,
            last_qty=1.0,
            last_price=50000.0,
        )
        await ex._process_execution(execution)
        mock_client._log_execution_to_db.assert_not_awaited()
        mock_client._log_order_update_to_db.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handle_cancellation_logs_db_update(self) -> None:
        """Verify cancellation handler logs CANCELED status to DB.

        Given: Pending order with db_order_id,
        When: Canceled execution is handled,
        Then: _log_order_update_to_db is called with CANCELED status.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="cancel-db-1")
        pending = base_module.PendingOrderState(
            request=order, db_order_id=99, order_public_id="pub-99"
        )
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-cancel-db"] = order.client_order_id
        mock_client = AsyncMock()
        ex.exchange_client = mock_client
        execution = ExecutionUpdate(
            order_id="ex-cancel-db",
            exec_type="canceled",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
        )
        result = await ex._handle_cancellation(
            execution, "ex-cancel-db", order.client_order_id, "kraken"
        )
        assert result is True
        mock_client._log_order_update_to_db.assert_awaited_once()
        call_args = mock_client._log_order_update_to_db.call_args
        assert call_args.kwargs["db_order_id"] == 99
        assert call_args.kwargs["status"] == OrderStatusEnum.CANCELED

    @pytest.mark.asyncio
    async def test_process_cancel_logs_db_update(self) -> None:
        """Verify _process_cancel logs CANCELED status to DB.

        Given: Pending order with db_order_id and successful cancel,
        When: Cancel is processed,
        Then: _log_order_update_to_db is called via exchange client.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(client_order_id="cancel-process-1")
        pending = base_module.PendingOrderState(
            request=order, db_order_id=77, order_public_id="pub-77"
        )
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-cancel-proc"] = order.client_order_id
        mock_client = AsyncMock()
        cancel_result = MagicMock()
        cancel_result.status = OrderStatusEnum.CANCELED
        mock_client.cancel_order = AsyncMock(return_value=cancel_result)
        ex.exchange_client = mock_client
        ex.msg_publisher = AsyncMock()
        cancel_data = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="ex-cancel-proc",
            client_order_id=order.client_order_id,
        )
        await ex._process_cancel(cancel_data)
        mock_client._log_order_update_to_db.assert_awaited_once()
        assert order.client_order_id not in ex.pending_orders


def _make_db_order(
    client_order_id: str = "c1",
    exchange_order_id: str = "ex-1",
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    status: str = "open",
    side: str = "buy",
    size: float = 1.0,
    filled_size: float = 0.0,
) -> dict[str, Any]:
    """Create a mock DB OrderRow dict for recovery tests."""
    return {
        "public_id": f"pub-{client_order_id}",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "session_id": "s1",
        "sequence_id": 1,
        "instrument": instrument,
        "exchange": exchange,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
        "updated_at": None,
        "side": side,
        "order_type": "market",
        "price": None,
        "size": size,
        "filled_size": filled_size,
        "average_price": None,
        "status": status,
        "time_in_force": None,
        "error": None,
    }


class TestExecutorRecovery:
    """Tests for Stage D: executor startup recovery."""

    @pytest.mark.asyncio
    async def test_recover_pending_from_exchange_open_order(self) -> None:
        """Verify open exchange order is recovered into pending state.

        Given: Exchange reports one open order matching DB active order,
        When: _recover_pending_orders runs,
        Then: PendingOrderState is created with correct fill state.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        snap = SimpleNamespace(
            id="ex-1",
            filled=0.3,
            remaining=0.7,
            status=OrderStatusEnum.OPEN,
            db_order_id=None,
            db_order_public_id=None,
        )
        mock_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_db_order(exchange_order_id="ex-1", filled_size=0.3)]
        )
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert "c1" in ex.pending_orders
        pending = ex.pending_orders["c1"]
        assert pending.exchange_order_id == "ex-1"
        assert pending.last_seen_cum_qty == pytest.approx(0.3)
        assert "ex-1" in ex.client_by_exchange

    @pytest.mark.asyncio
    async def test_recover_db_order_missing_on_exchange_closed(self) -> None:
        """Verify DB active order that is closed on exchange gets updated.

        Given: DB says order is open, but exchange says CLOSED,
        When: _recover_pending_orders runs,
        Then: DB is updated and order is NOT added to pending.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        closed_snap = SimpleNamespace(
            id="ex-1",
            filled=1.0,
            remaining=0.0,
            status=OrderStatusEnum.CLOSED,
        )
        mock_client.get_order = AsyncMock(return_value=closed_snap)
        mock_client._log_order_update_to_db = AsyncMock()
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_db_order(exchange_order_id="ex-1")]
        )
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert "c1" not in ex.pending_orders
        mock_client._log_order_update_to_db.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recover_no_active_orders(self) -> None:
        """Verify clean start with no active orders.

        Given: No open orders on exchange or in DB,
        When: _recover_pending_orders runs,
        Then: No pending state created, no errors.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert len(ex.pending_orders) == 0

    @pytest.mark.asyncio
    async def test_recover_exchange_query_failure_continues(self) -> None:
        """Verify recovery continues if exchange query fails.

        Given: Exchange get_orders raises exception,
        When: _recover_pending_orders runs,
        Then: Recovery continues with DB-only data.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(side_effect=RuntimeError("network"))
        mock_client.get_order = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-1",
                filled=0.0,
                status=OrderStatusEnum.OPEN,
            )
        )
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_make_db_order()])
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert "c1" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_recover_no_repository_skips(self) -> None:
        """Verify recovery is skipped when no repository is configured.

        Given: Executor without repository,
        When: _recover_pending_orders runs,
        Then: Recovery is skipped gracefully.
        """
        ex: Any = MergedDummyExecutor()
        ex.exchange_client = AsyncMock()
        ex.repository = None
        await ex._recover_pending_orders("kraken")
        assert len(ex.pending_orders) == 0

    @pytest.mark.asyncio
    async def test_recover_order_unverifiable_on_exchange(self) -> None:
        """Verify unverifiable order is skipped during recovery.

        Given: DB active order, exchange get_order raises,
        When: _recover_pending_orders runs,
        Then: Order is skipped (not added to pending).
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        mock_client.get_order = AsyncMock(side_effect=RuntimeError("not found"))
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_make_db_order()])
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert "c1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_recover_skips_orders_without_exchange_id(self) -> None:
        """Verify orders without exchange_order_id are skipped.

        Given: DB active order with no exchange_order_id,
        When: _recover_pending_orders runs,
        Then: Order is skipped.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_db_order(exchange_order_id="")]
        )
        ex.repository = mock_repo
        await ex._recover_pending_orders("kraken")
        assert len(ex.pending_orders) == 0


@pytest.mark.asyncio
async def test_record_venue_event_writes_to_sqlalchemy_repo() -> None:
    """Venue event is written to DB when repository is SQLAlchemyRepository.

    Given: an executor with a mocked SQLAlchemyRepository as repository,
    When: _record_venue_event is called with fill_observed event,
    Then: insert_venue_event is called on the repository.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(return_value=1)
    ex.repository = mock_repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
            "exchange_order_id": "ex-1",
            "client_order_id": "cid-1",
            "side": "buy",
            "status": "filled",
            "fill_price": 50000.0,
            "fill_size": 0.5,
        }
    )
    mock_repo.insert_venue_event.assert_called_once()
    call_dict = mock_repo.insert_venue_event.call_args.args[0]
    assert call_dict["event_type"] == "fill_observed"
    assert call_dict["shard_key"] == "kraken.BTC-USD.live"


@pytest.mark.asyncio
async def test_record_venue_event_paper_strategy_tag_shard_key() -> None:
    """Paper venue event with strategy_tag produces 4-segment shard_key.

    Given: an executor with SQLAlchemyRepository,
    When: _record_venue_event is called with exchange_name=paper and strategy_tag=scalp,
    Then: shard_key is paper.BTC-USD.paper.scalp.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(return_value=1)
    ex.repository = mock_repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "paper",
            "instrument": "BTC-USD",
            "side": "buy",
            "strategy_tag": "scalp",
        }
    )
    mock_repo.insert_venue_event.assert_called_once()
    call_dict = mock_repo.insert_venue_event.call_args.args[0]
    assert call_dict["shard_key"] == "paper.BTC-USD.paper.scalp"


@pytest.mark.asyncio
async def test_record_venue_event_handles_db_error() -> None:
    """Venue event DB write failure is logged but does not propagate.

    Given: an executor with a SQLAlchemyRepository that raises on insert,
    When: _record_venue_event is called,
    Then: no exception propagates to the caller.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(side_effect=RuntimeError("DB down"))
    ex.repository = mock_repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
        }
    )


@pytest.mark.asyncio
async def test_handle_cancellation_records_terminal_venue_event() -> None:
    """Cancellation records an order_terminal venue event.

    Given: an executor with a pending order and SQLAlchemyRepository,
    When: _handle_cancellation is called with a cancelled execution,
    Then: _record_venue_event is called with event_type='order_terminal'.
    """
    ex: Any = MergedDummyExecutor()
    mock_client = AsyncMock()
    mock_client._log_order_update_to_db = AsyncMock()
    ex.exchange_client = mock_client
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(return_value=1)
    ex.repository = mock_repo
    order_req = SimpleNamespace(instrument="BTC-USD", client_order_id="cid-1", strategy_tag=None)
    ex.pending_orders["cid-1"] = base_module.PendingOrderState(
        request=order_req, db_order_id=1, order_public_id="op-1"
    )
    ex.client_by_exchange["ex-1"] = "cid-1"
    execution = SimpleNamespace(exec_type="canceled", symbol="BTC-USD")
    result = await ex._handle_cancellation(execution, "ex-1", "cid-1", "kraken")
    assert result is True
    mock_repo.insert_venue_event.assert_called_once()
    call_dict = mock_repo.insert_venue_event.call_args.args[0]
    assert call_dict["event_type"] == "order_terminal"
    assert call_dict["instrument"] == "BTC-USD"


@pytest.mark.asyncio
async def test_handle_cancellation_without_pending_uses_execution_symbol() -> None:
    """Cancellation for unknown order uses execution symbol for venue event.

    Given: an executor with no pending order for the client_order_id,
    When: _handle_cancellation is called with an expired execution,
    Then: instrument is taken from execution.symbol as fallback.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(return_value=1)
    ex.repository = mock_repo
    ex.exchange_client = None
    execution = SimpleNamespace(exec_type="expired", symbol="ETH-USD")
    result = await ex._handle_cancellation(execution, "ex-99", "cid-99", "kraken")
    assert result is True
    call_dict = mock_repo.insert_venue_event.call_args.args[0]
    assert call_dict["instrument"] == "ETH-USD"
    assert call_dict["status"] == "expired"


@pytest.mark.asyncio
async def test_record_venue_event_fail_closed_in_durable_mode() -> None:
    """VenueEvent write failure raises in durable command mode.

    Given: an executor with use_durable_commands=True and a failing repo,
    When: _record_venue_event is called,
    Then: RuntimeError propagates (fail-closed).
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(side_effect=RuntimeError("DB down"))
    ex.repository = mock_repo
    ex.settings = SimpleNamespace(use_durable_commands=True)
    with pytest.raises(RuntimeError, match="DB down"):
        await ex._record_venue_event(
            {
                "event_type": "fill_observed",
                "exchange_name": "kraken",
                "instrument": "BTC-USD",
            }
        )


def test_create_reconciliation_task_always_none() -> None:
    """Executor reconciliation always returns None (runs in coordinator).

    Given: an executor with any settings,
    When: _create_reconciliation_task is called,
    Then: None is returned because reconciliation runs in coordinator.
    """
    ex: Any = MergedDummyExecutor()
    ex.settings = SimpleNamespace(use_durable_commands=True)
    task = ex._create_reconciliation_task("kraken")
    assert task is None


def test_resolve_fee_prefers_usd_equiv() -> None:
    """Verify _resolve_fee prefers fee_usd_equiv over fees breakdown.

    Given: Execution with fee_usd_equiv=5.0 and fees=[EUR 3.0],
    When: _resolve_fee is called,
    Then: Returns (5.0, "USD").
    """
    execution = SimpleNamespace(
        fee_usd_equiv=5.0,
        fees=[ExecutionFeeBreakdown(asset="EUR", quantity=3.0)],
    )
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (5.0, "USD")


def test_resolve_fee_negative_usd_rebate() -> None:
    """Verify _resolve_fee preserves negative fee_usd_equiv (rebates).

    Given: Execution with fee_usd_equiv=-0.5,
    When: _resolve_fee is called,
    Then: Returns (-0.5, "USD").
    """
    execution = SimpleNamespace(fee_usd_equiv=-0.5, fees=None)
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (-0.5, "USD")


def test_resolve_fee_falls_back_to_single_fee_entry() -> None:
    """Verify _resolve_fee uses fees breakdown when fee_usd_equiv is None.

    Given: Execution with fee_usd_equiv=None and fees=[PLN 2.5],
    When: _resolve_fee is called,
    Then: Returns (2.5, "PLN").
    """
    execution = SimpleNamespace(
        fee_usd_equiv=None,
        fees=[ExecutionFeeBreakdown(asset="PLN", quantity=2.5)],
    )
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (2.5, "PLN")


def test_resolve_fee_multi_asset_uses_first() -> None:
    """Verify _resolve_fee uses first non-zero entry for multi-asset fees.

    Given: Execution with fees=[ETH 0.1, BTC 0.001],
    When: _resolve_fee is called,
    Then: Returns (0.1, "ETH") from first entry.
    """
    execution = SimpleNamespace(
        fee_usd_equiv=None,
        fees=[
            ExecutionFeeBreakdown(asset="ETH", quantity=0.1),
            ExecutionFeeBreakdown(asset="BTC", quantity=0.001),
        ],
    )
    result = base_module.ExchangeExecutorService._resolve_fee(execution)
    assert result == (0.1, "ETH")


def test_resolve_fee_zero_quantity_entry_ignored() -> None:
    """Verify _resolve_fee ignores zero-quantity fee entries.

    Given: Execution with fees=[EUR 0.0],
    When: _resolve_fee is called,
    Then: Returns (0.0, "").
    """
    execution = SimpleNamespace(
        fee_usd_equiv=None,
        fees=[ExecutionFeeBreakdown(asset="EUR", quantity=0.0)],
    )
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (0.0, "")


def test_resolve_fee_no_fee_returns_zero() -> None:
    """Verify _resolve_fee returns zero when no fee data available.

    Given: Execution with no fee data,
    When: _resolve_fee is called,
    Then: Returns (0.0, "").
    """
    execution = SimpleNamespace(fee_usd_equiv=None, fees=None)
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (0.0, "")


def test_resolve_fee_zero_usd_equiv_falls_through() -> None:
    """Verify fee_usd_equiv=0.0 falls through to fees breakdown.

    Given: Execution with fee_usd_equiv=0.0 and fees=[PLN 1.5],
    When: _resolve_fee is called,
    Then: Returns (1.5, "PLN") not (0.0, "USD").
    """
    execution = SimpleNamespace(
        fee_usd_equiv=0.0,
        fees=[ExecutionFeeBreakdown(asset="PLN", quantity=1.5)],
    )
    assert base_module.ExchangeExecutorService._resolve_fee(execution) == (1.5, "PLN")
