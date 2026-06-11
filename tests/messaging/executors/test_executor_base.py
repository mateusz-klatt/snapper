"""Tests for ExchangeExecutorService base class."""

import asyncio
import contextlib
import json
import time as time_module
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
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

import snapper.application.process_manager.launcher as launcher_module
import snapper.messaging.executors.base as base_module
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.kraken import KrakenOrderExecutor
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.topics.builders import order_commands_prefix

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
        leverage=overrides.get("leverage"),
        reduce_only=overrides.get("reduce_only", False),
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
    """Test order handler treats a poison recv as consumed, then stops.

    Given: An executor whose first recv raises ValueError (a poison
        frame, already consumed by ZMQ) and flips running off,
    When: The handler hits the poison branch and re-checks running,
    Then: It exits without re-recv — poison frames never escape to the
        supervisor (transport errors do).
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
    assert len(logged_errors) == 1
    assert "Poison order frame dropped" in logged_errors[0]
    assert call_count == 1


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
    """Test execution handler propagates NotImplementedError.

    Given: A running executor with client that raises NotImplementedError,
    When: subscribe_executions raises NotImplementedError,
    Then: The error propagates so the supervisor can exit permanently.
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
    with pytest.raises(NotImplementedError):
        await ex._execution_handler()


@pytest.mark.asyncio
async def test_execution_handler_runtime_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution handler propagates runtime errors.

    Given: A running executor with client that raises RuntimeError,
    When: subscribe_executions raises RuntimeError,
    Then: The error propagates so the supervisor can log and respawn.
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
    with pytest.raises(RuntimeError, match="ws fail"):
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
async def test_heartbeat_loop_fault_escapes_to_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A heartbeat-loop fault propagates instead of hot-spinning.

    Given: A running executor whose _publish_heartbeat raises (the real
        method self-swallows send errors; a raise here models a genuine
        loop fault such as broken settings access),
    When: The heartbeat loop hits it,
    Then: The error escapes — the supervisor respawns with backoff; the
        old per-iteration catch hot-spun forever while heartbeating
        HEALTHY.
    """
    ex: Any = MergedDummyExecutor()
    ex.running = True
    ex.settings = SimpleNamespace(
        zmq_heartbeat_interval_ms=0,
        zmq_broker_xsub="xsub",
        zmq_broker_xpub="xpub",
    )
    ex._publish_heartbeat = AsyncMock(side_effect=RuntimeError("send fail"))
    with pytest.raises(RuntimeError, match="send fail"):
        await asyncio.wait_for(ex._heartbeat_loop(), timeout=1.0)


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
        mock_exchange_client.get_orders = AsyncMock(return_value=[])
        mock_repository = SimpleNamespace(
            get_active_orders_for_recovery=AsyncMock(return_value=[]),
        )
        raw_socket = zmq_socket_stub(connect=MagicMock(), close=MagicMock())
        context = SimpleNamespace(socket=MagicMock(return_value=raw_socket))
        start_called = asyncio.Event()

        async def mock_order_handler() -> None:
            start_called.set()
            await asyncio.sleep(0.5)

        with (
            patch.object(service, "_order_handler", side_effect=mock_order_handler),
            patch.object(service, "_heartbeat_loop", new=AsyncMock()),
            patch.object(service, "_create_exchange_client", return_value=mock_exchange_client),
            patch.object(base_module, "get_repository", return_value=mock_repository),
            patch.object(
                base_module, "get_settings_service", new=AsyncMock(return_value=MagicMock())
            ),
            patch.object(base_module, "get_settings_with_service", return_value=mock_settings),
            patch.object(base_module.zmq.asyncio, "Context", return_value=context),
            patch.object(
                base_module,
                "ValidatedSubscriber",
                lambda socket: SimpleNamespace(close=socket.close, subscribe=lambda *_: None),
            ),
            patch.object(
                base_module,
                "ValidatedPublisher",
                lambda socket: SimpleNamespace(close=socket.close),
            ),
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
        Then: NotImplementedError propagates to the supervisor.
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
        with pytest.raises(NotImplementedError):
            await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_error(self, mock_get_settings: MagicMock) -> None:
        """Verify execution handler handles connection errors.

        Given: Exchange client that raises error,
        When: Execution handler runs,
        Then: The error propagates so the supervisor can respawn.
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
        with pytest.raises(RuntimeError, match="Connection lost"):
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
    async def test_execute_live_order_exception_propagates(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify live order execution propagates API exceptions.

        Given: Exchange client that raises an exception,
        When: _execute_live_order is called,
        Then: The exception propagates (a blanket swallow here would
            coerce ambiguous failures into the definitive-reject
            path; classification happens in _process_order).
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = AsyncMock()
        mock_exchange_client.create_order = AsyncMock(side_effect=Exception("API error"))
        service_any.exchange_client = mock_exchange_client
        order = self._create_order()
        with pytest.raises(Exception, match="API error"):
            await service_any._execute_live_order(order)

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
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=ExchangeOrderStatusEnum.FILLED,
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
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=ExchangeOrderStatusEnum.CANCELED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CANCELED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
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
        Then: NotImplementedError propagates to the supervisor.
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
        with pytest.raises(NotImplementedError):
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

    def _get_exchange_name(self) -> Literal["kraken", "walutomat", "paper"]:
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
            patch.object(service, "_supervise_execution_stream", new=AsyncMock()),
            patch.object(service, "_supervise_loop", new=_passthrough_supervise_loop()),
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
        Then: NotImplementedError propagates to the supervisor.
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
        with pytest.raises(NotImplementedError):
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
        Then: The error propagates so the supervisor can log and respawn.
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
        with pytest.raises(RuntimeError, match="Connection lost"):
            await service_any._execution_handler()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_execution_handler_silent_when_not_running(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify execution handler silent when not running.

        Given: Service not running,
        When: RuntimeError is raised,
        Then: The error still propagates — the supervisor owns the
            running check and exits without respawn.
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
        with pytest.raises(RuntimeError, match="Connection lost"):
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
async def test_order_handler_transport_error_escapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport-level recv failure propagates to the supervisor.

    Given: A running executor whose subscriber raises a non-poison error,
    When: recv_multipart raises,
    Then: The error escapes _order_handler — the supervisor owns the
        respawn (and rebuilds the SUB socket first); swallowing it here
        used to hot-spin on a dead socket forever.
    """
    executor = DummyExecutorSimple()
    executor.running = True

    class FailingSubscriber:
        async def recv_multipart(self) -> tuple[str, bytes]:
            raise RuntimeError("boom")

    executor.subscriber = cast(Any, FailingSubscriber())
    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(executor._order_handler(), timeout=0.1)


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
    """Test execution handler propagates errors while running.

    Given: A running executor with client raising error,
    When: subscribe_executions raises ValueError,
    Then: The error propagates so the supervisor can log and respawn.
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
    with pytest.raises(ValueError, match="ws error"):
        await executor._execution_handler()


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
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.1,
            price=None,
            status=ExchangeOrderStatusEnum.OPEN,
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
        mock_exchange_client = MagicMock()
        mock_exchange_client.__aenter__ = AsyncMock(return_value=mock_exchange_client)
        mock_exchange_client.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(service, "_order_handler", new=AsyncMock(return_value=None)),
            patch.object(service, "_supervise_execution_stream", new=AsyncMock(return_value=None)),
            patch.object(service, "_supervise_loop", new=AsyncMock(return_value=None)),
            patch.object(service, "_heartbeat_loop", new=AsyncMock(return_value=None)),
            patch.object(service, "_reconciliation_handler", new=AsyncMock(return_value=None)),
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
        """Verify live order execution propagates errors.

        Given: Exchange client that raises exception,
        When: Order is executed,
        Then: The exception propagates to the caller (classification
            happens in _process_order).
        """
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        service = KrakenOrderExecutor()
        service_any = cast(Any, service)
        mock_exchange_client = MagicMock()
        service_any.exchange_client = mock_exchange_client
        order = self._create_order(mode="live")
        mock_exchange_client.create_order = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            await service_any._execute_live_order(order)

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
        Then: NotImplementedError propagates to the supervisor.
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
        with pytest.raises(NotImplementedError):
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.FILLED,
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
                service, "_supervise_execution_stream", new=AsyncMock(return_value=None)
            ) as execution_mock,
            patch.object(service, "_supervise_loop", new=_passthrough_supervise_loop()),
            patch.object(
                service, "_reconciliation_handler", new=AsyncMock(return_value=None)
            ) as recon_mock,
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
        recon_mock.assert_awaited_once()
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

        Given: Order for different exchange (walutomat vs kraken),
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
        wrong_order = self._create_order(exchange="walutomat")
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
        """Verify execution handler propagates iterator exceptions.

        Given: Exchange client that raises ValueError,
        When: Execution handler runs,
        Then: The exception propagates so the supervisor can respawn.
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
        with pytest.raises(ValueError, match="boom"):
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
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=ExchangeOrderStatusEnum.OPEN,
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
        """Provide mocked AppSettings without per-exchange credential properties.

        Credentials live in wallet_credentials and are
        loaded by ``CredentialResolver`` at executor startup. Tests that
        need a credential-less executor path use ``with_credentials=False``
        to document intent; the field is otherwise unused because
        ``_create_exchange_client`` reads from ``self._credentials``
        directly, not from AppSettings.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.db_url = TEST_DB_URL
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
            patch.object(service, "_supervise_execution_stream", new_callable=AsyncMock),
            patch.object(service, "_supervise_loop", new_callable=AsyncMock),
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
            patch.object(service, "_supervise_execution_stream", new_callable=AsyncMock),
            patch.object(service, "_supervise_loop", new_callable=AsyncMock),
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
        """Verify live order execution propagates venue exceptions.

        Given: Exchange client that raises exception,
        When: Order is executed,
        Then: The exception propagates and no pending entry appears
            (classification happens in _process_order).
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
        with pytest.raises(Exception, match="Insufficient balance"):
            await service._execute_live_order(order)
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
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=ExchangeOrderStatusEnum.FILLED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.PARTIALLY_FILLED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CANCELED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.FILLED,
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
        """Verify NotImplementedError reaches the caller of the handler.

        Given: Client that raises NotImplementedError,
        When: Execution handler runs,
        Then: The error propagates — the supervisor turns it into a
            permanent exit for venues that cannot stream executions.
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
        with pytest.raises(NotImplementedError):
            await service_any._execution_handler()


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
            '"exchange":"walutomat","instrument":"EUR-PLN",'
            '"exchange_order_id":"WALUTOMAT-123","client_order_id":"client_456","new_price":200000.0}'
        )
        await service_any._handle_replace_command(payload, "kraken", "EUR-PLN")
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
        mock_result.status = ExchangeOrderStatusEnum.CANCELED
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
        mock_result.status = ExchangeOrderStatusEnum.CANCELED
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
        mock_result.status = ExchangeOrderStatusEnum.OPEN
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
    """Tests for delta fill fields and order status cleanup."""

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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
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
        Then: last_size is computed as cum_qty minus last_seen_cum_qty,
            and the seen cumulative is NOT advanced — that registration
            belongs to the post-publish step in _process_execution so a
            failed publish never poisons the dedupe gate.
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50050.0,
        )
        _topic, fill = ex._build_execution_data(execution, "ex-2", order, "kraken")
        assert fill.size == pytest.approx(0.5)
        assert fill.last_size == pytest.approx(0.2)
        assert fill.last_price == pytest.approx(50050.0)
        assert ex.pending_orders["delta-2"].last_seen_cum_qty == pytest.approx(0.3)

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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CLOSED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CLOSED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CANCELED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.EXPIRED,
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

    @pytest.mark.asyncio
    async def test_publish_order_status_propagates_leverage_and_reduce_only(self) -> None:
        """Verify leverage and reduce_only are copied from request to OrderData event.

        Given: Running executor with publisher and an order request that
            carries leverage=3 and reduce_only=True,
        When: _publish_order_status is called,
        Then: The published OrderData event preserves both fields so the
            frontend / DB can render shorts and reduce-only intent correctly.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.msg_publisher = AsyncMock()
        order = make_order(quantity=1.0, leverage=3, reduce_only=True)
        await ex._publish_order_status(order, "submitted")
        ex.msg_publisher.send.assert_awaited_once()
        published = ex.msg_publisher.send.call_args[0][1]
        assert published.leverage == 3
        assert published.reduce_only is True

    @pytest.mark.asyncio
    async def test_publish_order_status_defaults_leverage_and_reduce_only(self) -> None:
        """Verify leverage/reduce_only default to None/False on the event.

        Given: An order request with no leverage and reduce_only=False,
        When: _publish_order_status is called,
        Then: The published OrderData event mirrors the defaults — leverage
            is None and reduce_only is False — so spot orders are not falsely
            tagged as margin or reduce-only.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.msg_publisher = AsyncMock()
        order = make_order(quantity=1.0)
        await ex._publish_order_status(order, "submitted")
        ex.msg_publisher.send.assert_awaited_once()
        published = ex.msg_publisher.send.call_args[0][1]
        assert published.leverage is None
        assert published.reduce_only is False


class TestExecutorBasePersistence:
    """Tests for DB persistence in executor base."""

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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CLOSED,
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
        assert call_args.kwargs["status"] == ExchangeOrderStatusEnum.CLOSED

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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=0.5,
            average_price=50000.0,
            last_qty=0.5,
            last_price=50000.0,
            fee_usd_equiv=0.25,
        )
        await ex._process_execution(execution)
        call_args = mock_client._log_order_update_to_db.call_args
        assert call_args.kwargs["status"] == ExchangeOrderStatusEnum.OPEN
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CLOSED,
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
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC),
        )
        result = await ex._handle_cancellation(
            execution, "ex-cancel-db", order.client_order_id, "kraken"
        )
        assert result is True
        mock_client._log_order_update_to_db.assert_awaited_once()
        call_args = mock_client._log_order_update_to_db.call_args
        assert call_args.kwargs["db_order_id"] == 99
        assert call_args.kwargs["status"] == ExchangeOrderStatusEnum.CANCELED

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
        cancel_result.status = ExchangeOrderStatusEnum.CANCELED
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


def _passthrough_supervise_loop() -> Any:
    """Build a supervisor stand-in that runs the wrapped loop exactly once.

    Start() tests patch the supervisor so a mocked loop's immediate clean
    return is not treated as a death (the real supervisor would respawn
    it with live backoff and the gather would never finish).
    """

    async def _run_once(
        task_label: str,
        attempt: Callable[[], Awaitable[None]],
        pre_respawn: Callable[[], None] | None = None,
    ) -> None:
        await attempt()

    return _run_once


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
    """Tests for executor startup recovery."""

    @pytest.mark.asyncio
    async def test_recover_pending_from_exchange_open_order(self) -> None:
        """Open order recovers with PLANE-honest seeds, not venue truth.

        Given: Exchange reports an open order with filled=0.3 while the
            published plane (executions) proves nothing was published,
        When: _recover_pending_orders runs,
        Then: The pending entry seeds last_seen from the published plane
            (0.0) and the venue-ahead gap is handed to the corrective
            machinery instead of being silently re-baselined away.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        snap = SimpleNamespace(
            id="ex-1",
            filled=0.3,
            remaining=0.7,
            status=ExchangeOrderStatusEnum.OPEN,
            db_order_id=None,
            db_order_public_id=None,
        )
        mock_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_db_order(exchange_order_id="ex-1", filled_size=0.3)]
        )
        mock_repo.get_executions_for_order = AsyncMock(return_value=[])
        ex.repository = mock_repo
        ex._reconcile_fill_gap = AsyncMock()
        await ex._recover_pending_orders("kraken")
        assert "c1" in ex.pending_orders
        pending = ex.pending_orders["c1"]
        assert pending.exchange_order_id == "ex-1"
        assert pending.last_seen_cum_qty == pytest.approx(0.0)
        assert pending.last_recorded_cum_qty == pytest.approx(0.0)
        assert "ex-1" in ex.client_by_exchange
        ex._reconcile_fill_gap.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recover_db_order_missing_on_exchange_closed(self) -> None:
        """A terminal-during-downtime order is healed, projected, and popped.

        Given: DB says open, exchange says CLOSED with filled=1.0 and
            nothing was published pre-crash,
        When: _recover_pending_orders runs,
        Then: The downtime fill gap is handed to the corrective machinery
            and the terminal execution is emitted through the pipeline
            (which owns the pop and the status projection) — the old
            broken direct status write (sequence_id passed as PK) is gone.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        closed_snap = SimpleNamespace(
            id="ex-1",
            filled=1.0,
            remaining=0.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )
        mock_client.get_order = AsyncMock(return_value=closed_snap)
        mock_client._log_order_update_to_db = AsyncMock()
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_db_order(exchange_order_id="ex-1")]
        )
        mock_repo.get_executions_for_order = AsyncMock(return_value=[])
        ex.repository = mock_repo
        ex._reconcile_fill_gap = AsyncMock()
        ex._emit_disappeared_terminal = AsyncMock(
            side_effect=lambda *a, **k: ex.pending_orders.pop("c1", None)
        )
        await ex._recover_pending_orders("kraken")
        assert "c1" not in ex.pending_orders
        ex._reconcile_fill_gap.assert_awaited_once()
        ex._emit_disappeared_terminal.assert_awaited_once()
        mock_client._log_order_update_to_db.assert_not_awaited()

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
                status=ExchangeOrderStatusEnum.OPEN,
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
        """An unverifiable order is PARKED with DB seeds, not dropped.

        Given: DB active order, exchange get_order raises,
        When: _recover_pending_orders runs,
        Then: The order is registered in pending with DB-derived seeds
            and no emission — recon retries it every cycle instead of
            the order silently vanishing from tracking forever.
        """
        ex: Any = MergedDummyExecutor()
        mock_client = AsyncMock()
        mock_client.get_orders = AsyncMock(return_value=[])
        mock_client.get_order = AsyncMock(side_effect=RuntimeError("not found"))
        ex.exchange_client = mock_client
        mock_repo = AsyncMock()
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_make_db_order()])
        mock_repo.get_executions_for_order = AsyncMock(return_value=[])
        ex.repository = mock_repo
        ex._reconcile_fill_gap = AsyncMock()
        await ex._recover_pending_orders("kraken")
        assert "c1" in ex.pending_orders
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(0.0)
        ex._reconcile_fill_gap.assert_not_awaited()

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
async def test_record_venue_event_skips_when_required_identifiers_missing() -> None:
    """Venue event write is skipped when required identifiers are missing.

    Given: an executor with SQLAlchemyRepository,
    When: _record_venue_event is called without required identifying fields,
    Then: insert_venue_event is not called.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(return_value=1)
    ex.repository = mock_repo

    await ex._record_venue_event(
        {
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
        }
    )

    mock_repo.insert_venue_event.assert_not_called()


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
async def test_record_venue_event_fail_closed() -> None:
    """VenueEvent write failure always fail-closes the executor.

    Given:
        An executor wired to a failing ``SQLAlchemyRepository``.

    When:
        ``_record_venue_event`` is called.

    Then:
        ``RuntimeError`` propagates unchanged — the durable-only path
        requires every venue event to persist before the executor
        acknowledges it.
    """
    ex: Any = MergedDummyExecutor()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.insert_venue_event = AsyncMock(side_effect=RuntimeError("DB down"))
    ex.repository = mock_repo
    ex.settings = SimpleNamespace()
    with pytest.raises(RuntimeError, match="DB down"):
        await ex._record_venue_event(
            {
                "event_type": "fill_observed",
                "exchange_name": "kraken",
                "instrument": "BTC-USD",
            }
        )


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


def test_resolve_fill_quantities_accumulates_when_only_last_qty_present() -> None:
    """Verify cum_qty falls back to ``prev_cum + last_qty`` when SDK omits cum_qty.

    Given: An ExecutionUpdate with ``cum_qty=None`` and ``last_qty != None``
        (the partial-fill shape some venues produce — only the per-event
        delta is reported, the running cumulative must be reconstructed),
    When: ``_resolve_fill_quantities`` is called with a non-zero
        ``prev_cum``,
    Then: Returns the additive cumulative ``prev_cum + last_qty`` and
        forwards ``last_qty/last_price`` as the per-event delta.
    """
    execution = ExecutionUpdate(
        order_id="ex-1",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        cum_qty=None,
        last_qty=0.4,
        last_price=100.0,
        average_price=100.0,
    )
    cum_qty, delta_size, delta_price, avg_price = (
        base_module.ExchangeExecutorService._resolve_fill_quantities(execution, prev_cum=0.6)
    )
    assert cum_qty == pytest.approx(1.0)
    assert delta_size == pytest.approx(0.4)
    assert delta_price == pytest.approx(100.0)
    assert avg_price == pytest.approx(100.0)


class TestWalletScopedExecutor:
    """Per-wallet executor: credential resolver + message filter.

    Covers the new ``wallet_public_id`` constructor parameter, the
    ``_resolve_credentials`` startup hook, and the ``_is_for_my_wallet``
    routing filter on incoming command messages. Both the legacy
    backwards-compat path (empty ``wallet_public_id``) and the
    The per-wallet path is exercised.
    """

    def test_default_parameters_include_wallet_public_id(self) -> None:
        """``get_default_parameters`` advertises wallet_public_id default.

        Given: A fresh ``AppSettings`` instance,
        When: ``ExchangeExecutorService.get_default_parameters`` runs,
        Then: The returned dict includes ``wallet_public_id=""`` so the
            process launcher knows the parameter exists.
        """
        defaults = ExchangeExecutorService.get_default_parameters(cast(Any, MagicMock()))
        assert defaults == {"wallet_public_id": ""}

    def test_init_stores_empty_wallet_by_default(self) -> None:
        """Default constructor preserves the legacy empty-wallet behaviour."""
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor()
        assert executor.wallet_public_id == ""
        assert executor._credentials is None

    def test_init_stores_wallet_public_id_when_provided(self) -> None:
        """Constructor accepts an explicit wallet_public_id."""
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
        assert executor.wallet_public_id == "019d5a8b3c7d4e5f"
        assert executor._credentials is None

    @pytest.mark.asyncio
    async def test_resolve_credentials_returns_immediately_when_wallet_empty(
        self,
    ) -> None:
        """Empty wallet_public_id keeps the legacy fallback path active.

        Given: An executor with the default empty wallet_public_id,
        When: ``_resolve_credentials`` is called,
        Then: It returns without consulting the repository and leaves
            ``self._credentials`` as None so concrete
            ``_create_exchange_client`` falls back to ``AppSettings``.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor()
        executor.repository = MagicMock()
        await executor._resolve_credentials("kraken")
        assert executor._credentials is None

    @pytest.mark.asyncio
    async def test_resolve_credentials_raises_when_repository_missing(self) -> None:
        """Lookup fails fast when called before repository init.

        Given: An executor with wallet_public_id but no repository,
        When: ``_resolve_credentials`` is called,
        Then: A ``RuntimeError`` is raised before any DB call so the
            startup ordering bug surfaces immediately.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
        executor.repository = None
        with pytest.raises(RuntimeError, match="repository initialization"):
            await executor._resolve_credentials("kraken")

    @pytest.mark.asyncio
    async def test_resolve_credentials_populates_credentials_dict(self) -> None:
        """Per-wallet startup loads credentials via CredentialResolver.

        Given: An executor with a non-empty wallet_public_id and an
            initialized repository,
        When: ``_resolve_credentials`` is called,
        Then: It instantiates ``CredentialResolver``, fetches the row
            for the wallet+exchange pair, and stores the decrypted
            envelope on ``self._credentials``.
        """
        envelope = SimpleNamespace(
            api_key_value="resolver-public-id",
            api_secret_value="resolver-signing-blob",
        )
        expected_envelope = {
            "api_key": envelope.api_key_value,
            "api_secret": envelope.api_secret_value,
        }
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
        executor.repository = MagicMock()
        resolver_instance = MagicMock()
        resolver_instance.get_credentials = AsyncMock(return_value=expected_envelope)
        with patch.object(
            base_module, "CredentialResolver", return_value=resolver_instance
        ) as resolver_cls:
            await executor._resolve_credentials("kraken")
        resolver_cls.assert_called_once_with(executor.repository)
        resolver_instance.get_credentials.assert_awaited_once_with(
            exchange="kraken", wallet_public_id="019d5a8b3c7d4e5f"
        )
        assert executor._credentials == expected_envelope

    def test_is_for_my_wallet_accepts_everything_in_legacy_mode(self) -> None:
        """Empty wallet_public_id never filters — legacy behaviour preserved.

        Given: An executor with the default empty wallet_public_id,
        When: ``_is_for_my_wallet`` is called with any message,
        Then: It always returns True so existing tests and the
            single-wallet template path keep routing every command.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor()
        msg = SimpleNamespace(wallet_public_id="some-other-wallet")
        assert executor._is_for_my_wallet(msg) is True
        msg_no_attr = SimpleNamespace()
        assert executor._is_for_my_wallet(msg_no_attr) is True

    def test_is_for_my_wallet_drops_messages_for_other_wallets(self) -> None:
        """Filter drops cross-wallet commands silently.

        Given: An executor bound to wallet ``aaa``,
        When: ``_is_for_my_wallet`` is called with a message tagged
            ``bbb``,
        Then: It returns False so the executor skips processing.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="aaa")
        msg = SimpleNamespace(wallet_public_id="bbb")
        assert executor._is_for_my_wallet(msg) is False

    def test_is_for_my_wallet_accepts_messages_for_my_wallet(self) -> None:
        """Filter accepts commands matching its own wallet.

        Given: An executor bound to wallet ``aaa``,
        When: ``_is_for_my_wallet`` is called with a matching message,
        Then: It returns True so the executor proceeds with processing.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="aaa")
        msg = SimpleNamespace(wallet_public_id="aaa")
        assert executor._is_for_my_wallet(msg) is True

    def test_is_for_my_wallet_drops_messages_with_no_wallet_field(self) -> None:
        """Filter rejects messages missing wallet_public_id.

        Given: An executor bound to wallet ``aaa``,
        When: ``_is_for_my_wallet`` is called with a message that has
            no ``wallet_public_id`` field (legacy strategy emission
            that has not been updated yet),
        Then: It returns False because the executor cannot prove
            the message belongs to its wallet.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="aaa")
        msg = SimpleNamespace()
        assert executor._is_for_my_wallet(msg) is False

    def test_is_for_my_wallet_treats_none_wallet_field_as_unset(self) -> None:
        """Explicit None on wallet_public_id is treated as empty string.

        Given: An executor bound to wallet ``aaa``,
        When: ``_is_for_my_wallet`` is called with
            ``wallet_public_id=None`` (the schema default for
            backwards-compat envelopes),
        Then: It returns False because None != "aaa".
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="aaa")
        msg = SimpleNamespace(wallet_public_id=None)
        assert executor._is_for_my_wallet(msg) is False

    @pytest.mark.asyncio
    async def test_handle_submit_drops_message_for_other_wallet(self) -> None:
        """Submit handler skips _process_order when wallet does not match.

        Given: An executor bound to wallet ``mine``,
        When: ``_handle_submit_command`` receives an OrderRequestData
            tagged with ``wallet_public_id="other"``,
        Then: ``_process_order`` is never invoked, exercising the
            early-return branch in the submit handler.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="mine")
        process_mock = AsyncMock()
        cast(Any, executor)._process_order = process_mock
        payload = json.dumps(
            {
                "type": "order_request",
                "session_id": "s",
                "sequence_id": 1,
                "public_id": "abc",
                "timestamp": "2026-04-08T00:00:00Z",
                "strategy_id": "manual",
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "mode": "live",
                "side": "buy",
                "order_type": "market",
                "quantity": 0.1,
                "client_order_id": "co1",
                "wallet_public_id": "other",
            }
        )
        await cast(Any, executor)._handle_submit_command(payload, "kraken", "BTC-USD")
        process_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handle_cancel_drops_message_for_other_wallet(self) -> None:
        """Cancel handler skips _process_cancel when wallet does not match.

        Given: An executor bound to wallet ``mine``,
        When: ``_handle_cancel_command`` receives an OrderCancelData
            tagged with ``wallet_public_id="other"``,
        Then: ``_process_cancel`` is never invoked.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="mine")
        cancel_mock = AsyncMock()
        cast(Any, executor)._process_cancel = cancel_mock
        payload = json.dumps(
            {
                "type": "order_cancel",
                "session_id": "s",
                "sequence_id": 1,
                "public_id": "abc",
                "timestamp": "2026-04-08T00:00:00Z",
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "exchange_order_id": "ex-123",
                "client_order_id": "co1",
                "wallet_public_id": "other",
            }
        )
        await cast(Any, executor)._handle_cancel_command(payload, "kraken", "BTC-USD")
        cancel_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handle_replace_drops_message_for_other_wallet(self) -> None:
        """Replace handler skips _process_replace when wallet does not match.

        Given: An executor bound to wallet ``mine``,
        When: ``_handle_replace_command`` receives an OrderReplaceData
            tagged with ``wallet_public_id="other"``,
        Then: ``_process_replace`` is never invoked.
        """
        with patch("snapper.config.settings.get_settings") as mock_get_settings:
            mock_get_settings.return_value = MagicMock(db_url=TEST_DB_URL)
            executor = KrakenOrderExecutor(wallet_public_id="mine")
        replace_mock = AsyncMock()
        cast(Any, executor)._process_replace = replace_mock
        payload = json.dumps(
            {
                "type": "order_replace",
                "session_id": "s",
                "sequence_id": 1,
                "public_id": "abc",
                "timestamp": "2026-04-08T00:00:00Z",
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "exchange_order_id": "ex-123",
                "client_order_id": "co1",
                "new_quantity": 0.2,
                "wallet_public_id": "other",
            }
        )
        await cast(Any, executor)._handle_replace_command(payload, "kraken", "BTC-USD")
        replace_mock.assert_not_awaited()


class TestAmbiguousSubmitHandling:
    """Money-safety pins for the UNKNOWN ambiguous-submit path.

    An ambiguous venue failure (the order MAY exist) must never reach
    the REJECTED path: a fabricated rejection clears the engine's
    in-flight intent and a re-emitted replacement can double real
    exposure. These tests pin the executor side of that contract.
    """

    def _executor(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build a running dummy executor with tracked publish/record mocks."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._publish_order_status = AsyncMock()
        ex._record_venue_event = AsyncMock()
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        return ex

    @pytest.mark.asyncio
    async def test_ambiguous_submit_parks_unknown_never_rejects(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ambiguous failure publishes UNKNOWN, keeps pending, never REJECTED.

        Given: _execute_live_order raising AmbiguousOrderSubmitError,
        When: _process_order runs,
        Then: SUBMITTED then UNKNOWN are published (no REJECTED), an
            order_submit_unknown venue event is recorded (never
            order_rejected), and the pending entry is parked with
            submit_ambiguous set.
        """
        ex = self._executor(monkeypatch)
        ex._execute_live_order = AsyncMock(
            side_effect=AmbiguousOrderSubmitError(
                client_order_id="c1", instrument="BTC-USD", message="timeout after send"
            )
        )
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "unknown"]
        event_types = [c.args[0]["event_type"] for c in ex._record_venue_event.await_args_list]
        assert event_types == ["order_submit_unknown"]
        pending = ex.pending_orders[order.client_order_id]
        assert pending.submit_ambiguous is True
        assert pending.unknown_published is True

    @pytest.mark.asyncio
    async def test_unknown_published_only_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Repeated ambiguous handling publishes a single UNKNOWN event.

        Given: A pending entry that already published UNKNOWN,
        When: _handle_ambiguous_submit runs again for the same order,
        Then: No second UNKNOWN publish is emitted.
        """
        ex = self._executor(monkeypatch)
        order = make_order()
        error = AmbiguousOrderSubmitError(
            client_order_id=order.client_order_id, instrument="BTC-USD", message="boom"
        )
        await ex._handle_ambiguous_submit(order, error)
        await ex._handle_ambiguous_submit(order, error)
        unknown_publishes = [
            c for c in ex._publish_order_status.await_args_list if c.args[1] == "unknown"
        ]
        assert len(unknown_publishes) == 1

    @pytest.mark.asyncio
    async def test_unknown_published_even_when_venue_event_write_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing durable write must not block the UNKNOWN publish.

        Given: _record_venue_event raising (likely the same outage),
        When: _handle_ambiguous_submit runs,
        Then: UNKNOWN is still published so the engine guard holds.
        """
        ex = self._executor(monkeypatch)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = make_order()
        await ex._handle_ambiguous_submit(
            order,
            AmbiguousOrderSubmitError(
                client_order_id=order.client_order_id, instrument="BTC-USD", message="boom"
            ),
        )
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["unknown"]

    @pytest.mark.asyncio
    async def test_accept_db_failure_never_publishes_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A DB blip on a venue-accepted order must not fabricate REJECTED.

        Given: The venue accepted (exchange_order_id returned) but the
            durable order_accepted write raises,
        When: _process_order runs,
        Then: ACCEPTED is still published, the pending entry survives
            with accept_event_pending set for recon retry, the
            exchange-id mapping exists, and REJECTED never appears.
        """
        ex = self._executor(monkeypatch)
        ex._execute_live_order = AsyncMock(return_value="ex-live-9")
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]
        pending = ex.pending_orders[order.client_order_id]
        assert pending.accept_event_pending is True
        assert order.client_order_id in ex._unhealed_accept_events
        assert ex.client_by_exchange["ex-live-9"] == order.client_order_id

    @pytest.mark.asyncio
    async def test_definitive_venue_reject_still_rejects(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty exchange id remains the definitive-reject signal.

        Given: _execute_live_order returning None (venue said no),
        When: _process_order runs,
        Then: REJECTED is published and the pending entry is dropped.
        """
        ex = self._executor(monkeypatch)
        ex._execute_live_order = AsyncMock(return_value=None)
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "rejected"]
        assert order.client_order_id not in ex.pending_orders
        event_types = [c.args[0]["event_type"] for c in ex._record_venue_event.await_args_list]
        assert event_types == ["order_rejected"]

    @pytest.mark.asyncio
    async def test_accept_db_failure_without_pending_entry_still_accepts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Acceptance finalization survives a missing pending entry.

        Given: A venue-accepted order whose pending entry vanished
            (e.g. a racing cancel) AND a failing durable write,
        When: _finalize_accepted_submit runs,
        Then: ACCEPTED is still published and the id mapping exists —
            the flag write is simply skipped.
        """
        ex = self._executor(monkeypatch)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        order = make_order()
        await ex._finalize_accepted_submit(order, "ex-live-10")
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["accepted"]
        assert ex.client_by_exchange["ex-live-10"] == order.client_order_id
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_unknown_publish_failure_keeps_flag_unset_for_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing UNKNOWN publish never claims success.

        Given: _publish_order_status reporting failure on every retry,
        When: _handle_ambiguous_submit runs,
        Then: unknown_published stays False (a later retouch can retry)
            and all three bounded attempts were made.
        """
        ex = self._executor(monkeypatch)
        ex._publish_order_status = AsyncMock(return_value=False)
        monkeypatch.setattr(base_module.asyncio, "sleep", AsyncMock())
        order = make_order()
        await ex._handle_ambiguous_submit(
            order,
            AmbiguousOrderSubmitError(
                client_order_id=order.client_order_id, instrument="BTC-USD", message="boom"
            ),
        )
        pending = ex.pending_orders[order.client_order_id]
        assert pending.unknown_published is False
        assert ex._publish_order_status.await_count == 3

    @pytest.mark.asyncio
    async def test_unknown_publish_retries_until_confirmed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transient publish failure is retried to a confirmed send.

        Given: _publish_order_status failing once then succeeding,
        When: _handle_ambiguous_submit runs,
        Then: unknown_published is True after the second attempt.
        """
        ex = self._executor(monkeypatch)
        ex._publish_order_status = AsyncMock(side_effect=[False, True])
        monkeypatch.setattr(base_module.asyncio, "sleep", AsyncMock())
        order = make_order()
        await ex._handle_ambiguous_submit(
            order,
            AmbiguousOrderSubmitError(
                client_order_id=order.client_order_id, instrument="BTC-USD", message="boom"
            ),
        )
        pending = ex.pending_orders[order.client_order_id]
        assert pending.unknown_published is True
        assert ex._publish_order_status.await_count == 2

    @pytest.mark.asyncio
    async def test_accept_publish_failure_never_rejects(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing ACCEPTED publish must not fabricate any rejection.

        Given: A venue-accepted order whose ACCEPTED publish fails,
        When: _process_order runs,
        Then: No REJECTED publish happens and the pending entry with
            its exchange-id mapping survives for fills/recon.
        """
        ex = self._executor(monkeypatch)
        ex._publish_order_status = AsyncMock(return_value=False)
        ex._execute_live_order = AsyncMock(return_value="ex-live-11")
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]
        assert ex.client_by_exchange["ex-live-11"] == order.client_order_id
        assert order.client_order_id in ex.pending_orders


class TestAmbiguousVerification:
    """Venue-truth verification of ambiguous submits."""

    def _executor(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build a running executor with a verifying exchange client."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._publish_order_status = AsyncMock(return_value=True)
        ex._record_venue_event = AsyncMock()
        ex._reconcile_disappeared_order = AsyncMock()
        ex.exchange_client = MagicMock()
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        monkeypatch.setattr(base_module.asyncio, "sleep", AsyncMock())
        return ex

    def _ambiguous(self, order: OrderRequestData) -> AmbiguousOrderSubmitError:
        """Build the ambiguous error for the given order."""
        return AmbiguousOrderSubmitError(
            client_order_id=order.client_order_id, instrument="BTC-USD", message="boom"
        )

    @pytest.mark.asyncio
    async def test_found_open_resolves_to_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A live order found by client id resolves as accepted.

        Given: The venue returning an OPEN snapshot for the client id,
        When: _handle_ambiguous_submit runs,
        Then: ACCEPTED is published with the venue id, no UNKNOWN and
            no REJECTED appear, and the ambiguous flag is cleared.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-ver-1", status=base_module.ExchangeOrderStatusEnum.OPEN
            )
        )
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["accepted"]
        assert ex.client_by_exchange["ex-ver-1"] == order.client_order_id
        pending = ex.pending_orders[order.client_order_id]
        assert pending.submit_ambiguous is False
        assert pending.exchange_order_id == "ex-ver-1"
        ex._reconcile_disappeared_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_found_terminal_routes_through_reconciler(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An already-terminal order projects via the disappeared-order path.

        Given: The venue returning a CLOSED snapshot for the client id,
        When: _handle_ambiguous_submit runs,
        Then: ACCEPTED is published and _reconcile_disappeared_order is
            invoked so fills and the terminal event project normally.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-ver-2", status=base_module.ExchangeOrderStatusEnum.CLOSED
            )
        )
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["accepted"]
        ex._reconcile_disappeared_order.assert_awaited_once()
        assert ex._reconcile_disappeared_order.await_args.args[1] == "ex-ver-2"

    @pytest.mark.asyncio
    async def test_two_consecutive_not_found_rejects_safely(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two authoritative absences make the rejection venue-truth-based.

        Given: The venue answering authoritative-absent twice,
        When: _handle_ambiguous_submit runs,
        Then: REJECTED is published, the order_rejected venue event
            carries the verified-absent error, and the entry is gone.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["rejected"]
        assert order.client_order_id not in ex.pending_orders
        event = ex._record_venue_event.await_args_list[-1].args[0]
        assert event["event_type"] == "order_rejected"
        assert "venue verified order absent" in event["error"]
        assert ex.exchange_client.find_order_by_client_id.await_count == 2
        assert ex.client_by_exchange == {}

    @pytest.mark.asyncio
    async def test_single_not_found_after_error_does_not_reject(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A lone absence answer between failures is not enough to reject.

        Given: Verification answering error, absent, error,
        When: _handle_ambiguous_submit runs,
        Then: The order parks UNKNOWN — one not-found surrounded by
            failures never converts into a rejection.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            side_effect=[RuntimeError("down"), None, RuntimeError("down")]
        )
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["unknown"]
        assert ex.pending_orders[order.client_order_id].submit_ambiguous is True

    @pytest.mark.asyncio
    async def test_unsupported_lookup_parks_immediately(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A venue without client-id lookup parks without burning retries.

        Given: find_order_by_client_id raising NotImplementedError,
        When: _handle_ambiguous_submit runs,
        Then: Exactly one lookup attempt happens and the order parks.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            side_effect=NotImplementedError("no lookup")
        )
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["unknown"]
        assert ex.exchange_client.find_order_by_client_id.await_count == 1

    @pytest.mark.asyncio
    async def test_all_attempts_failing_parks_unknown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unreachable verification parks the order after three attempts.

        Given: Every lookup raising (same outage as the submit),
        When: _handle_ambiguous_submit runs,
        Then: Three attempts are made and the order parks UNKNOWN.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(side_effect=TimeoutError())
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["unknown"]
        assert ex.exchange_client.find_order_by_client_id.await_count == 3

    @pytest.mark.asyncio
    async def test_found_open_flushes_orphans(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A live verified order flushes buffered orphan executions.

        Given: A FOUND-open verification result,
        When: _handle_ambiguous_submit resolves it,
        Then: Orphan flushing runs for the venue order id (normal
            acceptance semantics).
        """
        ex = self._executor(monkeypatch)
        ex._try_process_orphaned = MagicMock()
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-ver-3", status=base_module.ExchangeOrderStatusEnum.OPEN
            )
        )
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        ex._try_process_orphaned.assert_called_once_with("ex-ver-3", order.client_order_id)

    @pytest.mark.asyncio
    async def test_found_terminal_drains_orphan_inline_before_reconcile(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A terminal verified order drains its orphan fill inline first.

        Given: A FOUND-CLOSED verification result with a buffered
            orphan execution for the venue order id,
        When: _handle_ambiguous_submit resolves it,
        Then: The buffered WS fill is processed synchronously BEFORE
            the REST reconciler runs — full fill fidelity (exec ids,
            fees) is preserved and no background task can interleave
            with the synthetic terminal projection.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-ver-4", status=base_module.ExchangeOrderStatusEnum.CLOSED
            )
        )
        call_order: list[str] = []

        async def record_execution(_execution: Any) -> None:
            call_order.append("orphan-fill")

        async def record_reconcile(*_args: Any) -> None:
            call_order.append("reconcile")

        ex._process_execution = record_execution
        ex._reconcile_disappeared_order = AsyncMock(side_effect=record_reconcile)
        orphan = SimpleNamespace(order_id="ex-ver-4", exec_type="trade")
        ex.orphaned_executions["ex-ver-4"] = (orphan, 0.0)
        order = make_order()
        await ex._handle_ambiguous_submit(order, self._ambiguous(order))
        assert call_order == ["orphan-fill", "reconcile"]
        assert "ex-ver-4" not in ex.orphaned_executions


class TestDuplicateSubmitGuard:
    """Replayed dispatches never reach the venue twice."""

    def _executor(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build a running executor with tracked submit-path mocks."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._publish_order_status = AsyncMock(return_value=True)
        ex._record_venue_event = AsyncMock()
        ex._execute_live_order = AsyncMock(return_value="ex-dup-1")
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        return ex

    @pytest.mark.asyncio
    async def test_pending_duplicate_is_dropped_and_state_preserved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A same-process replay never overwrites live pending state.

        Given: A parked-UNKNOWN pending entry with fill tracking for
            the client id,
        When: A replayed dispatch of the same id arrives,
        Then: Nothing is published, the venue is never called, and the
            ORIGINAL entry object with its ambiguous/fill state
            survives untouched.
        """
        ex = self._executor(monkeypatch)
        order = make_order()
        original = base_module.PendingOrderState(request=order)
        original.submit_ambiguous = True
        original.last_seen_cum_qty = 0.7
        ex.pending_orders[order.client_order_id] = original
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()
        survivor = ex.pending_orders[order.client_order_id]
        assert survivor is original
        assert survivor.submit_ambiguous is True
        assert survivor.last_seen_cum_qty == pytest.approx(0.7)

    @pytest.mark.asyncio
    async def test_unhealed_accept_duplicate_is_dropped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An accepted order awaiting durable heal blocks its replay.

        Given: The client id queued in _unhealed_accept_events with no
            pending entry (terminal fill popped it),
        When: A replayed dispatch arrives,
        Then: The command is dropped silently.
        """
        ex = self._executor(monkeypatch)
        order = make_order()
        ex._unhealed_accept_events[order.client_order_id] = {"event_type": "order_accepted"}
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_durable_evidence_drops_crash_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fresh process drops a replay backed by venue-event evidence.

        Given: Empty in-memory state and a repository reporting durable
            submit evidence for the id,
        When: The crash-replayed dispatch arrives,
        Then: The command is dropped before any publish or venue call.
        """
        ex = self._executor(monkeypatch)
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.has_order_submit_evidence = AsyncMock(return_value=True)
        repo.has_venue_event = AsyncMock(return_value=False)
        ex.repository = repo
        order = make_order()
        await ex._process_order(order)
        repo.has_order_submit_evidence.assert_awaited_once_with(order.client_order_id)
        ex._publish_order_status.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_breaker_evidence_replay_reruns_disposition(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A replay with breaker-open evidence reruns the terminal release.

        Given: Durable evidence whose breaker probe answers True (an
            executor crashed between the breaker event write and the
            REJECTED publish — the in-memory retry queue died with it),
        When: The redispatched frame arrives,
        Then: Instead of a silent drop the disposition reruns
            idempotently — no duplicate event write, the command row
            CAS-es FAILED, and REJECTED publishes so the engine intent
            releases deterministically; the venue is never touched.
        """
        ex = self._executor(monkeypatch)
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.has_order_submit_evidence = AsyncMock(return_value=True)
        repo.has_venue_event = AsyncMock(return_value=True)
        repo.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
        ex.repository = repo
        ex._record_venue_event = AsyncMock()
        order = make_order()
        await ex._process_order(order)
        ex._record_venue_event.assert_not_awaited()
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["rejected"]
        ex._execute_live_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_evidence_proceeds_with_normal_submit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fresh command with clean history submits normally.

        Given: A repository reporting no durable evidence,
        When: The dispatch arrives,
        Then: The full submit flow runs (SUBMITTED then ACCEPTED).
        """
        ex = self._executor(monkeypatch)
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.has_order_submit_evidence = AsyncMock(return_value=False)
        ex.repository = repo
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]
        ex._execute_live_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_sqlalchemy_repository_skips_durable_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a SQLAlchemy repository the durable check is skipped.

        Given: A plain mock repository (not SQLAlchemyRepository),
        When: The dispatch arrives,
        Then: The submit proceeds (same exposure model as the durable
            venue-event writes).
        """
        ex = self._executor(monkeypatch)
        ex.repository = MagicMock()
        order = make_order()
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]

    @pytest.mark.asyncio
    async def test_evidence_check_failure_drops_fail_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing durable check drops the command, never submits.

        Given: The evidence query raising (DB blip),
        When: The dispatch arrives,
        Then: FAIL-CLOSED — dropped with an ERROR log; a true duplicate
            would double a MARKET position while a fresh command
            re-emits via the engine timeout valve.
        """
        ex = self._executor(monkeypatch)
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.has_order_submit_evidence = AsyncMock(side_effect=RuntimeError("db down"))
        ex.repository = repo
        order = make_order()
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()


class TestStaleCommandGate:
    """Outage-backlog frames reject before any venue call."""

    def _executor(self, monkeypatch: pytest.MonkeyPatch, ttl: float = 30.0) -> Any:
        """Build a running executor with the TTL configured.

        The venue client answers authoritative absence by default so
        the reject path is reachable; adoption/unverifiable tests
        override the lookup.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.settings = SimpleNamespace(trade_command_dispatch_ttl_s=ttl)
        ex._publish_order_status = AsyncMock(return_value=True)
        ex._record_venue_event = AsyncMock()
        ex._execute_live_order = AsyncMock(return_value="ex-stale-1")
        ex.exchange_client = MagicMock()
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        monkeypatch.setattr(base_module, "is_tradeable", lambda _sym, _exch: True)
        return ex

    @pytest.mark.asyncio
    async def test_stale_frame_rejects_before_venue(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A frame older than the TTL rejects without touching the venue.

        Given: An order whose signaled_at is far beyond the TTL,
        When: _process_order runs,
        Then: REJECTED publishes with a stale-command venue event and
            create_order is never reached; no pending entry appears.
        """
        ex = self._executor(monkeypatch)
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["rejected"]
        event = ex._record_venue_event.await_args.args[0]
        assert event["event_type"] == "order_rejected"
        assert "stale command" in event["error"]
        ex._execute_live_order.assert_not_awaited()
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_stale_reject_publish_failure_writes_no_terminal_event(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed REJECTED publish never records the terminal row.

        Given: A stale frame with verified venue absence whose REJECTED
            publish returns False,
        When: _process_order runs,
        Then: The frame is consumed WITHOUT an order_rejected venue
            event — a terminal row before a confirmed publish would
            exempt the command from the dispatched-verification sweep
            while the engine guard stays held forever.
        """
        ex = self._executor(monkeypatch)
        ex._publish_order_status = AsyncMock(return_value=False)
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        await ex._process_order(order)
        ex._record_venue_event.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_fresh_frame_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A frame inside the TTL submits normally."""
        ex = self._executor(monkeypatch)
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=1))
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]

    @pytest.mark.asyncio
    async def test_frame_without_age_anchor_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Legacy frames without signaled_at skip the gate."""
        ex = self._executor(monkeypatch)
        order = make_order(signaled_at=None)
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]

    @pytest.mark.asyncio
    async def test_disabled_ttl_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """TTL <= 0 disables the gate entirely."""
        ex = self._executor(monkeypatch, ttl=0.0)
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=9_000))
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]

    @pytest.mark.asyncio
    async def test_unparseable_settings_disable_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Absent or non-numeric settings disable the gate rather than crash.

        Given: Settings lacking the TTL attribute, then carrying a
            non-numeric value,
        When: A very old frame arrives in each case,
        Then: The gate stays disabled and the submit proceeds.
        """
        ex = self._executor(monkeypatch)
        ex.settings = SimpleNamespace()
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=9_000))
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]
        ex._publish_order_status.reset_mock()
        ex.settings = SimpleNamespace(trade_command_dispatch_ttl_s="not-a-number")
        ex.pending_orders.clear()
        await ex._process_order(make_order(client_order_id="c-unparse-2"))
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["submitted", "accepted"]

    @pytest.mark.asyncio
    async def test_stale_duplicate_drops_silently_not_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The duplicate guard outranks the stale gate — ordering invariant.

        Given: A stale frame whose client id is already pending (a
            replayed dispatch of a live order),
        When: _process_order runs,
        Then: It drops SILENTLY as a duplicate — TTL-rejecting it would
            publish REJECTED for a possibly-live order (exactly the
            fabricated-terminal-state failure the UNKNOWN state exists
            to prevent).
        """
        ex = self._executor(monkeypatch)
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._record_venue_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_replay_of_placed_order_is_adopted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stale frame whose order EXISTS on the venue adopts, never rejects.

        Given: A stale frame and the venue answering with a live order
            for the client id (crash-window replay whose unknown-event
            durable write also failed — no evidence for the duplicate
            guard),
        When: _process_order runs,
        Then: The order is adopted as accepted; REJECTED never appears.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            return_value=SimpleNamespace(
                id="ex-stale-live", status=base_module.ExchangeOrderStatusEnum.OPEN
            )
        )
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        await ex._process_order(order)
        statuses = [c.args[1] for c in ex._publish_order_status.await_args_list]
        assert statuses == ["accepted"]
        assert ex.client_by_exchange["ex-stale-live"] == order.client_order_id
        ex._execute_live_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_unverifiable_drops_silently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An unverifiable stale frame drops without fabricating REJECTED.

        Given: A stale frame and a venue lookup that raises (outage or
            unsupported venue),
        When: _process_order runs,
        Then: Nothing publishes — rejecting without venue truth could
            fabricate a terminal state for a live order; the reconciler
            WARN and engine valve cover the silence.
        """
        ex = self._executor(monkeypatch)
        ex.exchange_client.find_order_by_client_id = AsyncMock(
            side_effect=NotImplementedError("no lookup")
        )
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._record_venue_event.assert_not_awaited()
        ex._execute_live_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_without_exchange_client_drops_silently(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stale frame with no venue client to ask drops silently."""
        ex = self._executor(monkeypatch)
        ex.exchange_client = None
        order = make_order(signaled_at=datetime.now(tz=UTC) - timedelta(seconds=300))
        await ex._process_order(order)
        ex._publish_order_status.assert_not_awaited()
        ex._record_venue_event.assert_not_awaited()


class TestSupervisedExecutionStream:
    """Tests for the execution-stream supervisor respawn semantics."""

    def _executor(self) -> Any:
        """Build a running executor with a streaming-capable client stub."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = MagicMock()
        ex.exchange_client.supports_websocket_executions = True
        ex._sleep_with_jitter = AsyncMock()
        return ex

    @pytest.mark.asyncio
    async def test_respawns_after_deaths_then_propagates_cancel(self) -> None:
        """Death by exception and clean return both respawn; cancel exits.

        Given: A handler that dies twice (exception, then clean return)
            and is then cancelled,
        When: The supervisor runs,
        Then: It respawns after each death with doubled backoff, counts
            both respawns, and lets CancelledError propagate.
        """
        ex = self._executor()
        ex._execution_handler = AsyncMock(
            side_effect=[ConnectionError("ws lost"), None, asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_execution_stream()
        assert ex._execution_handler.await_count == 3
        sleep_args = [c.args[0] for c in ex._sleep_with_jitter.await_args_list]
        assert sleep_args == [1.0, 2.0]
        assert ex._exec_stream_restarts == 2

    @pytest.mark.asyncio
    async def test_not_implemented_exits_permanently(self) -> None:
        """A venue that cannot stream executions stops supervision for good.

        Given: A handler raising NotImplementedError,
        When: The supervisor runs,
        Then: It returns after one attempt with no backoff sleep and no
            respawn count.
        """
        ex = self._executor()
        ex._execution_handler = AsyncMock(side_effect=NotImplementedError())
        await ex._supervise_execution_stream()
        assert ex._execution_handler.await_count == 1
        ex._sleep_with_jitter.assert_not_awaited()
        assert ex._exec_stream_restarts == 0

    @pytest.mark.asyncio
    async def test_exits_when_not_running(self) -> None:
        """Shutdown before the first attempt never touches the handler."""
        ex = self._executor()
        ex.running = False
        ex._execution_handler = AsyncMock()
        await ex._supervise_execution_stream()
        ex._execution_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exits_when_client_missing(self) -> None:
        """A missing exchange client makes supervision pointless."""
        ex = self._executor()
        ex.exchange_client = None
        ex._execution_handler = AsyncMock()
        await ex._supervise_execution_stream()
        ex._execution_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exits_when_ws_unsupported(self) -> None:
        """A venue without WS executions support exits the supervisor."""
        ex = self._executor()
        ex.exchange_client.supports_websocket_executions = False
        ex._execution_handler = AsyncMock()
        await ex._supervise_execution_stream()
        ex._execution_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_clean_return_after_stop_exits_without_respawn(self) -> None:
        """A clean handler return during shutdown is not treated as death.

        Given: A handler whose pass ends because running was cleared,
        When: The supervisor inspects the clean return,
        Then: It exits without logging a respawn or sleeping.
        """
        ex = self._executor()

        async def stop_and_return() -> None:
            ex.running = False

        ex._execution_handler = AsyncMock(side_effect=stop_and_return)
        await ex._supervise_execution_stream()
        ex._sleep_with_jitter.assert_not_awaited()
        assert ex._exec_stream_restarts == 0

    @pytest.mark.asyncio
    async def test_backoff_doubles_to_cap(self) -> None:
        """Backoff follows 1,2,4,...,cap and stays at the cap.

        Given: A handler that keeps dying,
        When: Eight respawns happen,
        Then: Sleeps follow the doubling schedule capped at 60s.
        """
        ex = self._executor()
        ex._execution_handler = AsyncMock(side_effect=ConnectionError("down"))
        sleeps: list[float] = []

        async def record_sleep(delay_s: float) -> None:
            sleeps.append(delay_s)
            if len(sleeps) >= 8:
                ex.running = False

        ex._sleep_with_jitter = AsyncMock(side_effect=record_sleep)
        await ex._supervise_execution_stream()
        assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]

    @pytest.mark.asyncio
    async def test_healthy_runtime_resets_backoff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A long-lived stream's death restarts backoff from the initial value.

        Given: Two quick deaths (backoff grows to 4s), then a stream that
            lived past the healthy-runtime threshold,
        When: The third death is handled,
        Then: The backoff resets to the initial 1s instead of continuing
            the doubling from the old incident.
        """
        ex = self._executor()
        ex._execution_handler = AsyncMock(side_effect=ConnectionError("down"))
        clock = iter([0.0, 1.0, 10.0, 11.0, 20.0, 321.0])
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: next(clock)))
        sleeps: list[float] = []

        async def record_sleep(delay_s: float) -> None:
            sleeps.append(delay_s)
            if len(sleeps) >= 3:
                ex.running = False

        ex._sleep_with_jitter = AsyncMock(side_effect=record_sleep)
        await ex._supervise_execution_stream()
        assert sleeps == [1.0, 2.0, 1.0]

    @pytest.mark.asyncio
    async def test_sleep_with_jitter_bounds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Jitter scales the delay within ±20 percent.

        Given: random.random pinned to its extremes,
        When: _sleep_with_jitter runs,
        Then: The actual sleep argument is delay*1.2 and delay*0.8.
        """
        ex: Any = MergedDummyExecutor()
        recorded: list[float] = []

        async def fake_sleep(delay: float) -> None:
            recorded.append(delay)

        monkeypatch.setattr(base_module.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(base_module.random, "random", lambda: 1.0)
        await ex._sleep_with_jitter(10.0)
        monkeypatch.setattr(base_module.random, "random", lambda: 0.0)
        await ex._sleep_with_jitter(10.0)
        assert recorded == [pytest.approx(12.0), pytest.approx(8.0)]

    @pytest.mark.asyncio
    async def test_start_spawns_supervisor_not_bare_handler(self) -> None:
        """start() wires the stream through the supervisor task."""
        ex: Any = MergedDummyExecutor()
        assert hasattr(ex, "_supervise_execution_stream")
        assert hasattr(ex, "_exec_stream_restarts")
        assert ex._exec_stream_restarts == 0


class TestFillDedupe:
    """Tests for the executor-level fill dedupe gate."""

    def _executor(self, quantity: float = 2.0) -> tuple[Any, Any]:
        """Build a running executor with one pending order mapped to ex1."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=quantity)
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex1"] = order.client_order_id
        ex._publish_execution = AsyncMock()
        ex._record_venue_event = AsyncMock()
        return ex, order

    @staticmethod
    def _fill(
        exec_id: str | None,
        last_qty: float | None,
        cum_qty: float | None,
        last_price: float | None = 100.0,
    ) -> SimpleNamespace:
        """Build a minimal execution frame correlated to ex1."""
        return SimpleNamespace(
            order_id="ex1",
            exec_type="trade",
            exec_id=exec_id,
            order_status=None,
            cum_qty=cum_qty,
            average_price=None,
            fee_usd_equiv=None,
            fees=None,
            last_qty=last_qty,
            last_price=last_price,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            side=SimpleNamespace(value="buy"),
            trade_id=None,
            liquidity_ind=None,
        )

    @pytest.mark.asyncio
    async def test_duplicate_exec_id_dropped_futures_shape(self) -> None:
        """The same delta fill redelivered (no cum_qty) books exactly once.

        Given: A futures-shaped fill (fill_id, last_qty, no cum_qty)
            delivered twice,
        When: Both frames are processed,
        Then: One publish, one venue event, and the cumulative advances
            once — the redelivery is absorbed by the exec-id LRU.
        """
        ex, order = self._executor()
        await ex._process_execution(self._fill("f1", 0.5, None))
        await ex._process_execution(self._fill("f1", 0.5, None))
        assert ex._publish_execution.await_count == 1
        assert ex._record_venue_event.await_count == 1
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5

    @pytest.mark.asyncio
    async def test_replayed_cum_dropped_with_fresh_exec_id(self) -> None:
        """A non-advancing cum_qty is dropped even under a new exec id.

        Given: A spot-shaped fill published, then a replay carrying the
            same cumulative under a NEVER-seen exec id,
        When: The replay is processed,
        Then: The cum-monotonic guard drops it — this is the cross-key
            hole the exec-id LRU alone cannot close.
        """
        ex, order = self._executor()
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        await ex._process_execution(self._fill("b2", 0.5, 0.5))
        assert ex._publish_execution.await_count == 1
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5

    @pytest.mark.asyncio
    async def test_advancing_fill_passes(self) -> None:
        """A genuinely advancing cumulative is published normally."""
        ex, order = self._executor(quantity=3.0)
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        await ex._process_execution(self._fill("a2", 0.5, 1.0))
        assert ex._publish_execution.await_count == 2
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 1.0

    @pytest.mark.asyncio
    async def test_status_only_frame_passes_despite_seen_exec_id(self) -> None:
        """Status frames carry no quantity and are never deduped."""
        ex, _order = self._executor()
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        await ex._process_execution(self._fill("a1", None, None))
        assert ex._publish_execution.await_count == 2

    @pytest.mark.asyncio
    async def test_failed_publish_keeps_redelivery_alive(self) -> None:
        """Neither dedupe key survives a publisher send failure.

        Given: A first delivery whose REAL _publish_execution reports
            failure because msg_publisher.send raised,
        When: The venue redelivers the same fill,
        Then: Nothing was committed (publish watermark unmoved, exec id
            unregistered), so the redelivery is processed and published
            instead of being mistaken for a replay; its durable row
            carries a zero gap because the first row already persisted.
        """
        ex, order = self._executor()
        del ex._publish_execution
        ex.msg_publisher = SimpleNamespace(
            send=AsyncMock(side_effect=[RuntimeError("zmq down"), None])
        )
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.0
        assert "a1" not in ex._seen_exec_ids
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex.msg_publisher.send.await_count == 2
        assert "a1" in ex._seen_exec_ids
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5
        durable_sizes = [c.args[0]["fill_size"] for c in ex._record_venue_event.await_args_list]
        assert durable_sizes == [pytest.approx(0.5), pytest.approx(0.0)]
        assert sum(durable_sizes) == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_failed_venue_event_keeps_redelivery_alive(self) -> None:
        """A FAIL-CLOSED venue-event write failure must not mark seen."""
        ex, order = self._executor()
        ex._record_venue_event = AsyncMock(side_effect=[RuntimeError("db down"), None])
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex._publish_execution.await_count == 0
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex._publish_execution.await_count == 1
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5

    @pytest.mark.asyncio
    async def test_exec_id_none_fill_still_cum_guarded(self) -> None:
        """Fills without exec ids fall through to the cumulative guard."""
        ex, order = self._executor()
        await ex._process_execution(self._fill(None, 0.5, 0.5))
        await ex._process_execution(self._fill(None, 0.5, 0.5))
        assert ex._publish_execution.await_count == 1
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5

    @pytest.mark.asyncio
    async def test_lru_evicts_oldest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The seen-exec-id LRU stays bounded by evicting oldest entries."""
        ex: Any = MergedDummyExecutor()
        monkeypatch.setattr(base_module, "_SEEN_EXEC_IDS_MAX", 2)
        ex._register_seen_exec_id("e1")
        ex._register_seen_exec_id("e2")
        ex._register_seen_exec_id("e3")
        assert list(ex._seen_exec_ids) == ["e2", "e3"]
        ex._register_seen_exec_id(None)
        assert list(ex._seen_exec_ids) == ["e2", "e3"]

    @pytest.mark.asyncio
    async def test_registration_tolerates_popped_pending(self) -> None:
        """Exec-id registration survives a concurrently popped order.

        Given: A publish during which the pending entry disappears,
        When: The post-publish registration runs,
        Then: The exec id is still registered (the reservation taken
            before the pop is simply moot).
        """
        ex, order = self._executor()

        async def publish_and_pop(topic: str, fill: Any) -> bool:
            ex.pending_orders.pop(order.client_order_id, None)
            return True

        ex._publish_execution = AsyncMock(side_effect=publish_and_pop)
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert "a1" in ex._seen_exec_ids

    @pytest.mark.asyncio
    async def test_fill_lock_excludes_concurrent_same_cum_twin(self) -> None:
        """A paused in-flight fill still excludes its concurrent twin.

        Given: A recon corrective holding the order's fill lock, paused
            inside its durable venue-event write,
        When: The live stream delivers the SAME cumulative under a fresh
            exec id during the pause,
        Then: The live twin parks on the lock and is gate-dropped once the
            corrective commits — exactly one fill is published.
        """
        ex, order = self._executor()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def pausing_record(params: Any) -> None:
            entered.set()
            await release.wait()

        ex._record_venue_event = AsyncMock(side_effect=pausing_record)
        corrective = asyncio.create_task(ex._process_execution(self._fill("recon-1", 0.5, 0.5)))
        await entered.wait()
        ex._record_venue_event = AsyncMock()
        live = asyncio.create_task(ex._process_execution(self._fill("live-1", 0.5, 0.5)))
        await asyncio.sleep(0.01)
        assert ex._publish_execution.await_count == 0
        release.set()
        await asyncio.gather(corrective, live)
        assert ex._publish_execution.await_count == 1
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.5

    @pytest.mark.asyncio
    async def test_unpublished_gap_absorbed_by_next_cum_fill(self) -> None:
        """A later cum-carrying fill absorbs a lost predecessor's quantity.

        Given: Fill A (cum 0.5) whose publish failed — nothing committed,
        When: Fill B (cum 1.0, venue last_qty 0.5) arrives before A's
            redelivery,
        Then: B's published delta is anchored to the COMMITTED cumulative
            (last_size 1.0, not 0.5), so the delta-applying engine ends up
            exactly at venue truth, and A's late redelivery is dropped as
            already-absorbed.
        """
        ex, order = self._executor(quantity=3.0)
        del ex._publish_execution
        ex.msg_publisher = SimpleNamespace(
            send=AsyncMock(side_effect=[RuntimeError("zmq down"), None, None])
        )
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.0
        await ex._process_execution(self._fill("b2", 0.5, 1.0))
        published = ex.msg_publisher.send.await_args_list[1].args[1]
        assert published.last_size == pytest.approx(1.0)
        assert published.size == pytest.approx(1.0)
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 1.0
        durable_sizes = [c.args[0]["fill_size"] for c in ex._record_venue_event.await_args_list]
        assert durable_sizes == [pytest.approx(0.5), pytest.approx(0.5)]
        assert sum(durable_sizes) == pytest.approx(published.size)
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex.msg_publisher.send.await_count == 2

    @pytest.mark.asyncio
    async def test_record_failure_gap_absorbed_durably_by_next_fill(self) -> None:
        """A lost durable row's quantity lands in the successor's row.

        Given: Fill A whose durable venue-event write raised (no row
            persisted, nothing committed),
        When: Fill B (cum 1.0) arrives and persists,
        Then: B's durable fill_size is the gap from the DURABLE watermark
            (1.0, not the venue's 0.5) so additive checkpoint replay
            recovers exactly venue truth — the sibling of the
            publish-failure case, anchored to the other watermark.
        """
        ex, order = self._executor(quantity=3.0)
        ex._record_venue_event = AsyncMock(side_effect=[RuntimeError("db down"), None])
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex._publish_execution.await_count == 0
        assert ex.pending_orders[order.client_order_id].last_recorded_cum_qty == 0.0
        await ex._process_execution(self._fill("b2", 0.5, 1.0))
        durable_row = ex._record_venue_event.await_args_list[1].args[0]
        assert durable_row["fill_size"] == pytest.approx(1.0)
        assert durable_row["cum_fill_size"] == pytest.approx(1.0)
        assert ex.pending_orders[order.client_order_id].last_recorded_cum_qty == 1.0
        published = ex._publish_execution.await_args_list[0].args[1]
        assert published.last_size == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_fill_books_on_captured_holder_when_popped_mid_section(self) -> None:
        """An entry popped inside the locked section still books safely.

        Given: The pending entry vanishes while _handle_cancellation runs
            under the order's captured fill_lock,
        When: The fill proceeds to booking,
        Then: It publishes normally — committed state is simply absent
            (the pop carried the final lifecycle truth).
        """
        ex, order = self._executor()

        async def pop_and_pass(*args: Any, **kwargs: Any) -> bool:
            ex.pending_orders.pop(order.client_order_id, None)
            return False

        ex._handle_cancellation = AsyncMock(side_effect=pop_and_pass)
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex._publish_execution.await_count == 1

    @pytest.mark.asyncio
    async def test_resolve_pop_race_drops_frame_defensively(self) -> None:
        """A frame whose entry vanished at resolve time is dropped whole.

        Given: _resolve_execution_order returned a correlation whose
            pending entry no longer exists (defensive guard),
        When: _process_execution looks up the fill-lock holder,
        Then: The frame is dropped — redelivery or recon re-orphans it.
        """
        ex, order = self._executor()
        ex._resolve_execution_order = MagicMock(return_value=("ex1", "ghost-cid", order))
        await ex._process_execution(self._fill("a1", 0.5, 0.5))
        assert ex._publish_execution.await_count == 0

    @pytest.mark.asyncio
    async def test_zero_qty_fill_commits_nothing(self) -> None:
        """A zero-quantity delta frame advances no committed cumulative."""
        ex, order = self._executor()
        await ex._process_execution(self._fill("z1", 0.0, None))
        assert ex.pending_orders[order.client_order_id].last_seen_cum_qty == 0.0
        assert ex._publish_execution.await_count == 1


class TestExecutionHandlerStreamClose:
    """Tests for deterministic stream finalization in the handler."""

    @pytest.mark.asyncio
    async def test_non_generator_stream_completes_without_aclose(self) -> None:
        """A plain async iterator (test double) needs no aclose call.

        Given: A subscribe_executions returning a non-generator iterator
            that completes cleanly,
        When: _execution_handler consumes it,
        Then: The handler finishes without attempting aclose on it.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True

        class PlainIterator:
            def __aiter__(self) -> PlainIterator:
                return self

            async def __anext__(self) -> ExecutionUpdate:
                raise StopAsyncIteration

        class Client:
            supports_websocket_executions = True

            def subscribe_executions(self) -> PlainIterator:
                return PlainIterator()

        ex.exchange_client = Client()
        await ex._execution_handler()

    @pytest.mark.asyncio
    async def test_generator_stream_acloses_on_break(self) -> None:
        """A real async generator is aclosed when the handler stops early.

        Given: A generator-backed stream and running flipped off after the
            first message,
        When: The handler breaks out of iteration,
        Then: The generator's finalizer runs before the handler returns —
            the seam that lets a supervisor respawn without a stale
            generator's deferred cleanup racing the fresh stream.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._process_execution = AsyncMock()
        finalized: list[str] = []

        class Client:
            supports_websocket_executions = True

            def subscribe_executions(self) -> Any:
                async def _gen() -> Any:
                    try:
                        while True:
                            ex.running = False
                            yield SimpleNamespace()
                    finally:
                        finalized.append("closed")

                return _gen()

        ex.exchange_client = Client()
        await ex._execution_handler()
        assert finalized == ["closed"]


class TestReconInterplay:
    """Tests for recon-lock serialization and the post-reconnect heal."""

    @pytest.mark.asyncio
    async def test_recon_lock_serializes_cycles(self) -> None:
        """Two concurrent recon callers never interleave their cycles.

        Given: A first cycle stalled inside the lock,
        When: A second caller invokes _reconcile_with_exchange,
        Then: It only enters after the first completes — preventing two
            cycles from double-emitting the same corrective fill.
        """
        ex: Any = MergedDummyExecutor()
        entered = asyncio.Event()
        release = asyncio.Event()
        order: list[str] = []

        async def stalled_cycle() -> None:
            order.append("first-in")
            entered.set()
            await release.wait()
            order.append("first-out")

        ex._reconcile_with_exchange_unlocked = AsyncMock(side_effect=stalled_cycle)
        first = asyncio.create_task(ex._reconcile_with_exchange())
        await entered.wait()

        async def second_cycle() -> None:
            order.append("second-in")

        ex._reconcile_with_exchange_unlocked = AsyncMock(side_effect=second_cycle)
        second = asyncio.create_task(ex._reconcile_with_exchange())
        await asyncio.sleep(0.01)
        assert "second-in" not in order
        release.set()
        await asyncio.gather(first, second)
        assert order == ["first-in", "first-out", "second-in"]

    @pytest.mark.asyncio
    async def test_post_death_recon_runs_before_reentry(self) -> None:
        """A respawn heals the dark window before resubscribing.

        Given: A stream that dies once and is then cancelled,
        When: The supervisor re-enters,
        Then: _post_reconnect_reconcile runs after the death and before
            the second handler attempt; the first attempt has no recon.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = MagicMock()
        ex.exchange_client.supports_websocket_executions = True
        ex._sleep_with_jitter = AsyncMock()
        calls: list[str] = []

        async def dying_handler() -> None:
            calls.append("handler")
            if len([c for c in calls if c == "handler"]) >= 2:
                raise asyncio.CancelledError()
            raise ConnectionError("down")

        async def recon() -> None:
            calls.append("recon")

        ex._execution_handler = AsyncMock(side_effect=dying_handler)
        ex._post_reconnect_reconcile = AsyncMock(side_effect=recon)
        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_execution_stream()
        assert calls == ["handler", "recon", "handler"]

    @pytest.mark.asyncio
    async def test_post_reconnect_reconcile_swallows_failure(self) -> None:
        """A failing heal never blocks the resubscribe attempt."""
        ex: Any = MergedDummyExecutor()
        ex._reconcile_with_exchange = AsyncMock(side_effect=RuntimeError("venue down"))
        await ex._post_reconnect_reconcile()
        ex._reconcile_with_exchange.assert_awaited_once()


class TestDeltaOnlyDurableAnchoring:
    """Tests for futures-style delta-only frames vs the durable plane."""

    @pytest.mark.asyncio
    async def test_delta_only_successor_writes_venue_delta(self) -> None:
        """A delta-only successor's row carries the venue delta, not a gap.

        Given: Futures fill A (delta 0.5, no cum) whose row persisted but
            whose publish failed, then DISTINCT fill B (delta 0.5),
        When: B is booked,
        Then: B's durable row writes the venue's own 0.5 (a watermark gap
            computed from B's FABRICATED cumulative would write zero and
            underbook exec-id-deduped additive replay) — rows sum to
            venue truth 1.0 even though the live engine catches up only
            via A's redelivery or the recon corrective.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=3.0)
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex1"] = order.client_order_id
        ex._record_venue_event = AsyncMock()
        ex.msg_publisher = SimpleNamespace(
            send=AsyncMock(side_effect=[RuntimeError("zmq down"), None])
        )

        def frame(exec_id: str) -> SimpleNamespace:
            return SimpleNamespace(
                order_id="ex1",
                exec_type="trade",
                exec_id=exec_id,
                order_status=None,
                cum_qty=None,
                average_price=None,
                fee_usd_equiv=None,
                fees=None,
                last_qty=0.5,
                last_price=100.0,
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                side=SimpleNamespace(value="buy"),
                trade_id=None,
                liquidity_ind=None,
            )

        await ex._process_execution(frame("f-a"))
        await ex._process_execution(frame("f-b"))
        durable_sizes = [c.args[0]["fill_size"] for c in ex._record_venue_event.await_args_list]
        assert durable_sizes == [pytest.approx(0.5), pytest.approx(0.5)]
        pending = ex.pending_orders[order.client_order_id]
        assert pending.last_seen_cum_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_cancellation_parks_on_fill_lock(self) -> None:
        """A cancel frame cannot pop the order mid-booking.

        Given: A fill booking holding the order's fill_lock, paused in
            its durable write,
        When: A cancellation frame for the same order arrives,
        Then: The cancel parks on the lock and pops only after the
            booking committed — the in-flight fill is never stranded by
            a concurrent lifecycle pop.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=3.0)
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex1"] = order.client_order_id
        ex._publish_execution = AsyncMock()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def pausing_record(params: Any) -> None:
            entered.set()
            await release.wait()

        ex._record_venue_event = AsyncMock(side_effect=pausing_record)
        fill_frame = SimpleNamespace(
            order_id="ex1",
            exec_type="trade",
            exec_id="f1",
            order_status=None,
            cum_qty=0.5,
            average_price=None,
            fee_usd_equiv=None,
            fees=None,
            last_qty=0.5,
            last_price=100.0,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            side=SimpleNamespace(value="buy"),
            trade_id=None,
            liquidity_ind=None,
        )
        booking = asyncio.create_task(ex._process_execution(fill_frame))
        await entered.wait()
        ex._record_venue_event = AsyncMock()
        cancel_frame = SimpleNamespace(
            order_id="ex1",
            exec_type="canceled",
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
            liquidity_ind=None,
        )
        cancel = asyncio.create_task(ex._process_execution(cancel_frame))
        await asyncio.sleep(0.01)
        assert order.client_order_id in ex.pending_orders
        release.set()
        await asyncio.gather(booking, cancel)
        assert ex._publish_execution.await_count == 1
        assert order.client_order_id not in ex.pending_orders


class TestRestCancelLifecycleLock:
    """Tests for the REST cancel path's serialized lifecycle pop."""

    @pytest.mark.asyncio
    async def test_rest_cancel_parks_on_fill_lock(self) -> None:
        """A REST cancel cannot pop the order mid-booking.

        Given: A fill booking holding the order's fill_lock, paused in
            its durable write,
        When: _process_cancel gets a CANCELED venue result for the same
            order,
        Then: Its lifecycle pop parks on the lock and runs only after the
            booking committed — the in-flight fill is published, not
            stranded as an orphan.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=3.0)
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex1"] = order.client_order_id
        ex._publish_execution = AsyncMock()
        ex._publish_cancel_event = AsyncMock()
        ex.exchange_client = MagicMock()
        ex.exchange_client.cancel_order = AsyncMock(
            return_value=SimpleNamespace(status=ExchangeOrderStatusEnum.CANCELED)
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def pausing_record(params: Any) -> None:
            entered.set()
            await release.wait()

        ex._record_venue_event = AsyncMock(side_effect=pausing_record)
        fill_frame = SimpleNamespace(
            order_id="ex1",
            exec_type="trade",
            exec_id="f1",
            order_status=None,
            cum_qty=0.5,
            average_price=None,
            fee_usd_equiv=None,
            fees=None,
            last_qty=0.5,
            last_price=100.0,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            side=SimpleNamespace(value="buy"),
            trade_id=None,
            liquidity_ind=None,
        )
        booking = asyncio.create_task(ex._process_execution(fill_frame))
        await entered.wait()
        ex._record_venue_event = AsyncMock()
        cancel_data = SimpleNamespace(exchange_order_id="ex1", instrument="BTC-USD")
        cancel = asyncio.create_task(ex._process_cancel(cancel_data))
        await asyncio.sleep(0.01)
        assert order.client_order_id in ex.pending_orders
        release.set()
        await asyncio.gather(booking, cancel)
        assert ex._publish_execution.await_count == 1
        assert order.client_order_id not in ex.pending_orders
        assert "ex1" not in ex.client_by_exchange

    @pytest.mark.asyncio
    async def test_rest_cancel_without_tracked_order_cleans_mapping(self) -> None:
        """A cancel for an untracked order still clears the id mapping."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._publish_cancel_event = AsyncMock()
        ex.exchange_client = MagicMock()
        ex.exchange_client.cancel_order = AsyncMock(
            return_value=SimpleNamespace(status=ExchangeOrderStatusEnum.CANCELED)
        )
        ex.client_by_exchange["ex-gone"] = "cid-gone"
        cancel_data = SimpleNamespace(exchange_order_id="ex-gone", instrument="BTC-USD")
        await ex._process_cancel(cancel_data)
        assert "ex-gone" not in ex.client_by_exchange
        ex._publish_cancel_event.assert_awaited_once()


class TestStableReconExecIds:
    """Stable synthetic exec ids and the extracted terminal helper."""

    def _pending(self, quantity: float = 2.0) -> tuple[Any, Any, Any]:
        """Build executor + tracked pending order mapped to ex-stable."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=quantity)
        pending = base_module.PendingOrderState(request=order)
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-stable"] = order.client_order_id
        ex._publish_execution = AsyncMock()
        ex._record_venue_event = AsyncMock()
        return ex, order, pending

    @staticmethod
    def _snapshot(filled: float, price: float | None = 100.0) -> SimpleNamespace:
        """Venue order snapshot with the given cumulative."""
        return SimpleNamespace(
            id="ex-stable",
            status=ExchangeOrderStatusEnum.OPEN,
            filled=filled,
            price=price,
            average_price=price,
        )

    @pytest.mark.asyncio
    async def test_same_gap_reuses_the_same_exec_id(self) -> None:
        """Re-emitting an identical gap produces an identical exec id.

        Given: The same (order, venue cumulative) gap emitted twice —
            publish failed, recon retries, or a restart recomputed it,
        When: _reconcile_fill_gap builds the correctives,
        Then: Both carry the same deterministic id, so the engine and
            every replay consumer dedupe instead of double-applying.
        """
        ex, order, pending = self._pending()
        ex._publish_execution = AsyncMock(side_effect=[RuntimeError("zmq down"), None])
        del ex._publish_execution
        ex.msg_publisher = SimpleNamespace(
            send=AsyncMock(side_effect=[RuntimeError("zmq down"), None])
        )
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(0.5))
        assert pending.last_seen_cum_qty == 0.0
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(0.5))
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert sent[0].trade_id == sent[1].trade_id
        assert sent[0].trade_id == "recon-ex-stable-c0.5"

    @pytest.mark.asyncio
    async def test_advanced_cum_changes_the_exec_id(self) -> None:
        """A gap at a new venue cumulative gets a distinct id."""
        ex, order, pending = self._pending(quantity=3.0)
        del ex._publish_execution
        ex.msg_publisher = SimpleNamespace(send=AsyncMock())
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(0.5))
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(1.0))
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert sent[0].trade_id == "recon-ex-stable-c0.5"
        assert sent[1].trade_id == "recon-ex-stable-c1.0"

    @pytest.mark.asyncio
    async def test_published_corrective_redelivery_drops_via_lru(self) -> None:
        """A successfully published corrective's re-emission is LRU-dropped."""
        ex, order, pending = self._pending(quantity=3.0)
        del ex._publish_execution
        ex.msg_publisher = SimpleNamespace(send=AsyncMock())
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(0.5))
        assert "recon-ex-stable-c0.5" in ex._seen_exec_ids
        pending.last_seen_cum_qty = 0.0
        await ex._reconcile_fill_gap("kraken", "ex-stable", pending, self._snapshot(0.5))
        assert ex.msg_publisher.send.await_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "exec_type"),
        [
            (ExchangeOrderStatusEnum.CLOSED, "filled"),
            (ExchangeOrderStatusEnum.EXPIRED, "expired"),
            (ExchangeOrderStatusEnum.CANCELED, "canceled"),
        ],
    )
    async def test_emit_disappeared_terminal_status_mapping(
        self, status: ExchangeOrderStatusEnum, exec_type: str
    ) -> None:
        """The extracted helper maps venue terminals to ExecTypes 1:1."""
        ex, order, pending = self._pending()
        captured: list[Any] = []
        ex._process_execution = AsyncMock(side_effect=lambda e: captured.append(e))
        snapshot = SimpleNamespace(
            id="ex-stable", status=status, filled=0.0, price=None, average_price=None
        )
        await ex._emit_disappeared_terminal("kraken", "ex-stable", pending, snapshot)
        assert captured[0].exec_type == exec_type
        assert captured[0].order_status == status


def _fill_row(
    cum: float,
    size: float,
    exec_id: str | None,
    price: float = 100.0,
    fee: float | None = None,
) -> dict[str, Any]:
    """Build a minimal VenueEventRow-shaped dict for recovery tests."""
    return {
        "id": 1,
        "event_type": "fill_observed",
        "client_order_id": "c1",
        "exchange_order_id": "ex-1",
        "cum_fill_size": cum,
        "fill_size": size,
        "fill_price": price,
        "fee": fee,
        "fee_asset": "USD" if fee is not None else None,
        "exec_id": exec_id,
        "venue_timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
    }


class TestRecoveryWatermarkSeeding:
    """Dual-watermark seeding and downtime healing at recovery."""

    def _executor(
        self,
        fill_rows: list[dict[str, Any]],
        exec_sizes: list[float],
        snap_filled: float = 0.0,
        snap_status: ExchangeOrderStatusEnum = ExchangeOrderStatusEnum.OPEN,
        db_order: dict[str, Any] | None = None,
    ) -> Any:
        """Build an executor wired for one-order recovery scenarios."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        mock_client = AsyncMock()
        mock_client.get_order_fill_summary = AsyncMock(return_value=None)
        mock_client.supports_fill_summary = False
        snap = SimpleNamespace(id="ex-1", filled=snap_filled, price=100.0, status=snap_status)
        if snap_status == ExchangeOrderStatusEnum.OPEN:
            mock_client.get_orders = AsyncMock(return_value=[snap])
        else:
            mock_client.get_orders = AsyncMock(return_value=[])
            mock_client.get_order = AsyncMock(return_value=snap)
        ex.exchange_client = mock_client
        repo = AsyncMock(spec=SQLAlchemyRepository)
        repo.get_active_orders_for_recovery = AsyncMock(return_value=[db_order or _make_db_order()])
        repo.get_fill_venue_events_for_order = AsyncMock(return_value=fill_rows)
        repo.get_executions_for_order = AsyncMock(return_value=[{"size": s} for s in exec_sizes])
        ex.repository = repo
        ex._record_venue_event = AsyncMock()
        ex.msg_publisher = SimpleNamespace(send=AsyncMock())
        return ex

    @pytest.mark.asyncio
    async def test_executions_ahead_of_rows_clamps_to_durable(self) -> None:
        """The published seed can never exceed the durable plane.

        Given: Executions summing past the durable max (pre-dual-watermark
            history anomaly),
        When: Recovery seeds the watermarks,
        Then: Both clamp to durable_max and nothing is republished.
        """
        ex = self._executor(
            fill_rows=[_fill_row(0.5, 0.5, "f1")], exec_sizes=[0.5, 0.5], snap_filled=0.5
        )
        await ex._recover_pending_orders("kraken")
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(0.5)
        assert pending.last_recorded_cum_qty == pytest.approx(0.5)
        ex.msg_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recorded_tail_republished_under_original_ids(self) -> None:
        """Recorded-but-unpublished rows republish with original exec ids.

        Given: Three durable rows (cums 0.3, 0.7, 1.0) while executions
            prove only 0.3 was published,
        When: Recovery runs,
        Then: Exactly the two tail rows republish ascending under their
            ORIGINAL exec ids with row price/fee, the published watermark
            telescopes to the durable max, and the already-published
            row's exec id is pre-warmed into the LRU.
        """
        rows = [
            _fill_row(0.3, 0.3, "f1"),
            _fill_row(0.7, 0.4, "f2", price=101.0, fee=0.2),
            _fill_row(1.0, 0.3, "f3", price=102.0),
        ]
        ex = self._executor(
            fill_rows=rows, exec_sizes=[0.3], snap_filled=1.0, db_order=_make_db_order(size=2.0)
        )
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert [f.trade_id for f in sent] == ["f2", "f3"]
        assert [f.size for f in sent] == [pytest.approx(0.7), pytest.approx(1.0)]
        assert sent[0].last_price == pytest.approx(101.0)
        assert sent[0].fee == pytest.approx(0.2)
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(1.0)
        assert "f1" in ex._seen_exec_ids
        assert "f2" in ex._seen_exec_ids

    @pytest.mark.asyncio
    async def test_venue_ahead_of_durable_emits_stable_gap_corrective(self) -> None:
        """The venue-ahead-of-durable remainder becomes one corrective.

        Given: Durable rows up to 0.5 (all published) while the venue
            reports filled=1.0,
        When: Recovery runs,
        Then: One corrective sized 0.5 with the stable recon id covers
            exactly the quantity present in NO durable row — never the
            quantity a pre-crash row already carries.
        """
        ex = self._executor(
            fill_rows=[_fill_row(0.5, 0.5, "f1")],
            exec_sizes=[0.5],
            snap_filled=1.0,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert len(sent) == 1
        assert sent[0].trade_id == "recon-ex-1-c1.0"
        assert sent[0].last_size == pytest.approx(0.5)
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_double_restart_is_idempotent(self) -> None:
        """A second recovery over healed state emits nothing.

        Given: DB state that already includes the first recovery's
            corrective row and execution,
        When: Recovery runs again,
        Then: Seeds equal the venue cumulative and zero emissions occur.
        """
        rows = [_fill_row(0.5, 0.5, "f1"), _fill_row(1.0, 0.5, "recon-ex-1-c1.0")]
        ex = self._executor(
            fill_rows=rows,
            exec_sizes=[0.5, 0.5],
            snap_filled=1.0,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        ex.msg_publisher.send.assert_not_awaited()
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(1.0)
        assert pending.last_recorded_cum_qty == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_terminal_with_downtime_fills_heals_then_projects(self) -> None:
        """A canceled-while-down partial fill is delivered before terminal.

        Given: Venue reports CANCELED with filled=0.5, durable plane empty,
        When: Recovery runs,
        Then: The gap corrective publishes first, then the canceled
            terminal flows through the pipeline, and the order is not
            left pending.
        """
        ex = self._executor(
            fill_rows=[],
            exec_sizes=[],
            snap_filled=0.5,
            snap_status=ExchangeOrderStatusEnum.CANCELED,
        )
        ex._publish_cancel_event = AsyncMock()
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert len(sent) == 1
        assert sent[0].trade_id == "recon-ex-1-c0.5"
        assert sent[0].last_size == pytest.approx(0.5)
        assert "c1" not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_db_order_pk_threads_into_pending(self) -> None:
        """The real integer PK reaches db_order_id for status updates."""
        db_order = _make_db_order()
        db_order["id"] = 4242
        ex = self._executor(fill_rows=[], exec_sizes=[], db_order=db_order)
        await ex._recover_pending_orders("kraken")
        assert ex.pending_orders["c1"].db_order_id == 4242

    @pytest.mark.asyncio
    async def test_venue_events_unreadable_falls_back_to_legacy(self) -> None:
        """An unreadable durable plane degrades to venue-truth seeding.

        Given: get_fill_venue_events_for_order raises,
        When: Recovery runs,
        Then: Both watermarks seed from the venue snapshot (today's
            behavior, no correctives, never a double-apply).
        """
        ex = self._executor(fill_rows=[], exec_sizes=[], snap_filled=0.7)
        ex.repository.get_fill_venue_events_for_order = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        await ex._recover_pending_orders("kraken")
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(0.7)
        assert pending.last_recorded_cum_qty == pytest.approx(0.7)
        ex.msg_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_executions_unreadable_republishes_full_tail(self) -> None:
        """An unreadable published plane republishes everything durably known.

        Given: Durable rows to 0.5, executions read raises, venue at 1.0,
        When: Recovery runs,
        Then: The committed seed is conservative (0.0) so the WHOLE
            durable tail republishes under original ids (consumers that
            saw a row dedupe it; seeding published=durable would
            permanently hide a recorded-but-unpublished fill), and the
            venue-ahead remainder still emits as the stable corrective.
        """
        ex = self._executor(
            fill_rows=[_fill_row(0.5, 0.5, "f1")],
            exec_sizes=[],
            snap_filled=1.0,
            db_order=_make_db_order(size=2.0),
        )
        ex.repository.get_executions_for_order = AsyncMock(side_effect=RuntimeError("db down"))
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert [f.trade_id for f in sent] == ["f1", "recon-ex-1-c1.0"]
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_idless_rows_count_as_shown_and_never_republish(self) -> None:
        """Id-less durable fills are never republished, only counted.

        Given: Walutomat-style id-less rows including an ABSORBED
            partition (rows 0.5 then 0.3 to cum 0.8 — a live engine may
            have applied the same total as a single 0.8 frame, so no
            identity key can correlate the partitions),
        When: Recovery runs with venue at the same cumulative,
        Then: Nothing publishes (neither republish nor corrective), the
            committed seed counts the id-less cumulative as shown, and a
            loud warning records the residual (an engine that missed the
            publish heals at its next checkpoint replay).
        """
        rows = [
            _fill_row(0.5, 0.5, None, price=4.32),
            _fill_row(0.8, 0.3, None, price=4.32),
        ]
        ex = self._executor(
            fill_rows=rows,
            exec_sizes=[],
            snap_filled=0.8,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        ex.msg_publisher.send.assert_not_awaited()
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(0.8)
        assert pending.last_recorded_cum_qty == pytest.approx(0.8)

    @pytest.mark.asyncio
    async def test_idless_cum_below_republished_tail_is_noop(self) -> None:
        """An id-less row already covered by republished ids changes nothing.

        Given: An id-less row at cum 0.6 alongside an id-bearing row at
            cum 0.8 whose republish advances the committed watermark past
            the id-less cumulative,
        When: Recovery finishes the row sweep,
        Then: The id-less counted-as-shown adjustment is a no-op (the
            committed watermark already exceeds it).
        """
        rows = [
            _fill_row(0.6, 0.1, None, price=4.32),
            _fill_row(0.8, 0.2, "f-big"),
        ]
        ex = self._executor(
            fill_rows=rows,
            exec_sizes=[0.5],
            snap_filled=0.8,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert [f.trade_id for f in sent] == ["f-big"]
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(0.8)

    @pytest.mark.asyncio
    async def test_idless_shown_applies_before_idbearing_republish(self) -> None:
        """Id-less quantities are shown BEFORE id-bearing rows republish.

        Given: An id-less row at cum 0.5 (its publish reached a live
            engine) followed by an id-bearing recon row at cum 0.8 whose
            publish failed, with no executions evidence,
        When: Recovery republishes the id-bearing row,
        Then: Its cum-anchored delta is 0.3 — counting the id-less span
            as shown only AFTER the republish would anchor the delta at
            0.0 and re-absorb the already-applied 0.5 into a 0.8 frame,
            double-applying it on the live engine.
        """
        rows = [
            _fill_row(0.5, 0.5, None, price=4.32),
            _fill_row(0.8, 0.3, "recon-ex-1-c0.8", price=4.32),
        ]
        ex = self._executor(
            fill_rows=rows,
            exec_sizes=[],
            snap_filled=0.8,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert [f.trade_id for f in sent] == ["recon-ex-1-c0.8"]
        assert sent[0].last_size == pytest.approx(0.3)
        assert sent[0].size == pytest.approx(0.8)
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(0.8)

    @pytest.mark.asyncio
    async def test_idless_shown_seed_still_emits_true_venue_gap(self) -> None:
        """A never-recorded remainder above id-less rows still heals.

        Given: Id-less rows to cum 0.5 and the venue at 0.8 (the 0.3
            remainder exists in NO durable row, so no engine anywhere can
            have applied it),
        When: Recovery runs,
        Then: Exactly the 0.3 corrective emits with the stable id.
        """
        ex = self._executor(
            fill_rows=[_fill_row(0.5, 0.5, None, price=4.32)],
            exec_sizes=[],
            snap_filled=0.8,
            db_order=_make_db_order(size=2.0),
        )
        await ex._recover_pending_orders("kraken")
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert len(sent) == 1
        assert sent[0].trade_id == "recon-ex-1-c0.8"
        assert sent[0].last_size == pytest.approx(0.3)

    @pytest.mark.asyncio
    async def test_durable_write_failure_continues_to_next_order(self) -> None:
        """A failing corrective write never aborts the recovery sweep.

        Given: Two recovering orders where the first's venue-event write
            raises during its gap corrective,
        When: Recovery runs,
        Then: The first order stays seeded with watermarks unmoved and
            the second order is still recovered.
        """
        first = _make_db_order()
        second = _make_db_order(client_order_id="c2", exchange_order_id="ex-2")
        ex = self._executor(fill_rows=[], exec_sizes=[], snap_filled=0.5)
        snap1 = SimpleNamespace(
            id="ex-1", filled=0.5, price=100.0, status=ExchangeOrderStatusEnum.OPEN
        )
        snap2 = SimpleNamespace(
            id="ex-2", filled=0.0, price=100.0, status=ExchangeOrderStatusEnum.OPEN
        )
        ex.exchange_client.get_orders = AsyncMock(return_value=[snap1, snap2])
        ex.repository.get_active_orders_for_recovery = AsyncMock(return_value=[first, second])
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("db down"))
        await ex._recover_pending_orders("kraken")
        assert ex.pending_orders["c1"].last_seen_cum_qty == pytest.approx(0.0)
        assert "c2" in ex.pending_orders
        ex.msg_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_null_cum_rows_are_skipped_everywhere(self) -> None:
        """Defensive null-cum rows neither seed nor republish."""
        rows = [
            _fill_row(0.5, 0.5, "f1"),
            {**_fill_row(0.0, 0.0, "weird"), "cum_fill_size": None},
        ]
        ex = self._executor(fill_rows=rows, exec_sizes=[0.5], snap_filled=0.5)
        await ex._recover_pending_orders("kraken")
        pending = ex.pending_orders["c1"]
        assert pending.last_recorded_cum_qty == pytest.approx(0.5)
        ex.msg_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_terminal_full_fill_pops_before_terminal_emission(self) -> None:
        """A gap corrective that completes the order makes terminal a no-op.

        Given: A CLOSED order whose gap corrective covers the full size
            (the pipeline pops it as FILLED),
        When: The terminal step runs,
        Then: There is no pending entry left to emit against and recovery
            finishes cleanly.
        """
        ex = self._executor(
            fill_rows=[],
            exec_sizes=[],
            snap_filled=1.0,
            snap_status=ExchangeOrderStatusEnum.CLOSED,
        )
        await ex._recover_pending_orders("kraken")
        assert "c1" not in ex.pending_orders
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert [f.trade_id for f in sent] == ["recon-ex-1-c1.0"]

    @pytest.mark.asyncio
    async def test_non_sqla_repo_with_unreadable_executions_seeds_zero(self) -> None:
        """A non-SQLA repo whose executions read fails seeds (0, 0)."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        mock_client = AsyncMock()
        snap = SimpleNamespace(
            id="ex-1", filled=0.0, price=None, status=ExchangeOrderStatusEnum.OPEN
        )
        mock_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client = mock_client
        repo = AsyncMock()
        repo.get_active_orders_for_recovery = AsyncMock(return_value=[_make_db_order()])
        repo.get_executions_for_order = AsyncMock(side_effect=RuntimeError("backend"))
        ex.repository = repo
        await ex._recover_pending_orders("kraken")
        pending = ex.pending_orders["c1"]
        assert pending.last_seen_cum_qty == pytest.approx(0.0)
        assert pending.last_recorded_cum_qty == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_start_sets_running_before_recovery(self) -> None:
        """Recovery emissions are publishable: running precedes recovery."""
        ex: Any = MergedDummyExecutor()
        observed: list[bool] = []

        async def observe(exchange_name: str) -> None:
            observed.append(ex.running)
            raise asyncio.CancelledError()

        ex._recover_pending_orders = AsyncMock(side_effect=observe)
        ex._initialize_settings = AsyncMock()
        ex._resolve_credentials = AsyncMock()
        ex._setup_zmq_sockets = MagicMock()
        client = AsyncMock()
        client.set_tracker = MagicMock()
        client.supports_websocket_executions = False
        ex._create_exchange_client = MagicMock(return_value=client)
        with contextlib.suppress(asyncio.CancelledError):
            await ex.start()
        assert observed == [True]


class TestStatusOnlyTerminalDelta:
    """Codex P0-3 round-1 regressions: terminal deltas and id collisions."""

    @pytest.mark.asyncio
    async def test_terminal_for_filled_order_publishes_zero_delta(self) -> None:
        """A status-only terminal never reverses an applied position.

        Given: An order whose 1.0 fill was already published (committed
            cumulative 1.0) and a synthetic status-only 'filled' terminal,
        When: The terminal flows through the pipeline,
        Then: The published frame carries size=1.0 and last_size=0.0 —
            resolving status frames to cumulative 0.0 used to emit a
            NEGATIVE 1.0 delta that a delta-applying engine would book as
            a position reversal.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=1.0)
        pending = base_module.PendingOrderState(
            request=order, last_seen_cum_qty=1.0, last_recorded_cum_qty=1.0
        )
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-term"] = order.client_order_id
        ex._record_venue_event = AsyncMock()
        ex.msg_publisher = SimpleNamespace(send=AsyncMock())
        terminal = SimpleNamespace(
            order_id="ex-term",
            exec_type="filled",
            exec_id=None,
            order_status=ExchangeOrderStatusEnum.CLOSED,
            cum_qty=None,
            average_price=None,
            fee_usd_equiv=None,
            fees=None,
            last_qty=None,
            last_price=None,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            side=SimpleNamespace(value="buy"),
            trade_id=None,
            liquidity_ind=None,
        )
        await ex._process_execution(terminal)
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert len(sent) == 1
        assert sent[0].last_size == pytest.approx(0.0)
        assert sent[0].size == pytest.approx(1.0)
        assert order.client_order_id not in ex.pending_orders

    @pytest.mark.asyncio
    async def test_unexpected_per_order_error_never_aborts_sweep(self) -> None:
        """One order's unexpected recovery error skips it, not the sweep.

        Given: Two DB-active orders where the first's recovery raises an
            unexpected error,
        When: _recover_pending_orders runs,
        Then: The error is logged, the first order is skipped, and the
            second order is still recovered.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        mock_client = AsyncMock()
        snap = SimpleNamespace(
            id="ex-2", filled=0.0, price=None, status=ExchangeOrderStatusEnum.OPEN
        )
        mock_client.get_orders = AsyncMock(return_value=[snap])
        ex.exchange_client = mock_client
        repo = AsyncMock()
        repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[
                _make_db_order(),
                _make_db_order(client_order_id="c2", exchange_order_id="ex-2"),
            ]
        )
        repo.get_executions_for_order = AsyncMock(return_value=[])
        ex.repository = repo
        original = ex._recover_single_order

        async def explode_first(db_order: Any, *args: Any, **kwargs: Any) -> bool:
            if db_order["client_order_id"] == "c1":
                raise RuntimeError("unexpected")
            return cast(bool, await original(db_order, *args, **kwargs))

        ex._recover_single_order = explode_first
        await ex._recover_pending_orders("kraken")
        assert "c1" not in ex.pending_orders
        assert "c2" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_near_cum_gaps_get_distinct_exec_ids(self) -> None:
        """Floats that .12g would collapse stay distinct under repr.

        Given: Two advancing venue cumulatives differing only past the
            12th significant digit,
        When: _reconcile_fill_gap emits correctives for both,
        Then: The deterministic ids differ, so the second gap is NOT
            swallowed by the exec-id LRU as a duplicate of the first.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=200001.0)
        pending = base_module.PendingOrderState(request=order)
        ex.pending_orders[order.client_order_id] = pending
        ex.client_by_exchange["ex-big"] = order.client_order_id
        ex._record_venue_event = AsyncMock()
        ex.msg_publisher = SimpleNamespace(send=AsyncMock())
        snap1 = SimpleNamespace(
            id="ex-big",
            filled=100000.00000004,
            price=100.0,
            status=ExchangeOrderStatusEnum.OPEN,
        )
        snap2 = SimpleNamespace(
            id="ex-big",
            filled=100000.00000008,
            price=100.0,
            status=ExchangeOrderStatusEnum.OPEN,
        )
        await ex._reconcile_fill_gap("kraken", "ex-big", pending, snap1)
        await ex._reconcile_fill_gap("kraken", "ex-big", pending, snap2)
        sent = [c.args[1] for c in ex.msg_publisher.send.await_args_list]
        assert len(sent) == 2
        assert sent[0].trade_id != sent[1].trade_id


class TestSupervisedLoop:
    """Generic loop supervisor: respawn, backoff, escalation, hooks."""

    def _executor(self) -> Any:
        """Build a running executor with a recording jitter sleep."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._sleep_with_jitter = AsyncMock()
        return ex

    @pytest.mark.asyncio
    async def test_respawns_on_exception_and_clean_return(self) -> None:
        """Both death shapes respawn; cancellation propagates.

        Given: An attempt that raises, then returns cleanly, then is
            cancelled,
        When: The supervisor runs,
        Then: Two respawns with doubled backoff are recorded and the
            cancellation re-raises.
        """
        ex = self._executor()
        attempt = AsyncMock(side_effect=[RuntimeError("died"), None, asyncio.CancelledError()])
        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_loop("order_handler", attempt)
        assert attempt.await_count == 3
        sleeps = [c.args[0] for c in ex._sleep_with_jitter.await_args_list]
        assert sleeps == [1.0, 2.0]
        assert ex._task_restarts["order_handler"] == 2

    @pytest.mark.asyncio
    async def test_exit_silently_when_stopped(self) -> None:
        """A death observed after running cleared is shutdown, not a death."""
        ex = self._executor()

        async def stop_and_raise() -> None:
            ex.running = False
            raise RuntimeError("socket closed by stop")

        attempt = AsyncMock(side_effect=stop_and_raise)
        await ex._supervise_loop("order_handler", attempt)
        ex._sleep_with_jitter.assert_not_awaited()
        assert ex._task_restarts == {}

    @pytest.mark.asyncio
    async def test_clean_return_after_stop_exits(self) -> None:
        """A clean return after running cleared exits without respawn."""
        ex = self._executor()

        async def stop_and_return() -> None:
            ex.running = False

        attempt = AsyncMock(side_effect=stop_and_return)
        await ex._supervise_loop("heartbeat", attempt)
        ex._sleep_with_jitter.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pre_respawn_runs_only_after_a_death(self) -> None:
        """The pre-respawn hook never runs before the first attempt.

        Given: An attempt that dies once then is cancelled,
        When: The supervisor re-enters,
        Then: pre_respawn ran exactly once — before attempt 2, not 1.
        """
        ex = self._executor()
        order: list[str] = []

        async def dying() -> None:
            order.append("attempt")
            if len([o for o in order if o == "attempt"]) >= 2:
                raise asyncio.CancelledError()
            raise RuntimeError("died")

        def hook() -> None:
            order.append("hook")

        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_loop("order_handler", dying, pre_respawn=hook)
        assert order == ["attempt", "hook", "attempt"]

    @pytest.mark.asyncio
    async def test_healthy_runtime_resets_backoff_and_streak(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A long-lived run resets both backoff and the death streak."""
        ex = self._executor()
        clock = iter([0.0, 1.0, 10.0, 11.0, 20.0, 321.0])
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: next(clock)))
        sleeps: list[float] = []

        async def record(delay: float) -> None:
            sleeps.append(delay)
            if len(sleeps) >= 3:
                ex.running = False

        ex._sleep_with_jitter = AsyncMock(side_effect=record)
        attempt = AsyncMock(side_effect=RuntimeError("down"))
        await ex._supervise_loop("reconciliation", attempt)
        assert sleeps == [1.0, 2.0, 1.0]

    @pytest.mark.asyncio
    async def test_death_streak_past_ceiling_escalates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A streak older than the ceiling raises ExecutorTaskDeadError.

        Given: Deaths whose streak start sits more than the escalation
            ceiling in the past,
        When: The next death is handled,
        Then: ExecutorTaskDeadError carries the task label — in-process
            respawn has proven insufficient, the launcher must rebuild.
        """
        ex = self._executor()
        clock = iter([0.0, 1.0, 1490.0, 1502.0])
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: next(clock)))
        attempt = AsyncMock(side_effect=RuntimeError("down"))
        with pytest.raises(base_module.ExecutorTaskDeadError, match="order_handler"):
            await ex._supervise_loop("order_handler", attempt)
        assert attempt.await_count == 2

    def test_ceiling_exceeds_launcher_total_reset(self) -> None:
        """INVARIANT: escalation always presents as long-healthy uptime.

        Given: the executor escalation ceiling and the launcher's
            total-reset uptime threshold,
        When: compared,
        Then: ceiling > threshold — a ceiling-driven service death always
            resets the launcher's restart budget, so escalation yields an
            unbounded slow retry cadence, never a permanent park.
        """
        assert base_module._TASK_DEATH_ESCALATION_CEILING_S > launcher_module._TOTAL_RESET_UPTIME_S


class TestOrderSubscriberRebuild:
    """Pre-respawn SUB rebuild for the order handler."""

    def test_rebuild_closes_old_and_resubscribes(self) -> None:
        """The poisoned subscriber is closed and a fresh one connected.

        Given: An executor with a live context and a poisoned subscriber,
        When: _rebuild_order_subscriber runs,
        Then: The old socket gets LINGER 0 + close, and the new SUB is
            connected to the broker with all three subscriptions.
        """
        ex: Any = MergedDummyExecutor()
        ex.settings = SimpleNamespace(zmq_broker_xpub="tcp://x:1")
        old = MagicMock()
        ex.subscriber = old
        raw = MagicMock()
        ex.context = MagicMock()
        ex.context.socket = MagicMock(return_value=raw)
        ex._rebuild_order_subscriber()
        old.setsockopt.assert_called_once_with(zmq.LINGER, 0)
        old.close.assert_called_once()
        raw.connect.assert_called_once_with("tcp://x:1")
        assert ex.subscriber is not old
        subscribed = [c.args[1] for c in raw.setsockopt_string.call_args_list]
        assert subscribed == [
            order_commands_prefix("kraken"),
            "system.symbol_aliases",
            "system.settings",
        ]

    def test_rebuild_survives_close_failure(self) -> None:
        """A failing close never blocks the fresh subscriber."""
        ex: Any = MergedDummyExecutor()
        ex.settings = SimpleNamespace(zmq_broker_xpub="tcp://x:1")
        old = MagicMock()
        old.setsockopt = MagicMock(side_effect=RuntimeError("already dead"))
        ex.subscriber = old
        ex.context = MagicMock()
        ex._rebuild_order_subscriber()
        assert ex.subscriber is not old

    def test_rebuild_without_existing_subscriber(self) -> None:
        """A missing subscriber just builds a fresh one."""
        ex: Any = MergedDummyExecutor()
        ex.settings = SimpleNamespace(zmq_broker_xpub="tcp://x:1")
        ex.subscriber = None
        ex.context = MagicMock()
        ex._rebuild_order_subscriber()
        assert ex.subscriber is not None


class TestReconCycleTimeout:
    """Bounded reconciliation cycles release the lock on wedge."""

    @pytest.mark.asyncio
    async def test_wedged_cycle_times_out_and_releases_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A hung venue call no longer holds _recon_lock forever.

        Given: A cycle body that never returns,
        When: _reconcile_with_exchange runs with a tiny timeout,
        Then: TimeoutError propagates and the lock is free afterwards —
            unblocking both the periodic loop and the stream supervisor's
            post-reconnect heal.
        """
        ex: Any = MergedDummyExecutor()
        monkeypatch.setattr(base_module, "_RECON_CYCLE_TIMEOUT_S", 0.01)

        async def wedge() -> None:
            await asyncio.Event().wait()

        ex._reconcile_with_exchange_unlocked = AsyncMock(side_effect=wedge)
        with pytest.raises(TimeoutError):
            await ex._reconcile_with_exchange()
        assert ex._recon_lock.locked() is False

    @pytest.mark.asyncio
    async def test_handler_logs_wedge_and_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The periodic handler logs the wedge and keeps cycling."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        calls = 0

        async def recon() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError()
            ex.running = False

        ex._reconcile_with_exchange = AsyncMock(side_effect=recon)

        async def fake_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr(
            base_module, "asyncio", SimpleNamespace(sleep=fake_sleep, timeout=asyncio.timeout)
        )
        await ex._reconciliation_handler()
        assert calls == 2


class TestStartSiblingContainment:
    """start() never orphans sibling tasks on a crash."""

    @pytest.mark.asyncio
    async def test_crashing_supervisor_cancels_siblings_and_stops(self) -> None:
        """One supervisor's death tears the whole service down cleanly.

        Given: A start() whose execution-stream supervisor raises while
            the other supervisors run forever,
        When: The gather propagates the error,
        Then: Siblings observe cancellation (no orphans outliving the
            exchange client), the crash-path stop() runs (running False),
            and the original error propagates out of start().
        """
        ex: Any = MergedDummyExecutor()
        cancelled: list[str] = []

        async def forever(label: str) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(label)
                raise

        async def fake_supervise(
            task_label: str,
            attempt: Callable[[], Awaitable[None]],
            pre_respawn: Callable[[], None] | None = None,
        ) -> None:
            await forever(task_label)

        async def boom() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("stream supervisor died for good")

        ex._initialize_settings = AsyncMock()
        ex._resolve_credentials = AsyncMock()
        ex._setup_zmq_sockets = MagicMock()
        ex._recover_pending_orders = AsyncMock()
        ex._supervise_loop = fake_supervise
        ex._supervise_execution_stream = boom
        ex.stop = AsyncMock(side_effect=lambda: setattr(ex, "running", False))
        client = AsyncMock()
        client.set_tracker = MagicMock()
        client.supports_websocket_executions = True
        ex._create_exchange_client = MagicMock(return_value=client)
        with pytest.raises(RuntimeError, match="died for good"):
            await ex.start()
        assert sorted(cancelled) == ["heartbeat", "order_handler", "reconciliation"]
        ex.stop.assert_awaited_once()


class TestLoopResidualBranches:
    """Containment branches that stay inside the supervised loops."""

    @pytest.mark.asyncio
    async def test_routing_failure_is_contained_per_frame(self) -> None:
        """A frame whose ROUTING raises is logged, not a loop death.

        Given: A consumed, decodable frame whose _route_message raises,
        When: The order handler processes it,
        Then: The error is logged and the loop continues to the next
            frame — post-recv processing failures are poison containment
            on an already-consumed frame, unlike transport errors.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        frames = iter([("orders.commands.kraken.BTC-USD.submit", b"{}")])

        async def recv() -> tuple[str, bytes]:
            try:
                return next(frames)
            except StopIteration:
                ex.running = False
                raise RuntimeError("drained") from None

        ex.subscriber = SimpleNamespace(recv_multipart=AsyncMock(side_effect=recv))
        ex._route_message = AsyncMock(side_effect=RuntimeError("router broke"))
        with pytest.raises(RuntimeError, match="drained"):
            await ex._order_handler()
        ex._route_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_heartbeat_breaks_when_stopped_during_sleep(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop during the heartbeat sleep exits before publishing."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.settings = SimpleNamespace(
            zmq_heartbeat_interval_ms=1,
            zmq_broker_xsub="xsub",
            zmq_broker_xpub="xpub",
        )
        ex._publish_heartbeat = AsyncMock()

        async def stopping_sleep(_delay: float) -> None:
            ex.running = False

        monkeypatch.setattr(
            base_module,
            "asyncio",
            SimpleNamespace(sleep=stopping_sleep, timeout=asyncio.timeout),
        )
        await ex._heartbeat_loop()
        ex._publish_heartbeat.assert_not_awaited()


class TestAmbiguousVerificationFairness:
    """Per-cycle cap + rotation over parked ambiguous orders."""

    @pytest.mark.asyncio
    async def test_tail_orders_are_reached_across_cycles(self) -> None:
        """Every parked entry is attempted within ceil(N/cap) cycles.

        Given: Five parked ambiguous orders (cap is 3) whose verification
            consumes the whole per-cycle budget,
        When: Two recon cycles run,
        Then: All five entries were attempted exactly once — a fixed
            iteration order would burn the budget on the same prefix
            every cycle and starve the tail forever.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = AsyncMock()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        for i in range(5):
            order = make_order(client_order_id=f"amb-{i}")
            ex.pending_orders[f"amb-{i}"] = base_module.PendingOrderState(
                request=order, submit_ambiguous=True
            )
        attempted: list[str] = []

        async def verify(entry: Any) -> None:
            attempted.append(entry.request.client_order_id)

        ex._resolve_ambiguous_pending = AsyncMock(side_effect=verify)
        await ex._reconcile_with_exchange()
        assert attempted == ["amb-0", "amb-1", "amb-2"]
        await ex._reconcile_with_exchange()
        assert attempted == ["amb-0", "amb-1", "amb-2", "amb-3", "amb-4", "amb-0"]

    @pytest.mark.asyncio
    async def test_resolved_entries_drop_out_of_rotation(self) -> None:
        """Entries that gained an exchange id or vanished are skipped."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = AsyncMock()
        ex.exchange_client.get_orders = AsyncMock(return_value=[])
        ex.exchange_client.get_balance = AsyncMock(return_value={})
        for i in range(2):
            order = make_order(client_order_id=f"amb-{i}")
            ex.pending_orders[f"amb-{i}"] = base_module.PendingOrderState(
                request=order, submit_ambiguous=True
            )
        attempted: list[str] = []

        async def verify_and_resolve(entry: Any) -> None:
            attempted.append(entry.request.client_order_id)
            entry.exchange_order_id = "ex-found"
            ex.pending_orders.pop("amb-1", None)

        ex._resolve_ambiguous_pending = AsyncMock(side_effect=verify_and_resolve)
        await ex._reconcile_with_exchange()
        assert attempted == ["amb-0"]


class TestTerminalSectionsCancelSafety:
    """Terminal emissions pop tracking LAST — cancel/crash safe."""

    @pytest.mark.asyncio
    async def test_verified_absent_keeps_entry_until_terminal_persisted(self) -> None:
        """A cut-short absent-rejection leaves the entry parked for retry.

        Given: A parked ambiguous order whose second authoritative
            absence triggers the REJECTED terminal, with the durable
            write raising (modelling a recon-cycle timeout or DB fault
            cutting the section),
        When: The verification round runs,
        Then: The entry is STILL parked — the pop happens only after the
            terminal publish and durable write both completed, so the
            next cycle retries instead of losing the rejection forever.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = AsyncMock()
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        order = make_order(client_order_id="amb-cut")
        pending = base_module.PendingOrderState(request=order, submit_ambiguous=True)
        ex.pending_orders["amb-cut"] = pending
        ex._publish_order_status = AsyncMock(return_value=True)
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("cut mid-write"))
        with (
            patch.object(base_module.asyncio, "sleep", new=AsyncMock()),
            pytest.raises(RuntimeError, match="cut mid-write"),
        ):
            await ex._verify_ambiguous_submit(order, pending)
        assert "amb-cut" in ex.pending_orders

    @pytest.mark.asyncio
    async def test_verified_absent_failed_publish_keeps_entry_parked(self) -> None:
        """A swallowed REJECTED publish failure never pops the entry.

        Given: Two authoritative absences but _publish_order_status
            reports False (ZMQ send failed — the engine did NOT receive
            the rejection and its UNKNOWN guard stays held),
        When: The verification round runs,
        Then: No durable rejection is written and the entry stays parked
            — popping here would strand the engine guard forever, since
            UNKNOWN disables the in-flight timeout release.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = AsyncMock()
        ex.exchange_client.find_order_by_client_id = AsyncMock(return_value=None)
        order = make_order(client_order_id="amb-pubfail")
        pending = base_module.PendingOrderState(request=order, submit_ambiguous=True)
        ex.pending_orders["amb-pubfail"] = pending
        ex._publish_order_status = AsyncMock(return_value=False)
        ex._record_venue_event = AsyncMock()
        with patch.object(base_module.asyncio, "sleep", new=AsyncMock()):
            resolved = await ex._verify_ambiguous_submit(order, pending)
        assert resolved is False
        assert "amb-pubfail" in ex.pending_orders
        ex._record_venue_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancellation_keeps_entry_until_terminal_persisted(self) -> None:
        """A cut-short cancel terminal leaves tracking intact for re-heal.

        Given: A cancel execution whose durable order_terminal write
            raises mid-section,
        When: _handle_cancellation runs,
        Then: pending_orders and client_by_exchange still track the order
            — recon's disappeared-path re-detects and re-emits the
            terminal instead of the order vanishing untracked with no
            durable terminal row.
        """
        ex: Any = MergedDummyExecutor()
        ex.running = True
        order = make_order(quantity=2.0)
        ex.pending_orders[order.client_order_id] = base_module.PendingOrderState(request=order)
        ex.client_by_exchange["ex-1"] = order.client_order_id
        ex._record_venue_event = AsyncMock(side_effect=RuntimeError("cut mid-write"))
        cancel = SimpleNamespace(
            order_id="ex-1",
            exec_type="canceled",
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
            liquidity_ind=None,
        )
        await ex._process_execution(cancel)
        assert order.client_order_id in ex.pending_orders
        assert "ex-1" in ex.client_by_exchange
        ex._record_venue_event = AsyncMock()
        await ex._process_execution(cancel)
        assert order.client_order_id not in ex.pending_orders
        assert "ex-1" not in ex.client_by_exchange


class TestHonestHeartbeatStatus:
    """Honest status derivation from supervised-loop seams."""

    def _executor(self, recon_age: float = 0.0, now: float = 10_000.0) -> Any:
        """Build an executor with a seeded recon clock at the given age."""
        ex: Any = MergedDummyExecutor()
        ex._task_last_pass["reconciliation"] = now - recon_age
        return ex, now

    def test_healthy_baseline(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fresh seams report HEALTHY with recon-age lag."""
        ex, now = self._executor(recon_age=42.0)
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        status, lag_ms, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.HEALTHY
        assert lag_ms == 42_000
        assert reasons == []

    def test_streak_age_maps_to_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An active death streak past the threshold is ERROR."""
        ex, now = self._executor()
        ex._task_streak_started["order_handler"] = now - 601.0
        ex._task_last_death["order_handler"] = now - 30.0
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        status, _lag, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.ERROR
        assert any("death streak" in r for r in reasons)

    def test_recon_age_boundaries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """299/301/899/901s recon ages map HEALTHY/WARNING/WARNING/ERROR."""
        expectations = [
            (299.0, base_module.HealthStatusEnum.HEALTHY),
            (301.0, base_module.HealthStatusEnum.WARNING),
            (899.0, base_module.HealthStatusEnum.WARNING),
            (901.0, base_module.HealthStatusEnum.ERROR),
        ]
        for age, expected in expectations:
            ex, now = self._executor(recon_age=age)
            monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda now=now: now))
            status, _lag, _reasons = ex._compute_heartbeat_status()
            assert status is expected, age

    def test_never_passing_recon_seeded_at_start_ages_out(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The start() seed means a never-succeeding recon still alarms."""
        ex: Any = MergedDummyExecutor()
        ex._task_last_pass["reconciliation"] = 0.0
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: 1000.0))
        status, lag_ms, _reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.ERROR
        assert lag_ms == 1_000_000

    def test_self_healed_streak_never_pages(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A streak whose loop self-healed goes stale, not ERROR.

        Given: A streak started 700s ago whose LAST death was 400s ago —
            the supervisors clear their dicts only at the next death, so
            the entry lingers while the respawned loop runs fine,
        When: Status is computed,
        Then: HEALTHY — without the freshness filter this aged into a
            false ERROR page at the 600s streak threshold.
        """
        ex, now = self._executor()
        ex._task_streak_started["order_handler"] = now - 700.0
        ex._task_last_death["order_handler"] = now - 400.0
        ex._task_deaths_in_streak["order_handler"] = 3
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.HEALTHY
        assert reasons == []

    def test_two_deaths_in_streak_warn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Two deaths in one active streak WARN; one death stays HEALTHY."""
        ex, now = self._executor()
        ex._task_streak_started["heartbeat"] = now - 5.0
        ex._task_last_death["heartbeat"] = now - 5.0
        ex._task_deaths_in_streak["heartbeat"] = 1
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        status, _l, _r = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.HEALTHY
        ex._task_deaths_in_streak["heartbeat"] = 2
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.WARNING
        assert any("2 deaths" in r for r in reasons)

    def test_inflight_command_ages_to_warning_then_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A command wedged inside processing surfaces via in-flight age.

        Given: An order command in flight for 150s, then 700s — it never
            dies (no supervisor signal) and never stamps progress,
        When: Status is computed,
        Then: WARNING then ERROR — the only visible symptom of a wedge
            inside the venue-call path.
        """
        ex, now = self._executor()
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        ex._order_inflight_started = now - 150.0
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.WARNING
        assert any("in flight" in r for r in reasons)
        ex._order_inflight_started = now - 700.0
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.ERROR
        assert any("in flight" in r for r in reasons)

    def test_backlogs_warn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unhealed accept events and parked UNKNOWNs map to WARNING."""
        ex, now = self._executor()
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        ex._unhealed_accept_events["c1"] = cast(Any, {"event_type": "order_accepted"})
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.WARNING
        assert any("unhealed" in r for r in reasons)
        ex._unhealed_accept_events.clear()
        order = make_order(client_order_id="amb-hb")
        ex.pending_orders["amb-hb"] = base_module.PendingOrderState(
            request=order, submit_ambiguous=True
        )
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.WARNING
        assert any("parked ambiguous" in r for r in reasons)

    def test_error_precedence_over_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ERROR conditions suppress WARNING reason scanning."""
        ex, now = self._executor(recon_age=901.0)
        ex._unhealed_accept_events["c1"] = cast(Any, {"event_type": "order_accepted"})
        monkeypatch.setattr(base_module, "time", SimpleNamespace(monotonic=lambda: now))
        status, _l, reasons = ex._compute_heartbeat_status()
        assert status is base_module.HealthStatusEnum.ERROR
        assert all("unhealed" not in r for r in reasons)

    @pytest.mark.asyncio
    async def test_tick_publishes_honest_frame_with_forensics(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One heartbeat tick carries derived status, lag and meta."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.settings = SimpleNamespace(
            zmq_heartbeat_interval_ms=1,
            zmq_broker_xsub="xsub",
            zmq_broker_xpub="xpub",
        )
        ex._task_last_pass["reconciliation"] = 0.0
        ex._task_restarts["order_handler"] = 3
        captured: list[Any] = []

        async def capture(topic: str, message: Any) -> None:
            captured.append(message)
            ex.running = False

        ex._publish_heartbeat = AsyncMock(side_effect=capture)
        monkeypatch.setattr(
            base_module,
            "time",
            SimpleNamespace(monotonic=lambda: 1000.0),
        )

        async def one_sleep(_d: float) -> None:
            return None

        monkeypatch.setattr(
            base_module, "asyncio", SimpleNamespace(sleep=one_sleep, timeout=asyncio.timeout)
        )
        await ex._heartbeat_loop()
        frame = captured[0]
        assert frame.status is base_module.HealthStatusEnum.ERROR
        assert frame.lag_ms == 1_000_000
        assert frame.meta["task_restarts"] == {"order_handler": 3}
        assert frame.meta["status_reasons"]
        assert frame.meta["running"] is True

    @pytest.mark.asyncio
    async def test_crashing_status_computation_degrades_not_kills(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A raising computation publishes WARNING and the loop survives."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.settings = SimpleNamespace(
            zmq_heartbeat_interval_ms=1,
            zmq_broker_xsub="xsub",
            zmq_broker_xpub="xpub",
        )
        ex._compute_heartbeat_status = MagicMock(side_effect=RuntimeError("boom"))
        captured: list[Any] = []

        async def capture(topic: str, message: Any) -> None:
            captured.append(message)
            if len(captured) >= 2:
                ex.running = False

        ex._publish_heartbeat = AsyncMock(side_effect=capture)

        async def fast_sleep(_d: float) -> None:
            return None

        monkeypatch.setattr(
            base_module, "asyncio", SimpleNamespace(sleep=fast_sleep, timeout=asyncio.timeout)
        )
        await ex._heartbeat_loop()
        assert len(captured) == 2
        assert all(f.status is base_module.HealthStatusEnum.WARNING for f in captured)
        assert all(
            any("status_computation_failed" in r for r in f.meta["status_reasons"])
            for f in captured
        )

    @pytest.mark.asyncio
    async def test_supervisor_death_populates_health_seams(self) -> None:
        """Loop deaths stamp last-death, streak and per-streak counts."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex._sleep_with_jitter = AsyncMock()
        attempt = AsyncMock(
            side_effect=[RuntimeError("d1"), RuntimeError("d2"), asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_loop("order_handler", attempt)
        assert "order_handler" in ex._task_last_death
        assert "order_handler" in ex._task_streak_started
        assert ex._task_deaths_in_streak["order_handler"] == 2

    @pytest.mark.asyncio
    async def test_stream_supervisor_death_populates_health_seams(self) -> None:
        """Fill-stream deaths stamp the execution_stream label."""
        ex: Any = MergedDummyExecutor()
        ex.running = True
        ex.exchange_client = MagicMock()
        ex.exchange_client.supports_websocket_executions = True
        ex._sleep_with_jitter = AsyncMock()
        ex._post_reconnect_reconcile = AsyncMock()
        ex._execution_handler = AsyncMock(
            side_effect=[ConnectionError("ws"), asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await ex._supervise_execution_stream()
        assert ex._task_deaths_in_streak["execution_stream"] == 1
        assert "execution_stream" in ex._task_streak_started
