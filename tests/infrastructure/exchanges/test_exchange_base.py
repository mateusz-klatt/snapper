"""Tests for ExchangeClientBase abstract class."""

import asyncio
import threading
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

import snapper.utils.logging as logging_module
from snapper.data.repository import ExecutionScopeResolutionError
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.errors import RestPoolDispatchError
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context


class DummyExchangeClient(ExchangeClientBase):
    """Stub exchange client for testing base class functionality."""

    def __init__(self, repository: Repository | None = None, exchange_name: str = "dummy") -> None:
        """Initialize the instance."""
        super().__init__(repository, exchange_name)
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        """Establish connection."""
        self.connected = True

    async def disconnect(self) -> None:
        """Close connection."""
        self.disconnected = True

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Get ticker snapshot for symbol."""
        return TickerSnapshot(
            symbol=symbol,
            bid=100.0,
            ask=101.0,
            last=100.5,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Get OHLCV data for symbol."""
        return []

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create order from request."""
        return ExchangeOrderSnapshot(
            id="test_order_123",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            filled=0.0,
            remaining=0.0,
            status=ExchangeOrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel order by ID."""
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id="",
            symbol=symbol or "BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
            filled=0.0,
            remaining=0.0,
            status=ExchangeOrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get order by ID."""
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id="",
            symbol=symbol or "BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
            filled=0.5,
            remaining=0.0,
            status=ExchangeOrderStatusEnum.PARTIALLY_FILLED,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get list of orders."""
        return []

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get account balances."""
        return {}

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticker updates."""
        yield TickerUpdate(
            symbol=symbols[0],
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=1000.0,
            vwap=100.3,
            low=99.0,
            high=102.0,
            change=0.5,
            change_pct=0.5,
        )

    async def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candle updates."""
        yield CandleUpdate(
            symbol=symbols[0],
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            vwap=100.0,
            trades=100,
            volume=1000.0,
            interval_begin=datetime.now(UTC),
            interval=1,
        )

    async def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to trade updates."""
        yield TradeUpdate(
            symbol=symbols[0],
            side="buy",
            quantity=1.0,
            price=100.0,
            ord_type="market",
            trade_id="12345",
            timestamp=datetime.now(UTC),
        )

    async def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates."""
        yield ExecutionUpdate(
            order_id="test_order",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
        )

    async def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument updates."""
        yield {"symbol": "BTC-USD", "base": "BTC", "quote": "USD"}


@pytest.mark.asyncio()
async def test_context_manager_calls_connect_and_disconnect() -> None:
    """Context manager handles connection lifecycle.

    Given: DummyExchangeClient instance,
    When: Used as async context manager,
    Then: connect called on enter, disconnect on exit.
    """
    client = DummyExchangeClient()
    assert not client.connected
    assert not client.disconnected
    async with client:
        assert client.connected
        assert not client.disconnected
    assert client.connected
    assert client.disconnected


class _FailingConnectClient(DummyExchangeClient):
    """Client whose connect always fails, recording cleanup calls."""

    def __init__(self, exc: BaseException) -> None:
        """Initialize with the exception connect should raise."""
        super().__init__()
        self._exc = exc

    async def connect(self) -> None:
        """Raise the configured failure."""
        raise self._exc


class _FailingCleanupClient(_FailingConnectClient):
    """Client whose connect AND disconnect both fail."""

    async def disconnect(self) -> None:
        """Raise from cleanup to prove the original error wins."""
        raise ValueError("cleanup also failed")


@pytest.mark.asyncio()
async def test_aenter_failed_connect_triggers_disconnect() -> None:
    """A failed connect inside ``async with`` still cleans up the client.

    Given: A client whose connect raises,
    When: It is used as an async context manager,
    Then: The error propagates AND disconnect ran — ``__aexit__`` never
    fires after a failed ``__aenter__`` and no caller reliably stops a
    process that died inside connect, so this is the only cleanup seam.
    """
    client = _FailingConnectClient(RuntimeError("connect failed"))
    with pytest.raises(RuntimeError, match="connect failed"):
        async with client:
            pytest.fail("context body must not run")
    assert client.disconnected


@pytest.mark.asyncio()
async def test_aenter_cancelled_connect_triggers_disconnect() -> None:
    """A cancelled connect cleans up exactly like a failed one.

    Given: A client whose connect raises CancelledError,
    When: It is used as an async context manager,
    Then: The cancellation propagates (BaseException path) and
    disconnect ran.
    """
    client = _FailingConnectClient(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        async with client:
            pytest.fail("context body must not run")
    assert client.disconnected


@pytest.mark.asyncio()
async def test_aenter_cleanup_error_does_not_mask_connect_failure() -> None:
    """The ORIGINAL connect failure propagates even when cleanup fails.

    Given: A client whose connect and disconnect both raise,
    When: It is used as an async context manager,
    Then: The connect error (not the cleanup error) reaches the caller.
    """
    client = _FailingCleanupClient(RuntimeError("connect failed"))
    with pytest.raises(RuntimeError, match="connect failed"):
        async with client:
            pytest.fail("context body must not run")


@pytest.mark.asyncio()
async def test_dispatch_blocking_runs_on_named_pool_thread() -> None:
    """Blocking dispatch leaves the event loop and uses the client's pool.

    Given: A client and a sync callable recording its thread identity,
    When: ``_dispatch_blocking(callable)`` is awaited twice,
    Then: The callable runs off the loop thread, on a thread named with
    the per-exchange prefix, and the lazily created pool is reused.
    """
    client = DummyExchangeClient(exchange_name="dummyx")
    main_thread_id = threading.get_ident()
    seen: list[tuple[int, str]] = []

    def sync_call() -> str:
        seen.append((threading.get_ident(), threading.current_thread().name))
        return "ok"

    assert client._rest_pool is None
    result = await client._dispatch_blocking(sync_call)
    first_pool = client._rest_pool
    assert result == "ok"
    assert first_pool is not None
    assert seen[0][0] != main_thread_id
    assert seen[0][1].startswith("dummyx-rest")
    await client._dispatch_blocking(sync_call)
    assert client._rest_pool is first_pool


@pytest.mark.asyncio()
async def test_dispatch_blocking_passes_kwargs() -> None:
    """Keyword arguments reach the dispatched callable via partial.

    Given: A sync callable with positional and keyword parameters,
    When: ``_dispatch_blocking`` is awaited with both kinds of arguments,
    Then: The callable receives them unchanged (run_in_executor itself
    accepts no kwargs, so the partial path is load-bearing).
    """
    client = DummyExchangeClient()

    def sync_call(left: str, *, right: str) -> str:
        return f"{left}:{right}"

    result = await client._dispatch_blocking(sync_call, "a", right="b")
    assert result == "a:b"


@pytest.mark.asyncio()
async def test_shutdown_rest_pool_idempotent_and_blocks_dispatch() -> None:
    """Shutdown closes the pool, clears the slot, and stays idempotent.

    Given: A client whose pool was created by a successful dispatch,
    When: ``_shutdown_rest_pool`` runs twice and dispatch is retried,
    Then: The slot is cleared, the second shutdown is a no-op, and the
    retry fails loudly instead of resurrecting a pool nobody cleans up.
    """
    client = DummyExchangeClient(exchange_name="dummyx")
    await client._dispatch_blocking(lambda: "ok")
    assert client._rest_pool is not None
    client._shutdown_rest_pool()
    assert client._rest_pool is None
    assert client._rest_pool_closed
    client._shutdown_rest_pool()
    assert client._rest_pool is None
    with pytest.raises(RuntimeError, match="REST thread pool is closed"):
        await client._dispatch_blocking(lambda: "ok")


@pytest.mark.asyncio()
async def test_dispatch_spawn_failure_raises_ambiguous_dispatch_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-enqueue scheduling failure surfaces as RestPoolDispatchError.

    Given: An executor submit that ENQUEUES the work item and then fails
        with "can't start new thread" (the stdlib enqueues before
        spawning a worker, so the callable stays queued and may still
        drain on a freed worker),
    When: A blocking call is dispatched,
    Then: RestPoolDispatchError surfaces after exactly ONE scheduling
        attempt — submit paths classify it as ambiguous instead of a
        false definitive reject, and nothing retries the enqueued work.
    """
    client = DummyExchangeClient()
    loop = asyncio.get_running_loop()
    attempts: list[Any] = []

    def enqueue_then_fail(_pool: Any, fn: Any) -> Any:
        attempts.append(fn)
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(loop, "run_in_executor", enqueue_then_fail)
    with pytest.raises(RestPoolDispatchError, match="can't start new thread"):
        await client._dispatch_blocking(lambda: "queued-order")
    assert len(attempts) == 1
    client._shutdown_rest_pool()


@pytest.mark.asyncio()
async def test_dispatch_shutdown_race_keeps_plain_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pre-enqueue shutdown refusal keeps its loud native type.

    Given: An executor submit refused with "cannot schedule new futures
        after shutdown" (raised BEFORE the work item is enqueued — the
        callable was definitively NOT sent),
    When: A blocking call is dispatched,
    Then: Plain RuntimeError propagates (lifecycle-bug semantics), NOT
        the ambiguous RestPoolDispatchError.
    """
    client = DummyExchangeClient()
    loop = asyncio.get_running_loop()

    def refuse_pre_enqueue(_pool: Any, fn: Any) -> Any:
        raise RuntimeError("cannot schedule new futures after shutdown")

    monkeypatch.setattr(loop, "run_in_executor", refuse_pre_enqueue)
    with pytest.raises(RuntimeError, match="after shutdown") as exc_info:
        await client._dispatch_blocking(lambda: "never")
    assert not isinstance(exc_info.value, RestPoolDispatchError)
    client._shutdown_rest_pool()


@pytest.mark.asyncio()
async def test_worker_raised_runtime_error_stays_plain() -> None:
    """A RuntimeError raised INSIDE the worker keeps its native type.

    Given: A callable that raises RuntimeError while running on the
        pool (a venue SDK error during the call itself, not a
        scheduling failure),
    When: It is dispatched,
    Then: The error propagates unchanged — never reclassified as the
        ambiguous RestPoolDispatchError — and the callee ran exactly
        once.
    """
    client = DummyExchangeClient()
    calls: list[int] = []

    def sdk_failure() -> None:
        calls.append(1)
        raise RuntimeError("venue SDK failure")

    with pytest.raises(RuntimeError, match="venue SDK failure") as exc_info:
        await client._dispatch_blocking(sdk_failure)
    assert not isinstance(exc_info.value, RestPoolDispatchError)
    assert calls == [1]
    client._shutdown_rest_pool()


@pytest.mark.asyncio()
async def test_dispatch_blocking_propagates_log_context_var() -> None:
    """ContextVars set by the caller are visible in the worker.

    Given: The snapper log-context ContextVar set in the calling task,
    When: A blocking callable reads it on the pool thread,
    Then: It observes the caller's value (asyncio.to_thread parity —
        the Polygon client logs inside worker callables and the log
        formatter reads this var there).
    """
    client = DummyExchangeClient()
    set_log_context("ctx-probe")
    seen: list[str] = []

    def read_context() -> None:
        seen.append(logging_module._log_context.get())

    await client._dispatch_blocking(read_context)
    assert seen == ["ctx-probe"]
    client._shutdown_rest_pool()


def test_shutdown_rest_pool_without_pool_is_noop() -> None:
    """Shutdown before any dispatch only flips the closed flag.

    Given: A fresh client that never dispatched (lazy pool not built),
    When: ``_shutdown_rest_pool`` runs,
    Then: No pool exists and the closed flag is set.
    """
    client = DummyExchangeClient()
    client._shutdown_rest_pool()
    assert client._rest_pool is None
    assert client._rest_pool_closed


@pytest.mark.asyncio()
async def test_reopen_rest_pool_builds_fresh_pool_after_shutdown() -> None:
    """Reopen restores dispatch with a brand-new pool.

    Given: A client shut down after building a pool,
    When: ``_reopen_rest_pool`` runs and dispatch is awaited again,
    Then: A fresh pool is created and the call succeeds — the
    same-instance reconnect contract used by ``connect()``.
    """
    client = DummyExchangeClient(exchange_name="dummyx")
    await client._dispatch_blocking(lambda: "ok")
    client._shutdown_rest_pool()
    client._reopen_rest_pool()
    assert not client._rest_pool_closed
    result = await client._dispatch_blocking(lambda: "again")
    assert result == "again"
    assert client._rest_pool is not None
    client._shutdown_rest_pool()


@pytest.mark.asyncio()
async def test_log_order_to_db_returns_none_when_no_repository() -> None:
    """Log order returns None without repository.

    Given: Client without repository configured,
    When: _log_order_to_db is called,
    Then: Returns None without error.
    """
    client = DummyExchangeClient(repository=None)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC).timestamp(),
    )
    result = await client._log_order_to_db(request, order)
    assert result is None


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    side_effect=RuntimeError("connection pool torn down"),
)
async def test_log_order_to_db_never_raises(mock_resolve: AsyncMock) -> None:
    """Any failure in the post-accept DB write returns None, never raises.

    Given: A repository layer raising a non-SQLAlchemy error (pool
        teardown, timeout) during the auxiliary post-accept write,
    When: _log_order_to_db is called,
    Then: It returns None — an escaping exception would ride the venue
        client back into the executor's definitive-reject branch and
        misreport a LIVE order as rejected.
    """
    mock_repo = MagicMock(spec=Repository)
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC).timestamp(),
    )
    result = await client._log_order_to_db(request, order)
    assert result is None
    mock_resolve.assert_awaited_once()


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_logs_successfully(mock_resolve: AsyncMock) -> None:
    """Log order persists to database.

    Given: Client with mock repository,
    When: _log_order_to_db is called with order data,
    Then: Order is inserted and ID returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    tracker = SequenceTracker()
    client.set_tracker(tracker)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-123")
    mock_resolve.assert_awaited_once_with(mock_repo, "BTC-USD", as_of=ANY)
    call_kwargs = mock_repo.ensure_instrument.call_args.kwargs
    assert call_kwargs["symbol_public_id"] == "fake-spid"
    assert call_kwargs["exchange"] == "dummy"
    assert call_kwargs["session_id"] == tracker.session_id
    assert call_kwargs["sequence_id"] >= 1
    mock_repo.insert_order.assert_awaited_once()
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["leverage"] is None
    assert insert_kwargs["reduce_only"] is False


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_propagates_leverage_and_reduce_only(
    mock_resolve: AsyncMock,
) -> None:
    """Log order forwards leverage/reduce_only from request to repository.

    Given: An ExchangeOrderRequest with leverage=5 and reduce_only=True
        (mimicking a margin reduce-only order),
    When: _log_order_to_db is invoked,
    Then: insert_order is called with the same leverage/reduce_only so the
        Order DB row carries the margin/intent metadata for the frontend
        and SCD2 history.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_lev",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        leverage=5,
        reduce_only=True,
    )
    order = ExchangeOrderSnapshot(
        id="order_lev",
        client_order_id="client_lev",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    await client._log_order_to_db(request, order)
    mock_resolve.assert_awaited_once()
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["leverage"] == 5
    assert insert_kwargs["reduce_only"] is True


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value=None,
)
async def test_log_order_to_db_returns_none_when_symbol_not_resolved(
    mock_resolve: AsyncMock,
) -> None:
    """Log order returns None when no active Symbol row exists.

    Given: Client with repository configured,
    When: _log_order_to_db is called and symbol resolution returns None,
    Then: No instrument or order write is attempted.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result is None
    mock_resolve.assert_awaited_once_with(mock_repo, "BTC-USD", as_of=ANY)
    mock_repo.ensure_instrument.assert_not_awaited()
    mock_repo.insert_order.assert_not_awaited()


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_parses_symbol_without_delimiter(_mock_resolve: AsyncMock) -> None:
    """Log order parses base/quote from symbol without delimiter.

    Given: Client with mock repository and symbol without dash,
    When: _log_order_to_db is called,
    Then: base equals the full symbol and quote defaults to USD.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    tracker = SequenceTracker()
    client.set_tracker(tracker)
    request = ExchangeOrderRequest(
        client_order_id="client_456",
        symbol="BTCUSD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_456",
        client_order_id="client_456",
        symbol="BTCUSD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-123")
    _mock_resolve.assert_awaited_once_with(mock_repo, "BTCUSD", as_of=ANY)
    call_kwargs = mock_repo.ensure_instrument.call_args.kwargs
    assert call_kwargs["symbol_public_id"] == "fake-spid"
    assert call_kwargs["exchange"] == "dummy"
    assert call_kwargs["session_id"] == tracker.session_id
    assert call_kwargs["sequence_id"] >= 1


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_handles_exception(_mock_resolve: AsyncMock) -> None:
    """Log order handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_order_to_db is called,
    Then: Returns None without propagating error.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC).timestamp(),
    )
    result = await client._log_order_to_db(request, order)
    assert result is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_returns_none_when_no_repository() -> None:
    """Log order update returns None without repository.

    Given: Client without repository configured,
    When: _log_order_update_to_db is called,
    Then: Returns None without error.
    """
    client = DummyExchangeClient(repository=None)
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=ExchangeOrderStatusEnum.FILLED,
        exchange_order_id="order_123",
    )
    assert result is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_returns_new_order_id() -> None:
    """Log order update returns new order version id.

    Given: Client with mock repository returning new order id,
    When: _log_order_update_to_db is called with status,
    Then: Repository update_order is called and new id returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.update_order = AsyncMock(return_value=99)
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=ExchangeOrderStatusEnum.FILLED,
        exchange_order_id="order_123",
        error=None,
    )
    assert result == 99
    mock_repo.update_order.assert_called_once()
    call_args = mock_repo.update_order.call_args[1]
    assert call_args["order_id"] == 42
    assert call_args["status"] == "filled"
    assert call_args["exchange_order_id"] == "order_123"
    assert call_args["error"] is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_stamps_provenance_from_tracker() -> None:
    """Log order update passes provenance from injected SequenceTracker.

    Given: Client with a SequenceTracker injected,
    When: _log_order_update_to_db is called,
    Then: update_order receives session_id and sequence_id from the tracker.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.update_order = AsyncMock(return_value=55)
    tracker = SequenceTracker()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(tracker)
    result = await client._log_order_update_to_db(
        db_order_id=10,
        status=ExchangeOrderStatusEnum.FILLED,
    )
    assert result == 55
    call_kwargs = mock_repo.update_order.call_args[1]
    assert call_kwargs["session_id"] == tracker.session_id
    assert call_kwargs["sequence_id"] == 1


@pytest.mark.asyncio()
async def test_log_order_update_to_db_handles_exception() -> None:
    """Log order update handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_order_update_to_db is called,
    Then: Exception is suppressed and None returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.update_order = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=ExchangeOrderStatusEnum.FILLED,
    )
    assert result is None


@pytest.mark.asyncio()
async def test_log_execution_to_db_returns_early_when_no_repository() -> None:
    """Log execution returns early without repository.

    Given: Client without repository configured,
    When: _log_execution_to_db is called,
    Then: Returns immediately without error.
    """
    client = DummyExchangeClient(repository=None)
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=1.0,
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-1",
        wallet_public_id="",
        execution=execution,
    )


@pytest.mark.asyncio()
async def test_log_execution_to_db_logs_successfully() -> None:
    """Log execution persists fill data.

    Given: Client with mock repository,
    When: _log_execution_to_db is called with execution,
    Then: Execution is inserted with price and fee.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        exec_id="exec-123",
        trade_id=987654321,
        last_price=50000.0,
        last_qty=1.0,
        fee_usd_equiv=10.0,
        last_price_decimal="50000.000000000000000005",
        last_qty_decimal="1.000000000000000005",
        fee_usd_equiv_decimal="10.000000000000000005",
        counter_amount_decimal="50000.000000000000000005",
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-1",
        wallet_public_id="",
        execution=execution,
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["order_public_id"] == "order-pub-1"
    assert call_args["price"] == pytest.approx(50000.0)
    assert call_args["size"] == pytest.approx(1.0)
    assert call_args["fee"] == pytest.approx(10.0)
    assert call_args["status"] == "filled"
    assert call_args["fee_asset"] == "USD"
    assert call_args["exec_id"] == "exec-123"
    assert call_args["trade_id"] == "987654321"
    assert call_args["price_decimal"] == "50000.000000000000000005"
    assert call_args["size_decimal"] == "1.000000000000000005"
    assert call_args["fee_decimal"] == "10.000000000000000005"
    assert call_args["counter_amount_decimal"] == "50000.000000000000000005"
    assert call_args["numeric_provenance"] == "venue_raw"


@pytest.mark.asyncio()
async def test_log_execution_to_db_counter_only_row_is_venue_raw() -> None:
    """A fill carrying only the exact counter amount still stamps venue_raw.

    Given: An execution whose only raw evidence is the counter amount decimal
        (no matching price, size, or fee decimal),
    When: _log_execution_to_db is called,
    Then: The counter amount is persisted verbatim and provenance is venue_raw
        because at least one exact raw field is present.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=1.0,
        fee_usd_equiv=10.0,
        counter_amount_decimal="50000.30",
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-1",
        wallet_public_id="",
        execution=execution,
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["price_decimal"] is None
    assert call_args["size_decimal"] is None
    assert call_args["fee_decimal"] is None
    assert call_args["counter_amount_decimal"] == "50000.30"
    assert call_args["numeric_provenance"] == "venue_raw"


async def test_log_execution_to_db_drops_counter_when_a_gap_is_absorbed() -> None:
    """An absorbed publish gap drops the exact counter, not pairs it with a full size.

    Given: A fill whose executor-resolved size (2.5) spans more than the venue's
        per-poll last_qty (1.0) because an earlier fill's publish was absorbed,
        while the counter amount is only that single poll's amount,
    When: _log_execution_to_db persists it,
    Then: The counter is dropped (its poll-scoped amount would understate the
        quote leg against the gap-absorbing size), so the replay falls back to
        the tolerant size-times-price fold rather than an exact wrong quote.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=1.0,
        fee_usd_equiv=10.0,
        counter_amount_decimal="50000.30",
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-1",
        wallet_public_id="",
        execution=execution,
        delta_size=2.5,
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["counter_amount_decimal"] is None
    assert call_args["numeric_provenance"] == "legacy_float"


@pytest.mark.asyncio()
async def test_log_execution_to_db_uses_fallback_values() -> None:
    """Log execution uses fallback for missing fields.

    Given: Execution with None last_price and last_qty,
    When: _log_execution_to_db is called,
    Then: Uses average_price and cum_qty as fallbacks.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=None,
        average_price=49500.0,
        last_qty=None,
        cum_qty=2.5,
        fee_usd_equiv=None,
        average_price_decimal="49500.000000000000000005",
        cum_qty_decimal="2.500000000000000005",
        cum_fee=0.0,
        cum_fee_decimal="0.0",
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-2",
        wallet_public_id="",
        execution=execution,
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["status"] == "filled"
    assert call_args["price"] == pytest.approx(49500.0)
    assert call_args["size"] == pytest.approx(2.5)
    assert call_args["fee"] == pytest.approx(0.0)
    assert call_args["price_decimal"] == "49500.000000000000000005"
    assert call_args["size_decimal"] == "2.500000000000000005"
    assert call_args["fee_decimal"] == "0.0"


@pytest.mark.asyncio()
async def test_log_execution_to_db_partial_fill_status() -> None:
    """Log execution stores partial status for open orders with fills.

    Given: Execution with OPEN status and positive cum_qty,
    When: _log_execution_to_db is called,
    Then: Status is stored as 'partial' (not raw order_status value).
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_456",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=0.5,
        cum_qty=0.5,
        fee_usd_equiv=None,
        fees=[
            ExecutionFeeBreakdown(
                asset="BTC",
                quantity=5.0,
                quantity_decimal="5.000000000000000005",
            )
        ],
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-3",
        wallet_public_id="",
        execution=execution,
        fee=5.0,
        fee_asset="BTC",
    )
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["status"] == "partial"
    assert call_args["fee_decimal"] == "5.000000000000000005"


@pytest.mark.asyncio()
async def test_log_execution_to_db_uses_cumulative_decimal_for_ambiguous_fees() -> None:
    """Log execution falls back when multiple raw fees match.

    Given: Two matching fee rows and an exact cumulative fee,
    When: _log_execution_to_db resolves the fee decimal,
    Then: The cumulative decimal is persisted instead of an ambiguous row.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_ambiguous_fee",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        fees=[
            ExecutionFeeBreakdown(
                asset="BTC",
                quantity=5.0,
                quantity_decimal="5.000000000000000005",
            ),
            ExecutionFeeBreakdown(
                asset="BTC",
                quantity=5.0,
                quantity_decimal="5.000000000000000006",
            ),
        ],
        cum_fee=5.0,
        cum_fee_decimal="5.000000000000000007",
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-ambiguous-fee",
        wallet_public_id="",
        execution=execution,
        fee=5.0,
        fee_asset="BTC",
    )
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["fee_decimal"] == "5.000000000000000007"


@pytest.mark.asyncio()
async def test_log_execution_to_db_handles_exception() -> None:
    """Log execution handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_execution_to_db is called,
    Then: Exception is suppressed.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
    )
    await client._log_execution_to_db(
        order_public_id="order-pub-4",
        wallet_public_id="",
        execution=execution,
    )


@pytest.mark.asyncio()
async def test_log_execution_to_db_swallows_scope_resolution_refusal() -> None:
    """A fail-closed scope refusal is logged loudly and never crashes the pipeline.

    The repository refuses a fill whose active Order -> Instrument lineage
    is missing or crossed; persistence keeps log-and-continue parity with
    database errors — the fill stays published but unpersisted.

    Given: A repository whose ``insert_execution`` raises the typed
        ``ExecutionScopeResolutionError`` refusal.
    When: ``_log_execution_to_db`` is called.
    Then: The refusal is swallowed after an error log naming the reason.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock(
        side_effect=ExecutionScopeResolutionError("dangling_execution_order_lineage")
    )
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
    )
    with patch("snapper.infrastructure.exchanges.base.logger.error") as error_log:
        await client._log_execution_to_db(
            order_public_id="order-pub-5",
            wallet_public_id="",
            execution=execution,
        )
    error_log.assert_called_once()
    assert "dangling_execution_order_lineage" in error_log.call_args.args[0]


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_passes_provenance_when_tracker_set(
    mock_resolve: AsyncMock,
) -> None:
    """Log order passes session_id and sequence_id when tracker is injected.

    Given: Client with mock repository and an injected SequenceTracker,
    When: _log_order_to_db is called,
    Then: ensure_instrument receives non-empty session_id and sequence_id >= 1.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-99"))
    client = DummyExchangeClient(repository=mock_repo)
    tracker = SequenceTracker()
    client.set_tracker(tracker)
    request = ExchangeOrderRequest(
        client_order_id="client_prov",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_prov",
        client_order_id="client_prov",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-99")
    call_kwargs = mock_repo.ensure_instrument.call_args.kwargs
    assert call_kwargs["session_id"] == tracker.session_id
    assert call_kwargs["sequence_id"] >= 1


def test_subscription_health_snapshot_returns_empty_without_tracker() -> None:
    """Snapshot accessor degrades gracefully without a tracker.

    Given: a client whose ``_health_tracker`` is None,
    When: ``subscription_health_snapshot`` is called,
    Then: it returns an empty mapping rather than raising.
    """
    client = DummyExchangeClient()
    assert client._health_tracker is None
    assert client.subscription_health_snapshot() == {}


def test_subscription_health_snapshot_returns_tracker_state() -> None:
    """Snapshot accessor mirrors the tracker's entries.

    Given: a client with a tracker holding one pending entry,
    When: ``subscription_health_snapshot`` is called,
    Then: the mapping reflects that tracked (channel, symbol) and status.
    """
    client = DummyExchangeClient()
    client._health_tracker = SubscriptionHealthTracker()
    client._health_tracker.mark_pending("ticker", "BTC/USD")
    snapshot = client.subscription_health_snapshot()
    assert set(snapshot.keys()) == {("ticker", "BTC/USD")}
    assert snapshot[("ticker", "BTC/USD")].status == "pending"


@pytest.mark.asyncio
async def test_health_loop_logs_stale_before_retry_passes() -> None:
    """Stale-subscription logging precedes the retry passes each cycle.

    Given: A client with a tracker and a health loop allowed one cycle,
    When: The loop runs one iteration,
    Then: _log_stale_subscriptions runs BEFORE the retry passes, so a
        blocking retry can never mute stale telemetry again (in the
        2026-06-09 incident a wedged retry silenced every stale warning for
        the publisher's whole connected-but-dark lifetime).
    """
    client = DummyExchangeClient()
    client._health_tracker = MagicMock(retry_interval_s=0.0)
    order: list[str] = []

    def _log(_tracker: object) -> None:
        order.append("log")

    async def _overdue(_tracker: object) -> None:
        order.append("overdue")
        client._health_loop_running = False

    async def _failed(_tracker: object) -> None:
        order.append("failed")

    async def _dark(_tracker: object) -> None:
        order.append("dark")

    with (
        patch.object(client, "_log_stale_subscriptions", side_effect=_log),
        patch.object(
            client, "_retry_overdue_pending_subscriptions", AsyncMock(side_effect=_overdue)
        ),
        patch.object(client, "_retry_due_failed_subscriptions", AsyncMock(side_effect=_failed)),
        patch.object(client, "_recover_dark_subscriptions", AsyncMock(side_effect=_dark)),
    ):
        client._health_loop_running = True
        await client._subscription_health_loop()
    assert order == ["log", "overdue", "failed", "dark"]


@pytest.mark.asyncio()
async def test_find_order_by_client_id_default_raises() -> None:
    """The base lookup refuses rather than guessing absence.

    Given: A venue client without a client-id lookup implementation,
    When: find_order_by_client_id is called,
    Then: NotImplementedError names the venue — the caller must treat
        this as could-not-verify, never as authoritative absence (only
        an explicit None return means the venue confirmed absence).
    """
    client = DummyExchangeClient(repository=None)
    with pytest.raises(NotImplementedError, match="dummy"):
        await client.find_order_by_client_id("cid-x", "BTC-USD")


@pytest.mark.asyncio
async def test_get_order_fill_summary_defaults_to_none() -> None:
    """The base fill-summary hook returns None for venues without a fills lookup.

    Given: a client that does not override get_order_fill_summary,
    When: the hook is called,
    Then: it returns None, so fill-gap reconciliation keeps its documented
        fail-safe skip on such venues.
    """
    client = DummyExchangeClient(repository=None)
    assert await client.get_order_fill_summary("ex-1") is None


def test_account_history_capability_defaults_unsupported() -> None:
    """The base account-history capability fail-closes to UNSUPPORTED.

    Given: A venue client that does not override the capability,
    When: The class attribute is inspected,
    Then: It is UNSUPPORTED so the spot-anchor bootstrap never runs against a
        venue that cannot be faithfully account-history read.
    """
    assert ExchangeClientBase.account_history_capability is CapabilityStatus.UNSUPPORTED


@pytest.mark.asyncio()
async def test_read_account_history_tip_default_raises() -> None:
    """The base account-history tip read fail-closes rather than fabricate a tip.

    Given: A venue client without an account-history implementation,
    When: read_account_history_tip is called,
    Then: NotImplementedError is raised so an unsupported venue never anchors.
    """
    client = DummyExchangeClient(repository=None)
    with pytest.raises(NotImplementedError, match="account history"):
        await client.read_account_history_tip(200)


@pytest.mark.asyncio()
async def test_read_account_history_range_default_raises() -> None:
    """The base account-history range read fail-closes rather than fabricate rows.

    Given: A venue client without an account-history implementation,
    When: read_account_history_range is called,
    Then: NotImplementedError is raised so an unsupported venue never certifies
        a cursor range.
    """
    client = DummyExchangeClient(repository=None)
    with pytest.raises(NotImplementedError, match="account history"):
        await client.read_account_history_range(0, 100)


@pytest.mark.asyncio()
async def test_read_order_fill_legs_default_raises() -> None:
    """The base order-fill-legs read fail-closes on an unsupported venue.

    Given: A venue client without an account-history implementation,
    When: read_order_fill_legs is called,
    Then: NotImplementedError is raised.
    """
    client = DummyExchangeClient(repository=None)
    with pytest.raises(NotImplementedError, match="order fill leg"):
        await client.read_order_fill_legs("ex-1")


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_normalizes_wire_order_type_to_core(
    mock_resolve: AsyncMock,
) -> None:
    """The durable orders row speaks CORE, not the venue wire value (#156).

    Given: an accepted stop order whose venue snapshot carries the wire
        type 'stop-loss',
    When: _log_order_to_db persists it,
    Then: insert_order receives order_type='stop' so GET /orders
        serialization (OrderData CORE Literal) cannot 500 on the first
        accepted stop; coinciding values (limit) stay untouched.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-stop"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_stop",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.STOP_LOSS,
        amount=1.0,
        stop_price=48000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_stop",
        client_order_id="client_stop",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.STOP_LOSS,
        amount=1.0,
        price=None,
        filled=0.0,
        remaining=1.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-stop")
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["order_type"] == "stop"


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_persists_request_type_over_snapshot_type(
    mock_resolve: AsyncMock,
) -> None:
    """The orders row records the REQUEST's intent, not the snapshot (#156).

    Given: an adoption-repair shape — the reconstructed request says
        stop-loss-limit while the fetched ccxt snapshot collapsed the
        type to plain LIMIT,
    When: _log_order_to_db persists it,
    Then: insert_order receives order_type='stop_limit' from the request
        so a repaired protective stop is never durably recorded as a
        plain limit order.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-repair"))
    client = DummyExchangeClient(repository=mock_repo)
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_repair",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        amount=1.0,
        price=47900.0,
        stop_price=48000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_repair",
        client_order_id="client_repair",
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=47900.0,
        filled=0.0,
        remaining=1.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-repair")
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["order_type"] == "stop_limit"


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_labels_paper_exchange_as_paper_mode(
    mock_resolve: AsyncMock,
) -> None:
    """A paper-venue client durably records the order as ``mode='paper'``.

    Given: a client whose ``exchange_name`` is the paper venue,
    When: _log_order_to_db persists an accepted order,
    Then: insert_order receives ``mode='paper'`` — the repository default
        of ``live`` previously mislabeled paper orders, and mode
        participates in the active-order uniqueness index and fill-gap
        recovery matching, so the label is operational, not cosmetic.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-paper"))
    client = DummyExchangeClient(repository=mock_repo, exchange_name="paper")
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_paper",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_paper",
        client_order_id="client_paper",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-paper")
    mock_resolve.assert_awaited_once()
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["mode"] == "paper"


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_labels_non_paper_exchange_as_live_mode(
    mock_resolve: AsyncMock,
) -> None:
    """A real-venue client durably records the order as ``mode='live'``.

    Given: a client whose ``exchange_name`` is a live venue (kraken),
    When: _log_order_to_db persists an accepted order,
    Then: insert_order receives ``mode='live'`` — anything that is not the
        paper venue maps to live, mirroring the engine mode rule.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.ensure_instrument = AsyncMock(return_value=(42, "inst-pub-42"))
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-live"))
    client = DummyExchangeClient(repository=mock_repo, exchange_name="kraken")
    client.set_tracker(SequenceTracker())
    request = ExchangeOrderRequest(
        client_order_id="client_live",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_live",
        client_order_id="client_live",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=ExchangeOrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-live")
    mock_resolve.assert_awaited_once()
    insert_kwargs = mock_repo.insert_order.call_args.kwargs
    assert insert_kwargs["mode"] == "live"


def test_parse_execution_exec_id_defaults_to_none() -> None:
    """The base exec-id parse returns None for venues without a scheme.

    Given: A venue client that does not override the exec-id parse,
    When: parse_execution_exec_id is called,
    Then: None is returned so an unsupported venue never witnesses.
    """
    client = DummyExchangeClient(repository=None)
    assert client.parse_execution_exec_id("wal-x-c1") is None
