"""Fixtures for paper-mode E2E integration tests.

Spins up a real ZmqBrokerThread, PaperOrderExecutor, and TraderCoordinator
in-process and connects them via random TCP loopback ports. The
mock_settings_for_tests autouse fixture from the root conftest.py is
extended here with broker endpoint overrides + allow_short_selling = True.

The fixture is function-scoped (not session-scoped) so each test gets a
clean broker, executor, trader, and SQLite DB. ZMQ contexts are torn down
by the cleanup_zmq_sockets autouse fixture in the root conftest.
"""

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio

import snapper.application.engine.trader as snapper_trader
import snapper.config.settings as snapper_settings
from snapper.application.engine.trader import TraderCoordinator
from snapper.messaging.executors.paper import PaperOrderExecutor
from snapper.messaging.infrastructure.broker import ZmqBrokerThread


def _free_tcp_port() -> int:
    """Allocate a free TCP port on localhost.

    Tiny race window between socket close and broker bind, but acceptable
    for in-process tests.

    Returns:
        Free TCP port number on 127.0.0.1.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@dataclass
class PaperE2EStack:
    """Container for the live paper-mode test stack.

    Attributes:
        broker: Running ZmqBrokerThread.
        executor: Running PaperOrderExecutor (background task).
        trader: Running TraderCoordinator (background task).
        executor_task: Background asyncio task for executor.start().
        trader_task: Background asyncio task for trader.start().
        xsub_endpoint: Broker XSUB endpoint (publishers connect here).
        xpub_endpoint: Broker XPUB endpoint (subscribers connect here).
        client_context: zmq.asyncio.Context owned by the test for ad-hoc
            publisher/subscriber sockets used to inject signals and observe
            events. Closed during teardown.
    """

    broker: ZmqBrokerThread
    executor: PaperOrderExecutor
    trader: TraderCoordinator
    executor_task: asyncio.Task[None]
    trader_task: asyncio.Task[None]
    xsub_endpoint: str
    xpub_endpoint: str
    client_context: zmq.asyncio.Context


def _patch_settings_for_e2e(
    monkeypatch: pytest.MonkeyPatch,
    xsub_endpoint: str,
    xpub_endpoint: str,
) -> None:
    """Override mock_settings with broker endpoints and short selling on.

    The root conftest's mock_settings_for_tests autouse fixture has already
    patched get_settings to return a Mock. We mutate the Mock here, and
    additionally patch trader/executor local imports of
    get_settings_with_service so _initialize_settings does not replace
    the mock at start time.

    Args:
        monkeypatch: pytest monkeypatch fixture for module-level patches.
        xsub_endpoint: Broker XSUB endpoint to inject into settings.
        xpub_endpoint: Broker XPUB endpoint to inject into settings.
    """
    mock_settings = snapper_settings.get_settings()
    mock_settings.zmq_broker_xsub = xsub_endpoint
    mock_settings.zmq_broker_xpub = xpub_endpoint
    mock_settings.allow_short_selling = True
    mock_settings.has_db_access = True
    mock_settings.use_venue_reconciliation = False
    mock_settings.use_durable_commands = False

    def _return_mock(_service: object = None) -> object:
        return mock_settings

    async def _return_mock_service(*_args: object, **_kwargs: object) -> object:
        return object()

    monkeypatch.setattr(
        "snapper.application.engine.trader.get_settings_with_service",
        _return_mock,
    )
    monkeypatch.setattr(
        "snapper.application.engine.trader.get_settings_service",
        _return_mock_service,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.base.get_settings_with_service",
        _return_mock,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.base.get_settings_service",
        _return_mock_service,
    )
    monkeypatch.setattr(
        snapper_trader,
        "_bootstrap_settings",
        SimpleNamespace(zmq_broker_xpub=xpub_endpoint, zmq_broker_xsub=xsub_endpoint),
    )


async def _wait_for_executor_subscription(stack_xsub: str, instrument: str) -> None:
    """Best-effort wait for ZMQ slow-joiner stabilization.

    XPUB/XSUB does not synchronously confirm a subscription has propagated
    to the broker before the publisher's first send. Tests that publish
    immediately after stack startup may lose messages. A short asyncio
    sleep gives the proxy thread + executor subscriber loop time to
    establish the subscription before the test publishes.

    Args:
        stack_xsub: Broker XSUB endpoint (unused; kept for future
            handshake-based readiness check).
        instrument: Instrument symbol the test will publish for.
    """
    del stack_xsub, instrument
    await asyncio.sleep(0.5)


async def _stop_background_process(
    stop_coro: Awaitable[None],
    task: asyncio.Task[None],
) -> None:
    """Stop a background process gracefully before falling back to cancellation.

    Args:
        stop_coro: Awaitable returned by the process ``stop()`` method.
        task: Background task running the process ``start()`` loop.
    """
    with contextlib.suppress(Exception):
        await stop_coro
    if not task.done():
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
            await asyncio.wait_for(task, timeout=2.0)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
            await asyncio.wait_for(task, timeout=2.0)


@pytest_asyncio.fixture
async def paper_e2e_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[PaperE2EStack]:
    """Spin up broker + paper executor + trader on random ports.

    Yields a `PaperE2EStack` for the test to inject signals into via
    `client_context` and observe order/execution events from. All
    components are torn down on test exit; ZMQ contexts are cleaned up
    by the autouse cleanup_zmq_sockets fixture from the root conftest.
    """
    xsub_port = _free_tcp_port()
    xpub_port = _free_tcp_port()
    xsub_endpoint = f"tcp://127.0.0.1:{xsub_port}"
    xpub_endpoint = f"tcp://127.0.0.1:{xpub_port}"
    _patch_settings_for_e2e(monkeypatch, xsub_endpoint, xpub_endpoint)

    broker = ZmqBrokerThread(xsub_endpoint=xsub_endpoint, xpub_endpoint=xpub_endpoint)
    broker.start()

    executor = PaperOrderExecutor()
    executor._credentials = {"initial_balance": "1000000"}
    executor_task = asyncio.create_task(executor.start())

    trader = TraderCoordinator(signal_topics=["signals."])
    trader_task = asyncio.create_task(trader.start())

    client_context = zmq.asyncio.Context()

    await _wait_for_executor_subscription(xsub_endpoint, "BTC-USD")

    stack = PaperE2EStack(
        broker=broker,
        executor=executor,
        trader=trader,
        executor_task=executor_task,
        trader_task=trader_task,
        xsub_endpoint=xsub_endpoint,
        xpub_endpoint=xpub_endpoint,
        client_context=client_context,
    )
    try:
        yield stack
    finally:
        await _stop_background_process(trader.stop(), trader_task)
        await _stop_background_process(executor.stop(), executor_task)
        with contextlib.suppress(Exception):
            broker.stop()
        with contextlib.suppress(Exception):
            client_context.term()
