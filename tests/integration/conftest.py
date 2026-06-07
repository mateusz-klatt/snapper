"""Fixtures for paper-mode E2E integration tests.

Spins up a real ZmqBrokerThread, PaperOrderExecutor, and TraderCoordinator
in-process and connects them via random TCP loopback ports. The
mock_settings_for_tests autouse fixture from the root conftest.py is
extended here with broker endpoint overrides.

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
from typing import Any
from typing import cast

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio

import snapper.application.engine.trader as snapper_trader
import snapper.config.settings as snapper_settings
from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.messaging.executors.paper import PaperOrderExecutor
from snapper.messaging.infrastructure.broker import ZmqBrokerThread

_SIGNAL_TOPIC_PREFIX = "signals."


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
    mock_settings.has_db_access = True

    def _return_mock(_service: object = None) -> object:
        return mock_settings

    async def _return_mock_service(*_args: object, **_kwargs: object) -> object:
        await asyncio.sleep(0)
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

    trader = TraderCoordinator(signal_topics=[_SIGNAL_TOPIC_PREFIX])
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


def _per_coordinator_settings(
    base: Any,
    instance_id: int,
    instance_count: int = 2,
) -> SimpleNamespace:
    """Copy every attribute from ``base`` and overlay coordinator identity.

    Used by :func:`two_coordinator_stack` to produce per-instance
    settings that differ ONLY on ``coordinator_instance_id`` while
    sharing ``coordinator_instance_count``. All other fields
    (db_url, zmq endpoints, risk_*, etc.) come from the autouse mock
    so the two coordinators land in the same broker + DB.
    """
    fields: dict[str, Any] = {k: getattr(base, k) for k in dir(base) if not k.startswith("_")}
    fields["coordinator_instance_id"] = instance_id
    fields["coordinator_instance_count"] = instance_count
    return SimpleNamespace(**fields)


@dataclass
class TwoCoordinatorStack:
    """Container for the N=2 partitioning integration stack.

    Attributes:
        broker: Running ZmqBrokerThread shared by both coordinators.
        executor: Running PaperOrderExecutor (background task).
        traders: Tuple of the two running TraderCoordinator instances
            (trader_0 at instance_id=0, trader_1 at instance_id=1).
        trader_tasks: Background tasks for each ``trader.start()`` loop.
        executor_task: Background task for ``executor.start()``.
        xsub_endpoint: Broker XSUB endpoint (publishers connect here).
        xpub_endpoint: Broker XPUB endpoint (subscribers connect here).
        client_context: zmq.asyncio.Context owned by the test for
            signal injection / event observation.
        base_mock: Raw autouse mock settings object — scenario tests
            can build a third coordinator via
            :func:`_per_coordinator_settings` for restart scenarios.
    """

    broker: ZmqBrokerThread
    executor: PaperOrderExecutor
    traders: tuple[TraderCoordinator, TraderCoordinator]
    trader_tasks: tuple[asyncio.Task[None], asyncio.Task[None]]
    executor_task: asyncio.Task[None]
    xsub_endpoint: str
    xpub_endpoint: str
    client_context: zmq.asyncio.Context
    base_mock: Any


@pytest_asyncio.fixture
async def two_coordinator_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[TwoCoordinatorStack]:
    """Spin up broker + paper executor + TWO TraderCoordinator instances.

    Both coordinators share the same ZMQ broker + SQLite DB. Each is
    constructed with its own ``coordinator_instance_id`` (0, 1) via
    the ``settings=`` kwarg on ``TraderCoordinator.__init__``.
    The fixture reuses :func:`_patch_settings_for_e2e` for broker +
    executor + module-level ``_bootstrap_settings`` patching
    (essential — ``_setup_signal_subscriber`` reads the module-level
    variable), then threads per-coordinator ownership via
    ``settings=``.

    Readiness: gives the broker proxy thread + both coordinator
    subscribers time to establish subscriptions before yielding.
    The ``_wait_for_executor_subscription`` sleep-based approach works
    for these integration tests because they publish after the
    yield (giving the subscribers extra time beyond the initial
    sleep).
    """
    xsub_port = _free_tcp_port()
    xpub_port = _free_tcp_port()
    xsub_endpoint = f"tcp://127.0.0.1:{xsub_port}"
    xpub_endpoint = f"tcp://127.0.0.1:{xpub_port}"
    _patch_settings_for_e2e(monkeypatch, xsub_endpoint, xpub_endpoint)

    base_mock = snapper_settings.get_settings()

    broker = ZmqBrokerThread(xsub_endpoint=xsub_endpoint, xpub_endpoint=xpub_endpoint)
    broker.start()

    executor = PaperOrderExecutor()
    executor._credentials = {"initial_balance": "1000000"}
    executor_task = asyncio.create_task(executor.start())

    trader_0 = TraderCoordinator(
        signal_topics=[_SIGNAL_TOPIC_PREFIX],
        settings=cast(AppSettings, _per_coordinator_settings(base_mock, 0)),
    )
    trader_1 = TraderCoordinator(
        signal_topics=[_SIGNAL_TOPIC_PREFIX],
        settings=cast(AppSettings, _per_coordinator_settings(base_mock, 1)),
    )
    trader_0_task = asyncio.create_task(trader_0.start())
    trader_1_task = asyncio.create_task(trader_1.start())

    client_context = zmq.asyncio.Context()

    await _wait_for_executor_subscription(xsub_endpoint, "BTC-USD")
    await asyncio.sleep(0.3)

    stack = TwoCoordinatorStack(
        broker=broker,
        executor=executor,
        traders=(trader_0, trader_1),
        trader_tasks=(trader_0_task, trader_1_task),
        executor_task=executor_task,
        xsub_endpoint=xsub_endpoint,
        xpub_endpoint=xpub_endpoint,
        client_context=client_context,
        base_mock=base_mock,
    )
    try:
        yield stack
    finally:
        for trader, task in zip(stack.traders, stack.trader_tasks, strict=True):
            await _stop_background_process(trader.stop(), task)
        await _stop_background_process(executor.stop(), executor_task)
        with contextlib.suppress(Exception):
            broker.stop()
        with contextlib.suppress(Exception):
            client_context.term()
