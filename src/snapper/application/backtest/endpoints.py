"""Ephemeral broker allocation for the ZMQ replay engine.

Each backtest replay run gets its own private XPUB/XSUB broker bound to
OS-assigned local ports so multiple runs (or test workers) cannot collide
with each other or with the live broker. Allocation is OS-atomic via
``bind("tcp://127.0.0.1:0")`` — the kernel hands out a free ephemeral port
and ``LAST_ENDPOINT`` reads back the concrete address. There is no
TOCTOU window between probe and bind.

Returns a started ``ZmqBrokerProcess`` (with ``xpub_verbose=True`` so the
strategy + publisher echo-ack handshake works) and a ``ReplayEndpoints``
NamedTuple carrying the resolved ``xsub`` / ``xpub`` URIs.

Defence in depth: refuses to return endpoints that match the live broker's
configured endpoints, even though OS port allocation makes a collision
practically impossible — keeps the failure mode loud rather than allowing
a backtest to accidentally publish onto the live bus.
"""

from typing import NamedTuple

from snapper.config.settings import get_settings
from snapper.messaging.infrastructure.broker import ZmqBrokerProcess


class ReplayEndpoints(NamedTuple):
    """Resolved per-run replay broker endpoints.

    Attributes:
        xsub: tcp address publishers (the ``ReplayPublisher``) connect to.
        xpub: tcp address subscribers (the strategy) connect to.
    """

    xsub: str
    xpub: str


class _LiveBrokerCollisionError(RuntimeError):
    """Raised when ephemeral allocation accidentally hits the live broker.

    Practically impossible because OS-assigned ports never collide with
    bound ports, but kept as a hard guard so a misconfigured environment
    fails loudly instead of silently leaking backtest traffic onto the
    live bus.
    """


async def allocate_replay_endpoints() -> tuple[ZmqBrokerProcess, ReplayEndpoints]:
    """Start a fresh per-run replay broker on OS-assigned local ports.

    Returns:
        A tuple of (started broker, resolved endpoints). Caller owns the
        broker and must ``await broker.stop()`` on cleanup.

    Raises:
        _LiveBrokerCollisionError: If the OS-assigned endpoints somehow match
            the live broker's configured endpoints.
    """
    broker = ZmqBrokerProcess(
        xsub_endpoint="tcp://127.0.0.1:0",
        xpub_endpoint="tcp://127.0.0.1:0",
        xpub_verbose=True,
    )
    await broker.start()
    settings = get_settings()
    if (
        broker.xsub_endpoint == settings.zmq_broker_xsub
        or broker.xpub_endpoint == settings.zmq_broker_xpub
    ):
        await broker.stop()
        raise _LiveBrokerCollisionError(
            f"replay broker collided with live endpoints: "
            f"xsub={broker.xsub_endpoint} live_xsub={settings.zmq_broker_xsub} "
            f"xpub={broker.xpub_endpoint} live_xpub={settings.zmq_broker_xpub}"
        )
    return broker, ReplayEndpoints(xsub=broker.xsub_endpoint, xpub=broker.xpub_endpoint)
