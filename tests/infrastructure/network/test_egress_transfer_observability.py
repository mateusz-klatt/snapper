"""Tests for sidecar WireGuard transfer ZMQ publishing."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.infrastructure.network.egress_models import EgressTransferInterfaceSnapshot
from snapper.infrastructure.network.egress_transfer_observability import EGRESS_TRANSFER_TOPIC
from snapper.infrastructure.network.egress_transfer_observability import EgressTransferPublisher
from snapper.infrastructure.network.egress_transfer_stats import EgressTransferSampler
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker


class _FakeSampler(EgressTransferSampler):
    """Sampler stand-in returning preconfigured transfer rows."""

    def __init__(self, rows: list[EgressTransferInterfaceSnapshot]) -> None:
        """Store transfer rows returned by ``sample``."""
        self.rows = rows

    def sample(self) -> list[EgressTransferInterfaceSnapshot]:
        """Return configured transfer rows."""
        return self.rows


def _message_publisher() -> tuple[MessagePublisher, MagicMock]:
    """Build a ``MessagePublisher`` around a mocked validated publisher."""
    raw = MagicMock()
    raw.send_multipart = AsyncMock()
    raw.close = MagicMock()
    raw.setsockopt = MagicMock()
    return MessagePublisher(raw, SequenceTracker()), raw


def _transfer_row() -> EgressTransferInterfaceSnapshot:
    """Build one sidecar transfer sample."""
    return EgressTransferInterfaceSnapshot(
        interface="wg-pl",
        socks5_listen_port=1084,
        rx_bytes=100,
        tx_bytes=200,
        rx_rate_bytes_per_second=10.0,
        tx_rate_bytes_per_second=20.0,
        latest_handshake_at=datetime(2026, 6, 22, 9, 59, tzinfo=UTC),
        counter_reset=False,
        sampled_at=datetime(2026, 6, 22, 10, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_publish_once_returns_false_without_tunnel_rows() -> None:
    """Spec — no sampled tunnels produce no ZMQ frame.

    Given a transfer publisher with an empty sampler,
    When one publish tick runs,
    Then no event is sent.
    """
    publisher, raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([]),
        publisher=publisher,
        interval_seconds=1.0,
    )

    sent = await transfer_publisher.publish_once()

    assert sent is False
    raw.send_multipart.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_once_emits_transfer_event_with_port() -> None:
    """Spec — sampled tunnel rows publish on the system transfer topic.

    Given a sampler with one interface transfer row,
    When one publish tick runs,
    Then the ZMQ event includes the interface, SOCKS5 port, bytes, and rates.
    """
    publisher, raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([_transfer_row()]),
        publisher=publisher,
        interval_seconds=1.0,
    )

    sent = await transfer_publisher.publish_once()

    assert sent is True
    raw.send_multipart.assert_awaited_once()
    call_args = raw.send_multipart.await_args
    assert call_args.args[0] == EGRESS_TRANSFER_TOPIC
    payload: bytes = call_args.args[1]
    decoded = json.loads(payload)
    assert decoded["topic"] == EGRESS_TRANSFER_TOPIC
    assert decoded["type"] == "egress_transfer_event"
    assert decoded["interfaces"] == [
        {
            "interface": "wg-pl",
            "socks5_listen_port": 1084,
            "rx_bytes": 100,
            "tx_bytes": 200,
            "rx_rate_bytes_per_second": 10.0,
            "tx_rate_bytes_per_second": 20.0,
            "latest_handshake_at": "2026-06-22T09:59:00Z",
            "counter_reset": False,
            "sampled_at": "2026-06-22T10:00:00Z",
        }
    ]


@pytest.mark.asyncio
async def test_start_is_idempotent_and_stop_cancels_task() -> None:
    """Spec — the background transfer loop starts once and stops cleanly.

    Given a transfer publisher,
    When start is called twice and stop is called twice,
    Then only one task is created and it is cancelled.
    """
    publisher, _raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([_transfer_row()]),
        publisher=publisher,
        interval_seconds=10.0,
    )

    transfer_publisher.start()
    first_task = transfer_publisher._task
    transfer_publisher.start()
    second_task = transfer_publisher._task
    await transfer_publisher.stop()
    await transfer_publisher.stop()

    assert first_task is second_task
    assert first_task is not None
    assert first_task.cancelled()


@pytest.mark.asyncio
async def test_run_loop_logs_publish_errors_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — publish errors are isolated to one transfer tick.

    Given the periodic transfer publisher loop is running,
    When a publish tick raises an exception,
    Then the loop handles the error without leaking it.
    """
    publisher, _raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([_transfer_row()]),
        publisher=publisher,
        interval_seconds=-1.0,
    )
    calls: list[str] = []

    async def _publish_once() -> bool:
        calls.append("called")
        transfer_publisher._running = False
        raise RuntimeError("boom")

    async def _sleep(_seconds: float) -> None:
        return None

    transfer_publisher._running = True
    monkeypatch.setattr(transfer_publisher, "publish_once", _publish_once)
    monkeypatch.setattr(asyncio, "sleep", _sleep)

    await transfer_publisher._run_loop()

    assert calls == ["called"]


@pytest.mark.asyncio
async def test_run_loop_stops_after_sleep_without_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — stopping during cadence sleep skips the next transfer publish.

    Given the periodic transfer publisher loop is running,
    When the loop is stopped while it awaits the cadence sleep,
    Then the tick exits before calling publish_once.
    """
    publisher, _raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([_transfer_row()]),
        publisher=publisher,
        interval_seconds=1.0,
    )
    calls: list[str] = []

    async def _sleep(_seconds: float) -> None:
        transfer_publisher._running = False

    async def _publish_once() -> bool:
        calls.append("called")
        return True

    transfer_publisher._running = True
    monkeypatch.setattr(asyncio, "sleep", _sleep)
    monkeypatch.setattr(transfer_publisher, "publish_once", _publish_once)

    await transfer_publisher._run_loop()

    assert calls == []


@pytest.mark.asyncio
async def test_run_loop_propagates_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec — cancellation escapes the transfer loop for shutdown reaping.

    Given the periodic transfer publisher loop is running,
    When asyncio cancellation interrupts its sleep,
    Then the cancellation propagates to the awaiting stopper.
    """
    publisher, _raw = _message_publisher()
    transfer_publisher = EgressTransferPublisher(
        sampler=_FakeSampler([_transfer_row()]),
        publisher=publisher,
        interval_seconds=1.0,
    )

    async def _cancel_sleep(_seconds: float) -> None:
        raise asyncio.CancelledError()

    transfer_publisher._running = True
    monkeypatch.setattr(asyncio, "sleep", _cancel_sleep)

    with pytest.raises(asyncio.CancelledError):
        await transfer_publisher._run_loop()
