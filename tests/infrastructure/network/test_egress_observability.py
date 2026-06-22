"""Tests for cross-process egress snapshot publishing."""

import asyncio
import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_observability import EGRESS_SNAPSHOT_TOPIC
from snapper.infrastructure.network.egress_observability import EgressSnapshotPublisher
from snapper.infrastructure.network.egress_observability import resolve_egress_container_id
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker


@pytest.fixture(autouse=True)
def _reset_pool() -> None:
    """Reset the process-local egress pool around every publisher test."""
    reset_egress_pool()
    yield
    reset_egress_pool()


def _message_publisher() -> tuple[MessagePublisher, MagicMock]:
    """Build a ``MessagePublisher`` around a mocked validated publisher."""
    raw = MagicMock()
    raw.send_multipart = AsyncMock()
    raw.close = MagicMock()
    raw.setsockopt = MagicMock()
    return MessagePublisher(raw, SequenceTracker()), raw


def _configure_pool() -> None:
    """Install a direct-only egress pool for snapshot publisher tests."""
    configure_egress_pool(
        EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(
                    id="default",
                    kind="direct",
                    priority=100,
                )
            ],
        )
    )


def test_resolve_egress_container_id_uses_role_and_hostname() -> None:
    """Spec — source ids are stable human-readable role and host labels.

    Given a process role and hostname,
    When the egress source id is resolved,
    Then the id combines both labels with unknown fallbacks.
    """
    assert resolve_egress_container_id("pub:kraken", "feed-host") == "pub:kraken@feed-host"
    assert resolve_egress_container_id(" ", " ") == "unknown@unknown"


@pytest.mark.asyncio
async def test_publish_once_returns_false_without_pool() -> None:
    """Spec — disabled processes do not publish egress snapshots.

    Given no process-local egress pool,
    When one egress snapshot tick runs,
    Then no ZMQ frame is sent.
    """
    publisher, raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="pub:kraken@feed",
        publisher=publisher,
        interval_seconds=1.0,
    )

    sent = await snapshot_publisher.publish_once()

    assert sent is False
    raw.send_multipart.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_once_emits_snapshot_with_container() -> None:
    """Spec — a pool-bearing process publishes its snapshot on the system topic.

    Given a configured egress pool and a message publisher,
    When one egress snapshot tick runs,
    Then the ZMQ payload includes the source container and route snapshot.
    """
    _configure_pool()
    publisher, raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="pub:kraken@feed",
        publisher=publisher,
        interval_seconds=1.0,
    )

    sent = await snapshot_publisher.publish_once()

    assert sent is True
    raw.send_multipart.assert_awaited_once()
    call_args = raw.send_multipart.await_args
    assert call_args.args[0] == EGRESS_SNAPSHOT_TOPIC
    payload: bytes = call_args.args[1]
    decoded = json.loads(payload)
    assert decoded["topic"] == EGRESS_SNAPSHOT_TOPIC
    assert decoded["type"] == "egress_pool_snapshot_event"
    assert decoded["container"] == "pub:kraken@feed"
    assert decoded["snapshot"]["enabled"] is True
    assert decoded["snapshot"]["routes"][0]["id"] == "default"


@pytest.mark.asyncio
async def test_start_is_idempotent_and_stop_cancels_task() -> None:
    """Spec — the background loop starts once and stop reaps it cleanly.

    Given an egress snapshot publisher,
    When start is called twice and stop is called twice,
    Then only one task is created and it is cancelled.
    """
    publisher, _raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="api@host",
        publisher=publisher,
        interval_seconds=10.0,
    )

    snapshot_publisher.start()
    first_task = snapshot_publisher._task
    snapshot_publisher.start()
    second_task = snapshot_publisher._task
    await snapshot_publisher.stop()
    await snapshot_publisher.stop()

    assert snapshot_publisher.container == "api@host"
    assert first_task is second_task
    assert first_task is not None
    assert first_task.cancelled()


@pytest.mark.asyncio
async def test_run_loop_logs_publish_errors_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — publish failures do not kill the periodic egress loop.

    Given the periodic publisher loop is running,
    When a publish tick raises an exception,
    Then the loop handles the error without leaking it.
    """
    publisher, _raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="api@host",
        publisher=publisher,
        interval_seconds=-1.0,
    )
    calls: list[str] = []

    async def _publish_once() -> bool:
        calls.append("called")
        snapshot_publisher._running = False
        raise RuntimeError("boom")

    snapshot_publisher._running = True

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(snapshot_publisher, "publish_once", _publish_once)
    monkeypatch.setattr(asyncio, "sleep", _sleep)

    await snapshot_publisher._run_loop()

    assert calls == ["called"]


@pytest.mark.asyncio
async def test_run_loop_stops_after_sleep_without_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec — stopping during cadence sleep skips the next publish.

    Given the periodic publisher loop is running,
    When the loop is stopped while it awaits the cadence sleep,
    Then the tick exits before calling publish_once.
    """
    publisher, _raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="api@host",
        publisher=publisher,
        interval_seconds=1.0,
    )
    calls: list[str] = []
    snapshot_publisher._running = True

    async def _sleep(_seconds: float) -> None:
        snapshot_publisher._running = False

    async def _publish_once() -> bool:
        calls.append("called")
        return True

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    monkeypatch.setattr(snapshot_publisher, "publish_once", _publish_once)

    await snapshot_publisher._run_loop()

    assert calls == []


@pytest.mark.asyncio
async def test_run_loop_propagates_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec — cancellation escapes the egress loop for shutdown reaping.

    Given the periodic publisher loop is running,
    When asyncio cancellation interrupts its sleep,
    Then the cancellation propagates to the awaiting stopper.
    """
    publisher, _raw = _message_publisher()
    snapshot_publisher = EgressSnapshotPublisher(
        container="api@host",
        publisher=publisher,
        interval_seconds=1.0,
    )
    snapshot_publisher._running = True

    async def _cancel_sleep(_seconds: float) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "sleep", _cancel_sleep)

    with pytest.raises(asyncio.CancelledError):
        await snapshot_publisher._run_loop()
