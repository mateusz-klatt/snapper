"""Tests for the cross-coordinator process-summary cache.

Covers topic-authoritative ingestion (store / self-skip-by-topic /
non-summary-topic / omitted-payload-field fallback / parse-failure /
out-of-order rejection), TTL-bounded freshness queries, atomic lookup,
the listener loop against a fake subscriber, transient recv handling, and
start/stop lifecycle including idempotency, the empty-XPUB test-mode skip,
and idempotent teardown.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.server.remote_summary_cache import RemoteSummaryCache

_FIXED_TS = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)


class _FakeClock:
    """Controllable monotonic clock for deterministic TTL assertions."""

    def __init__(self, value: float = 0.0) -> None:
        """Start the clock at ``value``.

        Args:
            value: Initial reading returned by the clock.
        """
        self.value = value

    def __call__(self) -> float:
        """Return the current clock reading.

        Returns:
            The current ``value``.
        """
        return self.value


def _topic(coordinator: str) -> str:
    """Return the summary topic for ``coordinator``.

    Args:
        coordinator: Emitting node slug.

    Returns:
        The ``processes.events.summary.<coordinator>`` topic string.
    """
    return f"processes.events.summary.{coordinator}"


def _make_item(name: str, *, running: bool) -> ProcessSummaryItem:
    """Build a minimal per-process summary row.

    Args:
        name: Process name.
        running: Whether the row reports the process as running.

    Returns:
        A populated :class:`ProcessSummaryItem`.
    """
    return ProcessSummaryItem(
        name=name,
        running=running,
        enabled=True,
        role="core",
        lifecycle="long_running",
    )


def _payload(coordinator: str, items: list[ProcessSummaryItem]) -> bytes:
    """Serialize a summary event to wire bytes.

    Args:
        coordinator: Value for the JSON ``coordinator`` field (which the
            cache deliberately ignores in favour of the topic suffix).
        items: Per-process rows.

    Returns:
        UTF-8 JSON payload bytes.
    """
    event = ProcessSummaryEventData(
        session_id="sess-1",
        sequence_id=1,
        public_id="pub-1",
        timestamp=_FIXED_TS,
        coordinator=coordinator,
        processes=items,
        snapshot_at=_FIXED_TS,
    )
    return event.to_json().encode("utf-8")


class TestIngest:
    """Topic-authoritative ingestion."""

    def test_ingest_stores_remote_snapshot(self) -> None:
        """A remote coordinator's payload is parsed and stored under its topic."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("kfp", running=True)]))
        assert cache.is_running("kfp") is True

    def test_ingest_uses_topic_suffix_over_payload_field(self) -> None:
        """An omitted/default JSON coordinator is overridden by the topic.

        Regression for rolling compatibility: a producer that does not set
        ``coordinator`` (defaulting to ``coord-0``) published on the
        ``coord-1`` topic must still be attributed to ``coord-1`` and NOT
        dropped as the API's own snapshot.
        """
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-1"), _payload("coord-0", [_make_item("feed", running=True)]))
        assert cache.is_running("feed") is True
        assert cache.coordinator_for("feed") == "coord-1"

    def test_ingest_skips_own_coordinator_by_topic(self) -> None:
        """A snapshot on this node's own topic is ignored regardless of payload."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-0"), _payload("coord-1", [_make_item("feed", running=True)]))
        assert cache.is_running("feed") is False

    def test_ingest_skips_non_summary_topic(self) -> None:
        """A topic outside the summary prefix is ignored."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(
            "market.kraken.BTC-USD.ticks", _payload("coord-1", [_make_item("f", running=True)])
        )
        assert cache.is_running("f") is False

    def test_ingest_skips_empty_coordinator_suffix(self) -> None:
        """A bare prefix with no coordinator slug is ignored."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(
            "processes.events.summary.", _payload("coord-1", [_make_item("f", running=True)])
        )
        assert cache.is_running("f") is False

    def test_ingest_logs_and_skips_unparseable_payload(self) -> None:
        """Invalid JSON is swallowed without storing anything."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-1"), b"not-json")
        assert cache.is_running("anything") is False

    def test_ingest_last_received_wins(self) -> None:
        """The most recently received frame replaces the prior one.

        ZMQ delivers a single producer's frames in order, so a producer
        restart's fresh state takes effect immediately with no wall-clock
        ordering guard that a backward clock step could strand.
        """
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("f", running=True)]))
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("f", running=False)]))
        assert cache.is_running("f") is False

    def test_ingest_rejects_malformed_topic_suffix(self) -> None:
        """A multi-segment suffix fails topic validation and is dropped."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(
            "processes.events.summary.coord-1.extra",
            _payload("coord-1", [_make_item("f", running=True)]),
        )
        assert cache.is_running("f") is False

    def test_ingest_malformed_own_suffix_does_not_shadow(self) -> None:
        """A malformed topic resembling the own slug cannot bypass self-skip."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(
            "processes.events.summary.coord-0.extra",
            _payload("coord-1", [_make_item("f", running=True)]),
        )
        assert cache.lookup("f") == (False, None)


class TestFreshnessAndLookup:
    """TTL-bounded ``lookup`` / ``is_running`` / ``coordinator_for`` behaviour."""

    def test_lookup_returns_running_and_owner(self) -> None:
        """A fresh running process resolves to ``(True, owner)`` atomically."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=True)]))
        clock.value = 110.0
        assert cache.lookup("feed") == (True, "coord-1")

    def test_lookup_prefers_running_coordinator(self) -> None:
        """When two coordinators list a process, the running one wins."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=False)]))
        cache._ingest(_topic("coord-2"), _payload("coord-2", [_make_item("feed", running=True)]))
        running, owner = cache.lookup("feed")
        assert running is True
        assert owner == "coord-2"

    def test_lookup_owner_without_running(self) -> None:
        """A fresh non-running process still resolves its owner."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=False)]))
        assert cache.lookup("feed") == (False, "coord-1")

    def test_lookup_non_running_owner_is_freshest(self) -> None:
        """The freshest fresh owner wins regardless of dict iteration order.

        ``coord-1`` is inserted first but received more recently than the
        later-inserted ``coord-2``, exercising both the update and the skip
        paths of the freshest-owner selection.
        """
        clock = _FakeClock(105.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=False)]))
        clock.value = 100.0
        cache._ingest(_topic("coord-2"), _payload("coord-2", [_make_item("feed", running=False)]))
        clock.value = 110.0
        assert cache.lookup("feed") == (False, "coord-1")

    def test_is_running_false_for_stale_snapshot(self) -> None:
        """A snapshot older than the TTL is treated as stopped."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=True)]))
        clock.value = 120.0
        assert cache.is_running("feed") is False

    def test_is_running_false_for_unknown_process(self) -> None:
        """A process absent from the snapshot reports not running."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=True)]))
        assert cache.is_running("other") is False

    def test_lookup_none_when_empty(self) -> None:
        """An empty cache resolves to ``(False, None)``."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        assert cache.lookup("feed") == (False, None)

    def test_coordinator_for_none_when_stale(self) -> None:
        """A stale snapshot yields no owner."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=True)]))
        clock.value = 200.0
        assert cache.coordinator_for("feed") is None

    def test_has_fresh_snapshot_false_when_empty(self) -> None:
        """An empty cache has no fresh snapshot (broker liveness unknown)."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        assert cache.has_fresh_snapshot() is False

    def test_has_fresh_snapshot_true_with_fresh_coordinator(self) -> None:
        """Any within-TTL snapshot proves the broker is forwarding.

        The snapshot's process is deliberately ``running=False``: a fresh
        frame from any coordinator transits the broker regardless of what
        it reports, so its mere freshness is the broker-liveness signal.
        """
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=False)]))
        clock.value = 110.0
        assert cache.has_fresh_snapshot() is True

    def test_has_fresh_snapshot_false_when_all_stale(self) -> None:
        """Every coordinator stale past the TTL yields no fresh snapshot."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _payload("coord-1", [_make_item("feed", running=True)]))
        cache._ingest(_topic("coord-2"), _payload("coord-2", [_make_item("strat", running=True)]))
        clock.value = 200.0
        assert cache.has_fresh_snapshot() is False


class TestListenLoop:
    """Listener loop against a fake subscriber."""

    @pytest.mark.asyncio
    async def test_listen_loop_returns_when_no_subscriber(self) -> None:
        """A loop with no subscriber returns immediately."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        cache._running = True
        await cache._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_ingests_frame_then_exits(self) -> None:
        """One frame is folded into the cache; loop exits on ``running=False``."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        payload = _payload("coord-1", [_make_item("feed", running=True)])

        async def _recv_then_stop() -> tuple[str, bytes]:
            cache._running = False
            return (_topic("coord-1"), payload)

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        cache._subscriber = subscriber
        cache._running = True

        await cache._listen_loop()
        assert cache.is_running("feed") is True

    @pytest.mark.asyncio
    async def test_listen_loop_propagates_cancellation(self) -> None:
        """Cancellation during receive is logged and re-raised for reaping."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        cache._subscriber = subscriber
        cache._running = True
        with pytest.raises(asyncio.CancelledError):
            await cache._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_continues_when_recv_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transient recv error returns None; loop continues until stopped."""
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))

        async def _fail_then_stop() -> tuple[str, bytes]:
            cache._running = False
            raise RuntimeError("boom")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_fail_then_stop)
        cache._subscriber = subscriber
        cache._running = True

        await cache._listen_loop()
        assert cache.is_running("feed") is False


class TestRecvOneFrame:
    """Single-frame receive: success, transient failure, cancellation."""

    @pytest.mark.asyncio
    async def test_recv_one_frame_returns_frame(self) -> None:
        """A successful receive returns the decoded frame."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=("topic", b"payload"))
        result = await cache._recv_one_frame(subscriber)
        assert result == ("topic", b"payload")

    @pytest.mark.asyncio
    async def test_recv_one_frame_returns_none_on_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transient error is logged and yields ``None`` after backoff."""
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv fail"))
        result = await cache._recv_one_frame(subscriber)
        assert result is None

    @pytest.mark.asyncio
    async def test_recv_one_frame_propagates_cancellation(self) -> None:
        """Cancellation propagates so shutdown can reap the task."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await cache._recv_one_frame(subscriber)


class TestStartStop:
    """Start/stop lifecycle including idempotency + the empty-XPUB skip."""

    @pytest.mark.asyncio
    async def test_start_empty_xpub_skips_listener(self) -> None:
        """An empty broker XPUB skips socket setup entirely."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        await cache.start("")
        assert cache._listen_task is None
        assert cache._subscriber is None

    @pytest.mark.asyncio
    async def test_start_then_stop_real_socket(self) -> None:
        """A real socket spawns the listener; stop reaps every resource."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        await cache.start("tcp://127.0.0.1:5599")
        assert cache._listen_task is not None
        assert cache._subscriber is not None
        await cache.stop()
        assert cache._listen_task is None
        assert cache._subscriber is None
        assert cache._zmq_context is None

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self) -> None:
        """A second start while the listener is alive is a no-op (no leak)."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        await cache.start("tcp://127.0.0.1:5599")
        first_task = cache._listen_task
        await cache.start("tcp://127.0.0.1:5599")
        assert cache._listen_task is first_task
        await cache.stop()

    @pytest.mark.asyncio
    async def test_start_reaps_dead_listener_before_restart(self) -> None:
        """A restart after the listener died reaps the stale task and rebuilds."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        await cache.start("tcp://127.0.0.1:5599")
        first_task = cache._listen_task
        assert first_task is not None
        cache._running = False
        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task
        await cache.start("tcp://127.0.0.1:5599")
        assert cache._listen_task is not None
        assert cache._listen_task is not first_task
        await cache.stop()

    @pytest.mark.asyncio
    async def test_start_cleans_up_on_socket_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failure during socket setup reaps partial resources and re-raises."""

        def _boom() -> None:
            raise RuntimeError("context creation failed")

        monkeypatch.setattr("snapper.server.remote_summary_cache.zmq.asyncio.Context", _boom)
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        with pytest.raises(RuntimeError):
            await cache.start("tcp://127.0.0.1:5599")
        assert cache._subscriber is None
        assert cache._zmq_context is None
        assert cache._listen_task is None

    @pytest.mark.asyncio
    async def test_stop_idempotent_when_not_started(self) -> None:
        """Stopping an unstarted cache is a no-op."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(0.0))
        await cache.stop()
        assert cache._listen_task is None


def _labelled_payload(coordinator: str, label: str | None) -> bytes:
    """Build a summary payload carrying a coordinator label.

    Args:
        coordinator: Topic-field coordinator (ignored by the cache).
        label: The ``coordinator_label`` to stamp on the event.

    Returns:
        UTF-8 JSON payload bytes.
    """
    event = ProcessSummaryEventData(
        session_id="sess-1",
        sequence_id=1,
        public_id="pub-1",
        timestamp=_FIXED_TS,
        coordinator=coordinator,
        coordinator_label=label,
        processes=[_make_item("kfp", running=True)],
        snapshot_at=_FIXED_TS,
    )
    return event.to_json().encode("utf-8")


class TestLabelFor:
    """Remote coordinator label resolution for the managed-remotely UI rows."""

    def test_label_for_returns_fresh_coordinator_label(self) -> None:
        """A fresh coordinator's emitted label is returned for its slug."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        cache._ingest(_topic("coord-1"), _labelled_payload("coord-1", "Feed"))
        assert cache.label_for("coord-1") == "Feed"

    def test_label_for_unknown_coordinator_is_none(self) -> None:
        """A coordinator with no snapshot has no label."""
        cache = RemoteSummaryCache(own_coordinator="coord-0", clock=_FakeClock(100.0))
        assert cache.label_for("coord-9") is None

    def test_label_for_stale_snapshot_is_none(self) -> None:
        """A coordinator whose snapshot has aged past the TTL yields no label."""
        clock = _FakeClock(100.0)
        cache = RemoteSummaryCache(own_coordinator="coord-0", ttl_seconds=15.0, clock=clock)
        cache._ingest(_topic("coord-1"), _labelled_payload("coord-1", "Feed"))
        clock.value = 200.0
        assert cache.label_for("coord-1") is None
