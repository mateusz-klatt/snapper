"""Tests for API-side cross-process egress snapshot cache."""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import zmq

from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressRouteStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressTransferInterfaceSnapshot
from snapper.infrastructure.network.egress_observability import EGRESS_SNAPSHOT_TOPIC
from snapper.infrastructure.network.egress_transfer_observability import EGRESS_TRANSFER_TOPIC
from snapper.messaging.schemas.data import EgressPoolSnapshotEventData
from snapper.messaging.schemas.data import EgressTransferEventData
from snapper.server import egress_snapshot_cache as cache_module
from snapper.server.egress_snapshot_cache import EgressSnapshotCache


class _FakeClock:
    """Mutable monotonic clock for deterministic cache tests."""

    def __init__(self, value: float) -> None:
        """Store the initial monotonic value."""
        self.value = value

    def __call__(self) -> float:
        """Return the current fake monotonic value."""
        return self.value


class _FakeSocket:
    """Minimal ZMQ SUB socket stand-in for start/stop tests."""

    def __init__(self) -> None:
        """Initialize socket call recording fields."""
        self.connected_to: str | None = None
        self.options: list[tuple[int, int]] = []
        self.subscriptions: list[tuple[int, str]] = []
        self.closed = False

    def setsockopt(self, option: int, value: int) -> None:
        """Record an integer socket option."""
        self.options.append((option, value))

    def setsockopt_string(self, option: int, value: str) -> None:
        """Record a string socket option."""
        self.subscriptions.append((option, value))

    def connect(self, endpoint: str) -> None:
        """Record the endpoint the socket connected to."""
        self.connected_to = endpoint

    def close(self) -> None:
        """Record that the socket was closed."""
        self.closed = True


class _FakeContext:
    """Minimal ``zmq.asyncio.Context`` stand-in."""

    def __init__(self, socket: _FakeSocket) -> None:
        """Store the socket returned from ``socket``."""
        self.socket_instance = socket
        self.terminated = False

    def socket(self, _kind: int) -> _FakeSocket:
        """Return the fake socket."""
        return self.socket_instance

    def term(self) -> None:
        """Record context termination."""
        self.terminated = True


def _snapshot(route_id: str = "default", in_use_count: int = 0) -> EgressPoolStatusSnapshot:
    """Build a minimal egress pool status snapshot."""
    return EgressPoolStatusSnapshot(
        enabled=True,
        on_all_quarantined="wait",
        private_fallback_route_id=None,
        private_on_fallback=False,
        routes=[
            EgressRouteStatusSnapshot(
                id=route_id,
                kind="direct",
                priority=100,
                enabled=True,
                quarantined=False,
                quarantine_seconds_remaining=None,
                in_use_count=in_use_count,
            )
        ],
    )


def _payload(container: str, snapshot: EgressPoolStatusSnapshot) -> bytes:
    """Serialize one egress snapshot event."""
    event = EgressPoolSnapshotEventData(
        session_id="session-1",
        sequence_id=1,
        public_id="event-1",
        timestamp=datetime(2026, 6, 22, tzinfo=UTC),
        container=container,
        snapshot=snapshot,
    )
    return event.publish_to(EGRESS_SNAPSHOT_TOPIC)


def _transfer_payload(
    *,
    interface: str = "wg-pl",
    socks5_listen_port: int = 1084,
    rx_bytes: int = 100,
    tx_bytes: int = 200,
) -> bytes:
    """Serialize one egress transfer event."""
    event = EgressTransferEventData(
        session_id="session-1",
        sequence_id=1,
        public_id="event-1",
        timestamp=datetime(2026, 6, 22, tzinfo=UTC),
        interfaces=[
            EgressTransferInterfaceSnapshot(
                interface=interface,
                socks5_listen_port=socks5_listen_port,
                rx_bytes=rx_bytes,
                tx_bytes=tx_bytes,
                rx_rate_bytes_per_second=10.0,
                tx_rate_bytes_per_second=20.0,
                latest_handshake_at=datetime(2026, 6, 22, 9, 59, tzinfo=UTC),
                counter_reset=False,
                sampled_at=datetime(2026, 6, 22, 10, 0, tzinfo=UTC),
            )
        ],
    )
    return event.publish_to(EGRESS_TRANSFER_TOPIC)


class TestEgressSnapshotCacheIngest:
    """Payload ingestion and cache read behaviour."""

    def test_ingest_caches_per_container_and_updates_existing(self) -> None:
        """Spec — a newer message from the same container replaces the old one."""
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("feed", _snapshot("old", 1)))
        clock.value = 101.0
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("feed", _snapshot("new", 2)))

        entries = cache.latest_snapshots()

        assert len(entries) == 1
        assert entries[0].container == "feed"
        assert entries[0].snapshot.routes[0].id == "new"
        assert entries[0].snapshot.routes[0].in_use_count == 2
        assert entries[0].age_seconds == 0.0
        assert entries[0].stale is False

    def test_latest_snapshots_sort_and_flag_stale(self) -> None:
        """Spec — cached snapshots are sorted and stale based on receive age."""
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("z-feed", _snapshot()))
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("a-feed", _snapshot()))
        clock.value = 104.0

        entries = cache.latest_snapshots()

        assert [entry.container for entry in entries] == ["a-feed", "z-feed"]
        assert [entry.age_seconds for entry in entries] == [4.0, 4.0]
        assert [entry.stale for entry in entries] == [True, True]

    def test_ignores_wrong_topic_invalid_topic_self_and_bad_payload(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — malformed or local frames do not enter the cache."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        cache._ingest("system.settings", _payload("feed", _snapshot()))
        monkeypatch.setattr(cache_module, "validate_topic", lambda _topic: (False, "bad"))
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("feed", _snapshot()))
        monkeypatch.setattr(cache_module, "validate_topic", lambda _topic: (True, ""))
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, b"{not json")
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _payload("api", _snapshot()))

        assert cache.latest_snapshots() == []

    def test_ingest_caches_transfer_per_interface_and_updates_existing(self) -> None:
        """Spec — newer sidecar transfer samples replace old interface rows."""
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(
            EGRESS_TRANSFER_TOPIC,
            _transfer_payload(interface="wg-pl", rx_bytes=100, tx_bytes=200),
        )
        clock.value = 101.0
        cache._ingest(
            EGRESS_TRANSFER_TOPIC,
            _transfer_payload(interface="wg-pl", rx_bytes=150, tx_bytes=260),
        )

        entries = cache.latest_transfers()

        assert len(entries) == 1
        assert entries[0].interface == "wg-pl"
        assert entries[0].snapshot.rx_bytes == 150
        assert entries[0].snapshot.tx_bytes == 260
        assert entries[0].age_seconds == 0.0
        assert entries[0].stale is False

    def test_latest_transfers_sort_and_flag_stale(self) -> None:
        """Spec — transfer samples are sorted and stale by receive age."""
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(EGRESS_TRANSFER_TOPIC, _transfer_payload(interface="wg-z"))
        cache._ingest(EGRESS_TRANSFER_TOPIC, _transfer_payload(interface="wg-a"))
        clock.value = 104.0

        entries = cache.latest_transfers()

        assert [entry.interface for entry in entries] == ["wg-a", "wg-z"]
        assert [entry.age_seconds for entry in entries] == [4.0, 4.0]
        assert [entry.stale for entry in entries] == [True, True]

    def test_ingest_ignores_bad_transfer_payload(self) -> None:
        """Spec — malformed transfer payloads do not enter the transfer cache."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )

        cache._ingest(EGRESS_TRANSFER_TOPIC, b"{not json")

        assert cache.latest_transfers() == []


class TestEgressSnapshotCacheListener:
    """Listener loop and receive helper behaviour."""

    @pytest.mark.asyncio
    async def test_listen_loop_returns_when_no_subscriber(self) -> None:
        """Spec — a listener without a subscriber exits immediately."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        cache._running = True

        await cache._listen_loop()

        assert cache.latest_snapshots() == []

    @pytest.mark.asyncio
    async def test_listen_loop_ingests_frame_then_exits(self) -> None:
        """Spec — one received frame is folded into the cache."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )

        async def _recv_then_stop() -> tuple[str, bytes]:
            cache._running = False
            return EGRESS_SNAPSHOT_TOPIC, _payload("feed", _snapshot())

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        cache._subscriber = subscriber
        cache._running = True

        await cache._listen_loop()

        assert cache.latest_snapshots()[0].container == "feed"

    @pytest.mark.asyncio
    async def test_listen_loop_propagates_cancellation(self) -> None:
        """Spec — cancellation during receive escapes the listener loop."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        cache._subscriber = subscriber
        cache._running = True

        with pytest.raises(asyncio.CancelledError):
            await cache._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_continues_after_transient_recv_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — transient receive errors are swallowed with backoff."""

        async def _sleep(_seconds: float) -> None:
            return None

        async def _fail_then_stop() -> tuple[str, bytes]:
            cache._running = False
            raise RuntimeError("boom")

        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_fail_then_stop)
        cache._subscriber = subscriber
        cache._running = True
        monkeypatch.setattr(asyncio, "sleep", _sleep)

        await cache._listen_loop()

        assert cache.latest_snapshots() == []

    @pytest.mark.asyncio
    async def test_recv_one_frame_returns_frame(self) -> None:
        """Spec — a successful receive returns the decoded frame."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=(EGRESS_SNAPSHOT_TOPIC, b"{}"))

        frame = await cache._recv_one_frame(subscriber)

        assert frame == (EGRESS_SNAPSHOT_TOPIC, b"{}")

    @pytest.mark.asyncio
    async def test_recv_one_frame_propagates_cancellation(self) -> None:
        """Spec — receive cancellation is not swallowed."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await cache._recv_one_frame(subscriber)


class TestEgressSnapshotCacheStartStop:
    """Socket lifecycle behaviour for the cache."""

    @pytest.mark.asyncio
    async def test_start_empty_endpoint_skips_listener(self) -> None:
        """Spec — empty broker endpoints degrade to an empty cache."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )

        await cache.start("")

        assert cache._subscriber is None
        assert cache.stale_after_seconds == 3.0

    @pytest.mark.asyncio
    async def test_start_subscribes_and_stop_reaps_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — start wires the SUB socket and stop closes all resources."""
        fake_socket = _FakeSocket()
        fake_context = _FakeContext(fake_socket)
        monkeypatch.setattr(cache_module.zmq.asyncio, "Context", lambda: fake_context)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )

        await cache.start("tcp://broker:7501")
        first_task = cache._listen_task
        await cache.start("tcp://broker:7501")
        await cache.stop()

        assert first_task is not None
        assert fake_socket.connected_to == "tcp://broker:7501"
        assert (zmq.RCVHWM, 10000) in fake_socket.options
        assert (zmq.SUBSCRIBE, EGRESS_SNAPSHOT_TOPIC) in fake_socket.subscriptions
        assert (zmq.SUBSCRIBE, EGRESS_TRANSFER_TOPIC) in fake_socket.subscriptions
        assert fake_socket.closed is True
        assert fake_context.terminated is True

    @pytest.mark.asyncio
    async def test_start_reaps_done_task_before_restart(self) -> None:
        """Spec — a completed listener task is reaped before a no-op restart."""
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )
        cache._listen_task = asyncio.create_task(asyncio.sleep(0))
        await cache._listen_task

        await cache.start("")

        assert cache._listen_task is None

    @pytest.mark.asyncio
    async def test_start_failure_reaps_partial_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — startup exceptions clean up partial ZMQ state."""

        class _FailingContext:
            """Context whose socket creation fails."""

            def term(self) -> None:
                """Terminate without raising."""
                return None

            def socket(self, _kind: int) -> _FakeSocket:
                """Raise to simulate a ZMQ setup failure."""
                raise RuntimeError("boom")

        monkeypatch.setattr(cache_module.zmq.asyncio, "Context", lambda: _FailingContext())
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=_FakeClock(100.0),
        )

        with pytest.raises(RuntimeError, match="boom"):
            await cache.start("tcp://broker:7501")

        assert cache._subscriber is None
        assert cache._zmq_context is None
