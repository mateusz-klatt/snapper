"""Lifespan helper tests for egress snapshot cache and publisher."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.server.app import _egress_snapshot_interval_seconds
from snapper.server.app import _start_egress_snapshot_cache
from snapper.server.app import _start_egress_snapshot_publisher
from snapper.server.app import _stop_egress_snapshot_cache
from snapper.server.app import _stop_egress_snapshot_publisher


class _SucceedingCache:
    """Fake egress cache whose start records the broker endpoint."""

    def __init__(self, *, own_container: str, stale_after_seconds: float) -> None:
        """Capture constructor arguments."""
        self.own_container = own_container
        self.stale_after_seconds = stale_after_seconds
        self.started_with: str | None = None

    async def start(self, zmq_broker_xpub: str) -> None:
        """Record the endpoint passed by the lifespan helper."""
        self.started_with = zmq_broker_xpub


class _FailingCache:
    """Fake egress cache whose start fails."""

    def __init__(self, *, own_container: str, stale_after_seconds: float) -> None:
        """Accept constructor arguments before failing on start."""
        self.own_container = own_container
        self.stale_after_seconds = stale_after_seconds

    async def start(self, zmq_broker_xpub: str) -> None:
        """Raise to simulate a listener startup failure."""
        raise RuntimeError(zmq_broker_xpub)


class _SucceedingPublisher:
    """Fake egress snapshot publisher that records start calls."""

    def __init__(self, *, container: str, publisher: MagicMock, interval_seconds: float) -> None:
        """Capture constructor arguments."""
        self.container = container
        self.publisher = publisher
        self.interval_seconds = interval_seconds
        self.started = False

    def start(self) -> None:
        """Record that the publisher was started."""
        self.started = True


class _FailingPublisher:
    """Fake egress snapshot publisher that fails during construction."""

    def __init__(self, *, container: str, publisher: MagicMock, interval_seconds: float) -> None:
        """Raise to simulate publisher startup failure."""
        raise RuntimeError(container)


def test_egress_snapshot_interval_seconds_applies_floor() -> None:
    """Spec — heartbeat milliseconds convert to seconds with a positive floor.

    Given heartbeat intervals in milliseconds,
    When the egress snapshot cadence is derived,
    Then it returns seconds without allowing a zero interval.
    """
    assert _egress_snapshot_interval_seconds(2500) == 2.5
    assert _egress_snapshot_interval_seconds(0) == 0.001


class TestEgressSnapshotCacheLifespan:
    """Startup and shutdown behaviour for the API-side cache."""

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — a successful cache start attaches it to app state."""
        monkeypatch.setattr("snapper.server.app.EgressSnapshotCache", _SucceedingCache)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_egress_snapshot_cache(
            app,
            own_container="api@host",
            zmq_broker_xpub="tcp://broker:7501",
            heartbeat_interval_ms=1000,
        )

        assert isinstance(app.state.egress_snapshot_cache, _SucceedingCache)
        assert app.state.egress_snapshot_cache.own_container == "api@host"
        assert app.state.egress_snapshot_cache.stale_after_seconds == 3.0
        assert app.state.egress_snapshot_cache.started_with == "tcp://broker:7501"

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — cache startup failure degrades without attaching state."""
        monkeypatch.setattr("snapper.server.app.EgressSnapshotCache", _FailingCache)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_egress_snapshot_cache(
            app,
            own_container="api@host",
            zmq_broker_xpub="tcp://broker:7501",
            heartbeat_interval_ms=1000,
        )

        assert not hasattr(app.state, "egress_snapshot_cache")

    @pytest.mark.asyncio
    async def test_stop_no_op_when_attribute_absent(self) -> None:
        """Spec — stopping an absent cache returns cleanly."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_egress_snapshot_cache(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_cache(self) -> None:
        """Spec — stopping delegates to the attached cache."""
        stop = AsyncMock()
        app = SimpleNamespace(
            state=SimpleNamespace(egress_snapshot_cache=SimpleNamespace(stop=stop))
        )

        await _stop_egress_snapshot_cache(app)

        stop.assert_awaited_once()


class TestEgressSnapshotPublisherLifespan:
    """Startup and shutdown behaviour for the API-side publisher."""

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — a successful publisher start attaches it to app state."""
        monkeypatch.setattr("snapper.server.app.EgressSnapshotPublisher", _SucceedingPublisher)
        app = SimpleNamespace(state=SimpleNamespace())
        publisher = MagicMock()

        await _start_egress_snapshot_publisher(
            app,
            container="api@host",
            publisher=publisher,
            heartbeat_interval_ms=500,
        )

        assert isinstance(app.state.egress_snapshot_publisher, _SucceedingPublisher)
        assert app.state.egress_snapshot_publisher.container == "api@host"
        assert app.state.egress_snapshot_publisher.publisher is publisher
        assert app.state.egress_snapshot_publisher.interval_seconds == 0.5
        assert app.state.egress_snapshot_publisher.started is True

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — publisher startup failure degrades without attaching state."""
        monkeypatch.setattr("snapper.server.app.EgressSnapshotPublisher", _FailingPublisher)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_egress_snapshot_publisher(
            app,
            container="api@host",
            publisher=MagicMock(),
            heartbeat_interval_ms=500,
        )

        assert not hasattr(app.state, "egress_snapshot_publisher")

    @pytest.mark.asyncio
    async def test_stop_no_op_when_attribute_absent(self) -> None:
        """Spec — stopping an absent publisher returns cleanly."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_egress_snapshot_publisher(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_publisher(self) -> None:
        """Spec — stopping delegates to the attached publisher."""
        stop = AsyncMock()
        app = SimpleNamespace(
            state=SimpleNamespace(egress_snapshot_publisher=SimpleNamespace(stop=stop))
        )

        await _stop_egress_snapshot_publisher(app)

        stop.assert_awaited_once()
