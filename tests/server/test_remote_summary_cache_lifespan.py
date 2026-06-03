"""Lifespan helper tests for ``_start_remote_summary_cache`` / ``_stop_remote_summary_cache``.

Mirrors the snapshotter-helper convention: a successful start attaches the
consumer to ``app.state``; a failing start is swallowed and leaves no
attribute (the handlers degrade to the local-only view); stop is a no-op
when nothing is attached and otherwise delegates to ``cache.stop``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from snapper.server.app import _start_remote_summary_cache
from snapper.server.app import _stop_remote_summary_cache


class _SucceedingCache:
    """Fake cache whose ``start`` records the broker endpoint."""

    def __init__(self, own_coordinator: str) -> None:
        """Capture the owning coordinator slug.

        Args:
            own_coordinator: This node's coordinator slug.
        """
        self.own_coordinator = own_coordinator
        self.started_with: str | None = None

    async def start(self, zmq_broker_xpub: str) -> None:
        """Record the endpoint the consumer was started against.

        Args:
            zmq_broker_xpub: Broker XPUB endpoint.
        """
        self.started_with = zmq_broker_xpub


class _FailingCache:
    """Fake cache whose ``start`` raises to exercise graceful degradation."""

    def __init__(self, own_coordinator: str) -> None:
        """Accept the owning coordinator slug.

        Args:
            own_coordinator: This node's coordinator slug.
        """
        self._own_coordinator = own_coordinator

    async def start(self, zmq_broker_xpub: str) -> None:
        """Raise to simulate a startup failure.

        Args:
            zmq_broker_xpub: Broker XPUB endpoint.

        Raises:
            RuntimeError: Always, to simulate a failed start.
        """
        raise RuntimeError("boom")


class TestStartRemoteSummaryCache:
    """Startup behaviour of ``_start_remote_summary_cache``."""

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success the consumer is attached to ``app.state``."""
        monkeypatch.setattr("snapper.server.app.RemoteSummaryCache", _SucceedingCache)
        app = SimpleNamespace(state=SimpleNamespace())
        await _start_remote_summary_cache(
            app, own_coordinator="coord-0", zmq_broker_xpub="tcp://broker:7501"
        )
        assert isinstance(app.state.remote_summary_cache, _SucceedingCache)
        assert app.state.remote_summary_cache.started_with == "tcp://broker:7501"
        assert app.state.remote_summary_cache.own_coordinator == "coord-0"

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A startup exception is swallowed; ``app.state`` keeps no attribute."""
        monkeypatch.setattr("snapper.server.app.RemoteSummaryCache", _FailingCache)
        app = SimpleNamespace(state=SimpleNamespace())
        await _start_remote_summary_cache(
            app, own_coordinator="coord-0", zmq_broker_xpub="tcp://broker:7501"
        )
        assert not hasattr(app.state, "remote_summary_cache")


class TestStopRemoteSummaryCache:
    """Shutdown behaviour of ``_stop_remote_summary_cache``."""

    @pytest.mark.asyncio
    async def test_stop_no_op_when_attribute_absent(self) -> None:
        """Returns cleanly when no consumer is attached."""
        app = SimpleNamespace(state=SimpleNamespace())
        await _stop_remote_summary_cache(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_cache(self) -> None:
        """Delegates to the attached consumer's ``stop``."""
        stopped = AsyncMock()
        cache = SimpleNamespace(stop=stopped)
        app = SimpleNamespace(state=SimpleNamespace(remote_summary_cache=cache))
        await _stop_remote_summary_cache(app)
        stopped.assert_awaited_once()
