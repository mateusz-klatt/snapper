"""Lifespan helper tests for ``_start_command_ack_registry`` / ``_stop_command_ack_registry``.

Mirrors the remote-summary-cache lifespan convention: a successful start attaches
the registry to ``app.state``; a failing start is swallowed and leaves no attribute
(the PATCH degrades to reconcile-pending); stop is a no-op when nothing is attached
and otherwise delegates to ``registry.stop``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from snapper.server.app import _start_command_ack_registry
from snapper.server.app import _stop_command_ack_registry


class _SucceedingRegistry:
    """Fake registry whose ``start`` records the broker endpoint."""

    def __init__(self, signing_key: bytes) -> None:
        """Capture the signing key.

        Args:
            signing_key: The control-plane HMAC key.
        """
        self.signing_key = signing_key
        self.started_with: str | None = None

    async def start(self, zmq_broker_xpub: str) -> None:
        """Record the endpoint the registry was started against.

        Args:
            zmq_broker_xpub: Broker XPUB endpoint.
        """
        self.started_with = zmq_broker_xpub


class _FailingRegistry:
    """Fake registry whose ``start`` raises to exercise graceful degradation."""

    def __init__(self, signing_key: bytes) -> None:
        """Accept the signing key.

        Args:
            signing_key: The control-plane HMAC key.
        """
        self._signing_key = signing_key

    async def start(self, zmq_broker_xpub: str) -> None:
        """Raise to simulate a startup failure.

        Args:
            zmq_broker_xpub: Broker XPUB endpoint.

        Raises:
            RuntimeError: Always, to simulate a failed start.
        """
        raise RuntimeError("boom")


class TestStartCommandAckRegistry:
    """Startup behaviour of ``_start_command_ack_registry``."""

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success the registry is attached to ``app.state``."""
        monkeypatch.setattr("snapper.server.app.ProcessCommandAckRegistry", _SucceedingRegistry)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_command_ack_registry(
            app, master_password="master", zmq_broker_xpub="tcp://broker:7501"
        )

        assert isinstance(app.state.command_ack_registry, _SucceedingRegistry)
        assert app.state.command_ack_registry.started_with == "tcp://broker:7501"

    @pytest.mark.asyncio
    async def test_start_failure_leaves_slot_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A startup exception is swallowed; the slot is left explicitly ``None``.

        The slot is pre-set to ``None`` before the start attempt so a reused app
        (or a mock ``app.state`` that auto-creates attributes) never leaves a
        truthy stale value that ``_stop`` would try to await.
        """
        monkeypatch.setattr("snapper.server.app.ProcessCommandAckRegistry", _FailingRegistry)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_command_ack_registry(
            app, master_password="master", zmq_broker_xpub="tcp://broker:7501"
        )

        assert app.state.command_ack_registry is None


class TestStopCommandAckRegistry:
    """Shutdown behaviour of ``_stop_command_ack_registry``."""

    @pytest.mark.asyncio
    async def test_stop_no_op_when_attribute_absent(self) -> None:
        """Returns cleanly when no registry is attached."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_command_ack_registry(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_registry(self) -> None:
        """Delegates to the attached registry's ``stop``."""
        registry = SimpleNamespace(stop=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(command_ack_registry=registry))

        await _stop_command_ack_registry(app)

        registry.stop.assert_awaited_once()
