"""Tests for kraken-sdk Retry-After honoring monkeypatches.

Covers the public API of
:mod:`snapper.infrastructure.exchanges.kraken_sdk_patches`:

* idempotent installation
* Retry-After header parsing edge cases
* connector identity propagation via ``_CURRENT_CONNECTOR_ID``
* publisher registration via ``_CURRENT_PUBLISHER`` + ``weakref.finalize``
* patched ``__get_reconnect_wait`` honors the stash, falls back to SDK
* connect shim captures real ``websockets.exceptions.InvalidStatus`` 429s

The tests touch module-level state. Each test that mutates the dicts
clears them on entry to keep the suite order-independent.
"""

import asyncio
import contextlib
import gc
import logging
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from kraken.exceptions import MaxReconnectError
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.spot import SpotWSClient
from kraken.spot.websocket.connectors import ConnectSpotWebsocket
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from kraken.utils.utils import WSState
from loguru import logger as _logger
from websockets.exceptions import ConnectionClosedError
from websockets.exceptions import InvalidStatus
from websockets.exceptions import ProxyError
from websockets.frames import Close
from websockets.http11 import Headers
from websockets.http11 import Response

from snapper.core.json_types import JsonObject
from snapper.infrastructure.exchanges import kraken_sdk_patches
from snapper.infrastructure.exchanges.kraken_sdk_patches import _ALREADY_SUBSCRIBED_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _ALREADY_SUBSCRIBED_PATCH_LOGGED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CLOSE_CODE_BACKOFF_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CONNECTOR_PUBLISHERS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_CONNECTOR_ID
from snapper.infrastructure.exchanges.kraken_sdk_patches import _FUTURES_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _FUTURES_PATCH_LOGGED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _LAST_CLOSE_CODE
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PATCH_LOGGED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PENDING_RETRY_AFTER_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    _PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RESUBSCRIBE_PACE_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RESUBSCRIBE_PACE_PATCH_LOGGED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RESUBSCRIBE_PACE_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MAX_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MIN_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _TEARDOWN_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _TEARDOWN_PATCH_LOGGED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_CLOSE_TIMEOUT_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_PING_INTERVAL_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_PING_TIMEOUT_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _consume_task_result
from snapper.infrastructure.exchanges.kraken_sdk_patches import _drive_reconnect_children
from snapper.infrastructure.exchanges.kraken_sdk_patches import _interruptible_backoff
from snapper.infrastructure.exchanges.kraken_sdk_patches import _kraken_futures_ws
from snapper.infrastructure.exchanges.kraken_sdk_patches import _parse_retry_after
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_futures_reconnect
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_get_reconnect_wait
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_init
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_manage_subscriptions
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_reconnect
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_recover_subscriptions
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_run
from snapper.infrastructure.exchanges.kraken_sdk_patches import _reap_reconnect_children
from snapper.infrastructure.exchanges.kraken_sdk_patches import _unregister_connector
from snapper.infrastructure.exchanges.kraken_sdk_patches import _wrap_connect_factory
from snapper.infrastructure.exchanges.kraken_sdk_patches import _ws_client
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    apply_kraken_already_subscribed_filter,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_futures_pool_routing
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_resubscribe_pacing
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_retry_after_honoring
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_ws_teardown_hardening
from snapper.infrastructure.exchanges.kraken_sdk_patches import force_close_ws_client
from snapper.infrastructure.exchanges.kraken_sdk_patches import get_registered_publisher
from snapper.infrastructure.exchanges.kraken_sdk_patches import log_kraken_sdk_patches_status
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import _POOL_HOLDER
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.utils.logging import _FILE_SINK_READY


def _make_429_response(retry_after: str = "412") -> Response:
    """Build a real ``websockets.http11.Response`` carrying a 429 + Retry-After.

    Given: a header value to attach as ``Retry-After``,
    When: the helper is invoked,
    Then: a fully constructed ``Response`` is returned suitable for
        raising ``InvalidStatus(response=...)`` in tests.
    """
    headers = Headers([("Retry-After", retry_after)])
    return Response(status_code=429, reason_phrase="Too Many Requests", headers=headers, body=b"")


class TestParseRetryAfter:
    """Header parsing edge cases for ``_parse_retry_after``."""

    def test_parse_valid_integer(self) -> None:
        """Spec — full Given/When/Then below.

        Given a numeric Retry-After header,
        When parsed,
        Then the float seconds value is returned.
        """
        assert _parse_retry_after({"Retry-After": "412"}) == pytest.approx(412.0)

    def test_parse_clamps_above_max(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Retry-After above the 15-minute ceiling,
        When parsed,
        Then the value is clamped to ``_RETRY_AFTER_MAX_SECONDS``.
        """
        assert _parse_retry_after({"Retry-After": "99999"}) == pytest.approx(
            _RETRY_AFTER_MAX_SECONDS
        )

    def test_parse_floors_below_min(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Retry-After below the 1-second floor,
        When parsed,
        Then the value is floored to ``_RETRY_AFTER_MIN_SECONDS``.
        """
        assert _parse_retry_after({"Retry-After": "0.1"}) == pytest.approx(_RETRY_AFTER_MIN_SECONDS)

    def test_parse_non_numeric_returns_none(self) -> None:
        """Spec — full Given/When/Then below.

        Given a non-numeric Retry-After header,
        When parsed,
        Then ``None`` is returned.
        """
        assert _parse_retry_after({"Retry-After": "abc"}) is None

    def test_parse_zero_returns_none(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Retry-After of zero,
        When parsed,
        Then ``None`` is returned (zero is treated as 'no useful hint').
        """
        assert _parse_retry_after({"Retry-After": "0"}) is None

    def test_parse_negative_returns_none(self) -> None:
        """Spec — full Given/When/Then below.

        Given a negative Retry-After header,
        When parsed,
        Then ``None`` is returned.
        """
        assert _parse_retry_after({"Retry-After": "-5"}) is None

    def test_parse_missing_header_returns_none(self) -> None:
        """Spec — full Given/When/Then below.

        Given headers without a Retry-After key,
        When parsed,
        Then ``None`` is returned.
        """
        assert _parse_retry_after({}) is None

    def test_parse_none_headers_returns_none(self) -> None:
        """Spec — full Given/When/Then below.

        Given ``None`` headers,
        When parsed,
        Then ``None`` is returned without raising.
        """
        assert _parse_retry_after(None) is None


class TestApplyIdempotent:
    """Installation of the patches must be safe to call repeatedly."""

    def test_apply_sets_flag_to_true(self) -> None:
        """Spec — full Given/When/Then below.

        Given a freshly imported module,
        When ``apply_kraken_retry_after_honoring`` is called,
        Then ``_PATCH_APPLIED[0]`` is True and the SDK class is rebound.
        """
        apply_kraken_retry_after_honoring()
        assert _PATCH_APPLIED[0] is True

    def test_second_apply_is_noop(self) -> None:
        """Spec — full Given/When/Then below.

        Given the patch is already installed,
        When ``apply_kraken_retry_after_honoring`` is called again,
        Then nothing changes and the function returns without error.
        """
        apply_kraken_retry_after_honoring()
        before = _PATCH_APPLIED[0]
        apply_kraken_retry_after_honoring()
        assert _PATCH_APPLIED[0] is True
        assert before is True


class TestApplyKrakenFuturesPoolRouting:
    """Installation of the Futures pool-routing rebind must be safe to call repeatedly."""

    def test_apply_rebinds_futures_connect(self) -> None:
        """Spec — first call installs the rebind.

        Given ``_FUTURES_PATCH_APPLIED[0]`` is False (or already True
            from a previous test — the rebind is idempotent),
        When ``apply_kraken_futures_pool_routing`` is called,
        Then ``_FUTURES_PATCH_APPLIED[0]`` is True AND
            ``kraken.futures.websocket.connect`` is now a
            ``_ConnectShim`` factory (not the bare
            ``websockets.connect``).
        """
        apply_kraken_futures_pool_routing()
        assert _FUTURES_PATCH_APPLIED[0] is True
        rebound = getattr(_kraken_futures_ws, "connect")
        assert callable(rebound)
        assert rebound is not _ws_client.connect

    def test_second_apply_is_noop(self) -> None:
        """Spec — second call is a no-op.

        Given the Futures rebind is already installed,
        When ``apply_kraken_futures_pool_routing`` is called again,
        Then the flag stays True and the function returns without
            re-wrapping (which would otherwise wrap the already-wrapped
            shim and break the shim's signature assumptions).
        """
        apply_kraken_futures_pool_routing()
        before = getattr(_kraken_futures_ws, "connect")
        apply_kraken_futures_pool_routing()
        assert _FUTURES_PATCH_APPLIED[0] is True
        assert getattr(_kraken_futures_ws, "connect") is before


class TestPatchedGetReconnectWait:
    """The reconnect-wait override consumes the per-connector stash."""

    def test_honors_pending_retry_after(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Retry-After stash for a connector,
        When ``_patched_get_reconnect_wait`` is called for that connector,
        Then the stashed value is returned and popped.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _PENDING_RETRY_AFTER_S[connector_id] = 412.0
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=3)
            assert wait == pytest.approx(412.0)
            assert connector_id not in _PENDING_RETRY_AFTER_S
        finally:
            _PENDING_RETRY_AFTER_S.pop(connector_id, None)

    def test_falls_back_to_original_when_no_pending(self) -> None:
        """Spec — full Given/When/Then below.

        Given no Retry-After stash for the connector,
        When ``_patched_get_reconnect_wait`` is called,
        Then the SDK's original exponential backoff value is returned
        (the SDK formula is ``random() * min(180, 2**attempts - 1) + 1``
        so the result is always at least 1).
        """
        connector = MagicMock()
        _PENDING_RETRY_AFTER_S.pop(id(connector), None)
        wait = _patched_get_reconnect_wait(connector, attempts=3)
        assert wait >= 1


class TestPatchedRunSetsContextVar:
    """The ``__run`` override stamps ``_CURRENT_CONNECTOR_ID``."""

    @pytest.mark.asyncio
    async def test_context_var_set_during_run_and_reset_after(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — full Given/When/Then below.

        Given a patched ``__run`` wrapping a fake SDK run,
        When the coroutine executes,
        Then ``_CURRENT_CONNECTOR_ID`` carries ``id(self)`` during the body
        and is restored to ``None`` after.
        """
        observed: dict[str, Any] = {}

        async def fake_run(self_obj: Any, event_obj: asyncio.Event) -> None:
            observed["during"] = _CURRENT_CONNECTOR_ID.get()

        connector = MagicMock()
        event = asyncio.Event()
        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_RUN", fake_run)
        await _patched_run(connector, event)
        assert observed["during"] == id(connector)
        assert _CURRENT_CONNECTOR_ID.get() is None


class TestConnectShim:
    """Connect shim observes ``InvalidStatus`` 429 responses."""

    @pytest.mark.asyncio
    async def test_429_captured_with_pending_set(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connect call raising ``InvalidStatus`` 429,
        When the shim's ``__aenter__`` runs inside a connector ContextVar span,
        Then the Retry-After is stashed against ``id(self)`` and the
        exception is re-raised so the SDK enters its reconnect path.
        """
        response = _make_429_response(retry_after="412")

        class _FakeCM:
            async def __aenter__(self) -> Any:
                raise InvalidStatus(response=response)

            async def __aexit__(self, *_: Any) -> Any:
                return None

        def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeCM:
            return _FakeCM()

        shim_cls = _wrap_connect_factory(fake_connect)
        test_connector_id_1: int = 999999
        connector_id = test_connector_id_1
        token = _CURRENT_CONNECTOR_ID.set(connector_id)
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            shim = shim_cls("wss://example/ws")
            with pytest.raises(InvalidStatus):
                async with shim:
                    pass
            assert _PENDING_RETRY_AFTER_S[connector_id] == pytest.approx(412.0)
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)
            _PENDING_RETRY_AFTER_S.pop(connector_id, None)

    @pytest.mark.asyncio
    async def test_429_without_retry_after_still_raises(self) -> None:
        """Spec — full Given/When/Then below.

        Given an ``InvalidStatus`` 429 with no Retry-After header,
        When the shim's ``__aenter__`` runs,
        Then no stash is created and the exception is re-raised.
        """
        response = Response(
            status_code=429, reason_phrase="Too Many Requests", headers=Headers(), body=b""
        )

        class _FakeCM:
            async def __aenter__(self) -> Any:
                raise InvalidStatus(response=response)

            async def __aexit__(self, *_: Any) -> Any:
                return None

        def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeCM:
            return _FakeCM()

        shim_cls = _wrap_connect_factory(fake_connect)
        test_connector_id_2: int = 999998
        connector_id = test_connector_id_2
        token = _CURRENT_CONNECTOR_ID.set(connector_id)
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            shim = shim_cls("wss://example/ws")
            with pytest.raises(InvalidStatus):
                async with shim:
                    pass
            assert connector_id not in _PENDING_RETRY_AFTER_S
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)

    @pytest.mark.asyncio
    async def test_non_429_passes_through_unchanged(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connect call raising ``InvalidStatus`` with status 503,
        When the shim's ``__aenter__`` runs,
        Then no stash is created and the exception is re-raised verbatim.
        """
        response = Response(
            status_code=503, reason_phrase="Service Unavailable", headers=Headers(), body=b""
        )

        class _FakeCM:
            async def __aenter__(self) -> Any:
                raise InvalidStatus(response=response)

            async def __aexit__(self, *_: Any) -> Any:
                return None

        def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeCM:
            return _FakeCM()

        shim_cls = _wrap_connect_factory(fake_connect)
        test_connector_id_3: int = 999997
        connector_id = test_connector_id_3
        token = _CURRENT_CONNECTOR_ID.set(connector_id)
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            shim = shim_cls("wss://example/ws")
            with pytest.raises(InvalidStatus):
                async with shim:
                    pass
            assert connector_id not in _PENDING_RETRY_AFTER_S
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)

    @pytest.mark.asyncio
    async def test_aexit_delegates_to_underlying_cm(self) -> None:
        """Spec — full Given/When/Then below.

        Given a successful ``__aenter__``,
        When ``__aexit__`` is invoked,
        Then the call delegates to the wrapped context manager's ``__aexit__``.
        """
        exit_called: dict[str, Any] = {}

        class _FakeCM:
            async def __aenter__(self) -> Any:
                return "socket"

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
                exit_called["args"] = (exc_type, exc, tb)
                return None

        def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeCM:
            return _FakeCM()

        shim_cls = _wrap_connect_factory(fake_connect)
        async with shim_cls("wss://example/ws") as socket:
            assert socket == "socket"
        assert exit_called["args"] == (None, None, None)


class TestPublisherRegistration:
    """Publisher-to-connector mapping via ``_CURRENT_PUBLISHER`` ContextVar."""

    def test_init_registers_when_publisher_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — full Given/When/Then below.

        Given ``_CURRENT_PUBLISHER`` set to a publisher,
        When ``_patched_init`` runs (via the patched SDK __init__),
        Then the new connector id maps to the publisher in ``_CONNECTOR_PUBLISHERS``.
        """
        publisher = MagicMock()
        token = _CURRENT_PUBLISHER.set(publisher)
        try:
            connector = ConnectSpotWebsocketBase.__new__(ConnectSpotWebsocketBase)
            captured: dict[str, Any] = {}

            def fake_original(self_obj: Any, *args: Any, **kwargs: Any) -> None:
                captured["self"] = self_obj

            monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_INIT", fake_original)
            _patched_init(connector)
            assert _CONNECTOR_PUBLISHERS[id(connector)] is publisher
            assert get_registered_publisher(id(connector)) is publisher
        finally:
            _CURRENT_PUBLISHER.reset(token)
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)

    def test_init_does_not_register_when_no_publisher(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — full Given/When/Then below.

        Given no ``_CURRENT_PUBLISHER`` set,
        When ``_patched_init`` runs,
        Then no registry entry is created.
        """
        assert _CURRENT_PUBLISHER.get() is None
        connector = ConnectSpotWebsocketBase.__new__(ConnectSpotWebsocketBase)

        def fake_original(self_obj: Any, *args: Any, **kwargs: Any) -> None:
            return None

        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_INIT", fake_original)
        _patched_init(connector)
        assert id(connector) not in _CONNECTOR_PUBLISHERS

    def test_weakref_finalize_unregisters_on_gc(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — full Given/When/Then below.

        Given a connector registered against a publisher,
        When the connector is dereferenced and garbage-collected,
        Then ``weakref.finalize`` clears the registry entry automatically.
        """
        publisher = MagicMock()
        connector_id_holder: dict[str, int] = {}
        token = _CURRENT_PUBLISHER.set(publisher)
        try:
            connector = ConnectSpotWebsocketBase.__new__(ConnectSpotWebsocketBase)
            connector_id_holder["id"] = id(connector)

            def fake_original(self_obj: Any, *args: Any, **kwargs: Any) -> None:
                return None

            monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_INIT", fake_original)
            _patched_init(connector)
            assert connector_id_holder["id"] in _CONNECTOR_PUBLISHERS
            del connector
            gc.collect()
            assert connector_id_holder["id"] not in _CONNECTOR_PUBLISHERS
        finally:
            _CURRENT_PUBLISHER.reset(token)
            _CONNECTOR_PUBLISHERS.pop(connector_id_holder.get("id", -1), None)

    def test_unregister_connector_is_safe_when_missing(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connector id not present in the registry,
        When ``_unregister_connector`` is called,
        Then no exception is raised and the registry is unchanged.
        """
        before = dict(_CONNECTOR_PUBLISHERS)
        _unregister_connector(-987654)
        assert before == _CONNECTOR_PUBLISHERS


def _make_spot_connector(wait_s: float = 0.0, *, keep_alive: bool = True) -> ConnectSpotWebsocket:
    """Build a real Spot connector with a deterministic reconnect wait.

    The reconnect wait function is shadowed with an instance attribute so the
    hardened reconnect's mangled-name lookup picks the deterministic value
    instead of the SDK's randomized exponential.

    Args:
        wait_s: Backoff duration the connector should request.
        keep_alive: Initial ``keep_alive`` flag value.

    Returns:
        A ``ConnectSpotWebsocket`` bound to a mock client.
    """
    connector = ConnectSpotWebsocket(client=MagicMock(), endpoint="wss://unit-test", callback=None)
    connector.keep_alive = keep_alive
    connector._ConnectSpotWebsocketBase__get_reconnect_wait = lambda attempts: wait_s
    return connector


def _make_futures_connector(
    wait_s: float = 0.0, *, keep_alive: bool = True
) -> ConnectFuturesWebsocket:
    """Build a real Futures connector with a deterministic reconnect wait.

    Args:
        wait_s: Backoff duration the connector should request.
        keep_alive: Initial ``keep_alive`` flag value.

    Returns:
        A ``ConnectFuturesWebsocket`` bound to a mock client.
    """
    connector = ConnectFuturesWebsocket(client=MagicMock(), endpoint="unit-test", callback=None)
    connector.keep_alive = keep_alive
    connector._ConnectFuturesWebsocket__get_reconnect_wait = lambda attempts: wait_s
    return connector


class _CancelTracker:
    """Child-coroutine factory recording whether each child saw cancellation."""

    def __init__(self) -> None:
        self.cancelled: dict[str, bool] = {}
        self.started: dict[str, bool] = {}

    def hanging_child(self, name: str) -> Any:
        """Return an async callable that hangs until cancelled.

        Args:
            name: Key under which start/cancel observations are recorded.

        Returns:
            An async function compatible with the reconnect child signature.
        """
        self.cancelled[name] = False
        self.started[name] = False

        async def _child(event: asyncio.Event) -> None:
            self.started[name] = True
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled[name] = True
                raise

        return _child


class TestPatchedReconnect:
    """Hardened Spot ``__reconnect``: watchdog notify + #143 teardown safety."""

    @pytest.mark.asyncio
    async def test_notifies_publisher_before_reconnect_body(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connector mapped to a publisher with the watchdog hook,
        When ``_patched_reconnect`` is awaited,
        Then ``publisher._on_sdk_reconnect_attempt`` is called and the
        hardened body runs to its keep-alive early return (state CLOSED).
        """
        connector = _make_spot_connector(keep_alive=False)
        publisher = MagicMock()
        publisher._on_sdk_reconnect_attempt = MagicMock()
        _CONNECTOR_PUBLISHERS[id(connector)] = publisher
        try:
            await _patched_reconnect(connector)
        finally:
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        publisher._on_sdk_reconnect_attempt.assert_called_once_with()
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_unregistered_connector_still_reconnects(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connector not in the publisher registry (e.g. an executor
        process where no publisher ever registers),
        When ``_patched_reconnect`` is awaited,
        Then no publisher notification occurs and the hardened body still
        runs without raising.
        """
        connector = _make_spot_connector(keep_alive=False)
        _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        await _patched_reconnect(connector)
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_publisher_without_reconnect_hook_still_reconnects(self) -> None:
        """Spec — publishers without ``_on_sdk_reconnect_attempt`` do not break reconnect.

        Given a connector registered against a publisher that lacks
        the optional ``_on_sdk_reconnect_attempt`` watchdog hook
        (e.g. ``KrakenEquitiesMarketDataPublisher`` — which sets
        ``_CURRENT_PUBLISHER`` for egress tagging but does not need
        the reconnect-storm watchdog),
        When ``_patched_reconnect`` is awaited,
        Then no AttributeError is raised and the hardened body still runs.
        Regression guard for the live failure where adding Equities to
        _CURRENT_PUBLISHER broke every reconnect cycle because the patch
        was calling the hook unconditionally.
        """
        connector = _make_spot_connector(keep_alive=False)
        publisher_without_hook = MagicMock(spec=[])
        _CONNECTOR_PUBLISHERS[id(connector)] = publisher_without_hook
        try:
            await _patched_reconnect(connector)
        finally:
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_raises_max_reconnect_error_at_budget(self) -> None:
        """Spec — stock MaxReconnectError semantics are preserved.

        Given a connector whose reconnect counter sits one below the budget,
        When ``_patched_reconnect`` is awaited,
        Then ``MaxReconnectError`` is raised before any backoff or child
        spawn, matching the stock SDK behaviour.
        """
        connector = _make_spot_connector()
        connector._ConnectSpotWebsocketBase__reconnect_num = connector.MAX_RECONNECT_NUM - 1
        with pytest.raises(MaxReconnectError):
            await _patched_reconnect(connector)

    @pytest.mark.asyncio
    async def test_keep_alive_flip_mid_backoff_skips_children(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — the #143 root cause: teardown during backoff is prompt and clean.

        Given a connector sleeping a long reconnect backoff,
        When ``keep_alive`` flips False mid-sleep (the ``stop()`` path),
        Then the reconnect returns within the poll interval WITHOUT spawning
        any child task — the stock loop slept the full backoff and then
        orphaned freshly spawned children.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_BACKOFF_POLL_S", 0.01)
        tracker = _CancelTracker()
        connector = _make_spot_connector(wait_s=30.0)
        connector._recover_subscriptions = tracker.hanging_child("recover")
        connector._ConnectSpotWebsocketBase__run = tracker.hanging_child("run")
        reconnect_task = asyncio.create_task(_patched_reconnect(connector))
        await asyncio.sleep(0.05)
        connector.keep_alive = False
        await asyncio.wait_for(reconnect_task, timeout=1.0)
        assert tracker.started == {"recover": False, "run": False}
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_clean_child_completion_breaks_loop(self) -> None:
        """Spec — an all-done child set terminates the wait loop.

        Given children that both complete without raising while
        ``keep_alive`` stays True,
        When ``_patched_reconnect`` is awaited,
        Then it returns instead of hot-spinning ``asyncio.wait`` on an
        already-done task set (latent stock-SDK hazard).
        """

        async def _instant(event: asyncio.Event) -> None:
            return None

        connector = _make_spot_connector()
        connector._recover_subscriptions = _instant
        connector._ConnectSpotWebsocketBase__run = _instant
        await asyncio.wait_for(_patched_reconnect(connector), timeout=1.0)
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_child_exception_cancels_sibling(self) -> None:
        """Spec — a failing ``__run`` cannot strand ``_recover_subscriptions``.

        Given a ``__run`` child that raises immediately (connect failure
        during a blackout) and a recover child waiting on the readiness
        event,
        When ``_patched_reconnect`` is awaited,
        Then the recover child is cancelled by the reap instead of waiting
        on ``event.wait()`` forever, and the connector ends CLOSED.
        """
        tracker = _CancelTracker()

        async def _failing_run(event: asyncio.Event) -> None:
            raise OSError("simulated blackout")

        connector = _make_spot_connector()
        connector._recover_subscriptions = tracker.hanging_child("recover")
        connector._ConnectSpotWebsocketBase__run = _failing_run
        await asyncio.wait_for(_patched_reconnect(connector), timeout=1.0)
        assert tracker.cancelled["recover"] is True
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_parent_cancellation_reaps_children(self) -> None:
        """Spec — THE regression guard for the reverted first #143 fix attempt.

        Given a reconnect invocation whose children both hang (recover on the
        readiness event, run inside the handshake),
        When the parent reconnect task is cancelled (the
        ``force_close_ws_client`` path),
        Then BOTH children observe cancellation — the stock loop's
        ``asyncio.wait`` never cancelled its awaitables, which orphaned the
        children and produced ``Task exception was never retrieved``.
        """
        tracker = _CancelTracker()
        connector = _make_spot_connector()
        connector._recover_subscriptions = tracker.hanging_child("recover")
        connector._ConnectSpotWebsocketBase__run = tracker.hanging_child("run")
        reconnect_task = asyncio.create_task(_patched_reconnect(connector))
        await asyncio.sleep(0.05)
        assert tracker.started == {"recover": True, "run": True}
        reconnect_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reconnect_task
        assert tracker.cancelled == {"recover": True, "run": True}


class TestPatchedFuturesReconnect:
    """Hardened Futures ``__reconnect``: same #143 teardown safety."""

    @pytest.mark.asyncio
    async def test_raises_max_reconnect_error_at_budget(self) -> None:
        """Spec — stock bare MaxReconnectError semantics are preserved.

        Given a Futures connector whose counter sits one below the budget,
        When ``_patched_futures_reconnect`` is awaited,
        Then ``MaxReconnectError`` is raised before any backoff.
        """
        connector = _make_futures_connector()
        connector._ConnectFuturesWebsocket__reconnect_num = connector.MAX_RECONNECT_NUM - 1
        with pytest.raises(MaxReconnectError):
            await _patched_futures_reconnect(connector)

    @pytest.mark.asyncio
    async def test_keep_alive_flip_mid_backoff_skips_children(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — teardown during a Futures backoff is prompt and spawn-free.

        Given a Futures connector sleeping a long reconnect backoff,
        When ``keep_alive`` flips False mid-sleep,
        Then the reconnect returns within the poll interval without spawning
        children.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_BACKOFF_POLL_S", 0.01)
        tracker = _CancelTracker()
        connector = _make_futures_connector(wait_s=30.0)
        connector._ConnectFuturesWebsocket__recover_subscription_req_msg = tracker.hanging_child(
            "recover"
        )
        connector._ConnectFuturesWebsocket__run = tracker.hanging_child("run")
        reconnect_task = asyncio.create_task(_patched_futures_reconnect(connector))
        await asyncio.sleep(0.05)
        connector.keep_alive = False
        await asyncio.wait_for(reconnect_task, timeout=1.0)
        assert tracker.started == {"recover": False, "run": False}
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_child_exception_clears_challenge_and_cancels_sibling(self) -> None:
        """Spec — stock challenge-flag reset is preserved alongside the reap.

        Given a private Futures connector with a ready challenge and a
        ``__run`` child that raises,
        When ``_patched_futures_reconnect`` is awaited,
        Then the challenge-ready flag is cleared (stock behaviour — the
        recovered connection must re-authenticate) and the recover sibling
        is cancelled by the reap.
        """
        tracker = _CancelTracker()

        async def _failing_run(event: asyncio.Event) -> None:
            raise OSError("simulated blackout")

        connector = _make_futures_connector()
        connector._ConnectFuturesWebsocket__challenge_ready = True
        connector._ConnectFuturesWebsocket__recover_subscription_req_msg = tracker.hanging_child(
            "recover"
        )
        connector._ConnectFuturesWebsocket__run = _failing_run
        await asyncio.wait_for(_patched_futures_reconnect(connector), timeout=1.0)
        assert getattr(connector, "_ConnectFuturesWebsocket__challenge_ready") is False
        assert tracker.cancelled["recover"] is True
        assert connector.state == WSState.CLOSED

    @pytest.mark.asyncio
    async def test_parent_cancellation_reaps_children(self) -> None:
        """Spec — Futures mirror of the reverted-fix regression guard.

        Given a Futures reconnect whose children both hang,
        When the parent reconnect task is cancelled,
        Then both children observe cancellation.
        """
        tracker = _CancelTracker()
        connector = _make_futures_connector()
        connector._ConnectFuturesWebsocket__recover_subscription_req_msg = tracker.hanging_child(
            "recover"
        )
        connector._ConnectFuturesWebsocket__run = tracker.hanging_child("run")
        reconnect_task = asyncio.create_task(_patched_futures_reconnect(connector))
        await asyncio.sleep(0.05)
        reconnect_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reconnect_task
        assert tracker.cancelled == {"recover": True, "run": True}


class TestCloseCodeBackoff:
    """Kraken WebSocket close codes drive custom reconnect backoff.

    Captures the production cascade observed 2026-05-21 07:00-07:14: Kraken
    sent code 1012 'service restart', SDK's exponential backoff was too
    aggressive, Kraken responded with code 1008 'rate limit exceeded',
    and Cloudflare eventually banned the source IP with HTTP 429. The
    per-code backoff prevents the cascade origin.
    """

    def test_1012_returns_30s_backoff_and_pops_stash(self) -> None:
        """Close code 1012 picks the Kraken-service-restart backoff.

        Given a connector with last close code 1012 stashed,
        When ``_patched_get_reconnect_wait`` runs,
        Then the stash returns 30 seconds and is popped.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1012
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=1)
            assert wait == pytest.approx(30.0)
            assert connector_id not in _LAST_CLOSE_CODE
        finally:
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_1008_returns_15s_backoff(self) -> None:
        """Close code 1008 picks the per-user-rate-limit backoff.

        Given a connector with last close code 1008 stashed,
        When ``_patched_get_reconnect_wait`` runs,
        Then the stash returns 15 seconds.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1008
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=1)
            assert wait == pytest.approx(15.0)
        finally:
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_1013_returns_60s_backoff(self) -> None:
        """Close code 1013 picks the trading-engine-unavailable backoff.

        Given a connector with last close code 1013 stashed,
        When ``_patched_get_reconnect_wait`` runs,
        Then the stash returns 60 seconds.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1013
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=1)
            assert wait == pytest.approx(60.0)
        finally:
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_1011_returns_10s_backoff(self) -> None:
        """Close code 1011 picks the internal-server-error backoff.

        Given a connector with last close code 1011 stashed,
        When ``_patched_get_reconnect_wait`` runs,
        Then the stash returns 10 seconds.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1011
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=1)
            assert wait == pytest.approx(10.0)
        finally:
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_unknown_close_code_falls_back_to_original(self) -> None:
        """Unknown close codes do not match the table.

        Given a connector with a close code not in ``_CLOSE_CODE_BACKOFF_S``,
        When ``_patched_get_reconnect_wait`` runs,
        Then the SDK's exponential backoff is used and the stash is still popped.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1006
        _PENDING_RETRY_AFTER_S.pop(connector_id, None)
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=2)
            assert wait >= 1
            assert connector_id not in _LAST_CLOSE_CODE
        finally:
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_429_outranks_close_code(self) -> None:
        """Cloudflare 429 Retry-After wins over a close-code stash.

        Given a connector with both a stashed Retry-After AND a stashed close code,
        When ``_patched_get_reconnect_wait`` runs,
        Then the Retry-After value is returned (handshake 429 is more specific).
        """
        connector = MagicMock()
        connector_id = id(connector)
        _PENDING_RETRY_AFTER_S[connector_id] = 412.0
        _LAST_CLOSE_CODE[connector_id] = 1012
        try:
            wait = _patched_get_reconnect_wait(connector, attempts=1)
            assert wait == pytest.approx(412.0)
        finally:
            _PENDING_RETRY_AFTER_S.pop(connector_id, None)
            _LAST_CLOSE_CODE.pop(connector_id, None)

    def test_backoff_table_is_finite_and_documented(self) -> None:
        """The close-code backoff table covers the four documented Kraken codes.

        Given the production cascade (1012 → 1008 → 1013, with 1011 as a
        speculative add for internal-server-error),
        When the module is imported,
        Then ``_CLOSE_CODE_BACKOFF_S`` contains exactly those four codes
        with sensible positive durations.
        """
        assert set(_CLOSE_CODE_BACKOFF_S.keys()) == {1008, 1011, 1012, 1013}
        assert all(v > 0 for v in _CLOSE_CODE_BACKOFF_S.values())


class TestPatchedRunCapturesCloseCode:
    """``_patched_run`` records the server-sent close code for the watchdog."""

    @pytest.mark.asyncio
    async def test_connection_closed_with_code_stashes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The wrapper records ``exc.rcvd.code`` on ConnectionClosed.

        Given an underlying ``__run`` that raises ``ConnectionClosed`` with a
        ``Close`` frame whose ``code`` is 1012,
        When the patched ``__run`` is awaited,
        Then ``_LAST_CLOSE_CODE`` carries 1012 for ``id(self)`` and the
        exception is re-raised so the SDK's reconnect path still fires.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE.pop(connector_id, None)
        close_frame = Close(code=1012, reason="Kraken websockets restarting")

        async def raising_run(self_obj: Any, event_obj: asyncio.Event) -> None:
            raise ConnectionClosedError(rcvd=close_frame, sent=None)

        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_RUN", raising_run)
        closing_run_event = asyncio.Event()
        with pytest.raises(ConnectionClosedError):
            await kraken_sdk_patches._patched_run(connector, closing_run_event)
        assert _LAST_CLOSE_CODE.get(connector_id) == 1012
        _LAST_CLOSE_CODE.pop(connector_id, None)

    @pytest.mark.asyncio
    async def test_connection_closed_without_rcvd_does_not_stash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``ConnectionClosed`` with no ``rcvd`` frame is left unstashed.

        Given an underlying ``__run`` that raises ``ConnectionClosed`` with
        ``rcvd=None`` (client-side abort or other corner case),
        When the patched ``__run`` is awaited,
        Then no entry is added to ``_LAST_CLOSE_CODE`` (the SDK exponential
        backoff is the correct response in that case).
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE.pop(connector_id, None)

        async def raising_run(self_obj: Any, event_obj: asyncio.Event) -> None:
            raise ConnectionClosedError(rcvd=None, sent=None)

        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_RUN", raising_run)
        unstashed_run_event = asyncio.Event()
        with pytest.raises(ConnectionClosedError):
            await kraken_sdk_patches._patched_run(connector, unstashed_run_event)
        assert connector_id not in _LAST_CLOSE_CODE

    @pytest.mark.asyncio
    async def test_normal_completion_does_not_stash(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A clean return from ``__run`` leaves no close-code state behind.

        Given an underlying ``__run`` that returns normally,
        When the patched ``__run`` is awaited,
        Then ``_LAST_CLOSE_CODE`` is unchanged and the ContextVar is reset.
        """
        connector = MagicMock()
        connector_id = id(connector)
        _LAST_CLOSE_CODE.pop(connector_id, None)

        async def clean_run(self_obj: Any, event_obj: asyncio.Event) -> None:
            return None

        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_RUN", clean_run)
        await kraken_sdk_patches._patched_run(connector, asyncio.Event())
        assert connector_id not in _LAST_CLOSE_CODE
        assert _CURRENT_CONNECTOR_ID.get() is None


class TestUnregisterClearsAllStashes:
    """``_unregister_connector`` cleans up every per-connector dict."""

    def test_clears_publisher_retry_after_and_close_code(self) -> None:
        """Single call drops all three stashes for a connector id.

        Given a connector id present in ``_CONNECTOR_PUBLISHERS``,
        ``_PENDING_RETRY_AFTER_S``, AND ``_LAST_CLOSE_CODE``,
        When ``_unregister_connector`` runs,
        Then every dict no longer contains that id.
        """
        cid = 424242
        _CONNECTOR_PUBLISHERS[cid] = MagicMock()
        _PENDING_RETRY_AFTER_S[cid] = 50.0
        _LAST_CLOSE_CODE[cid] = 1012
        _unregister_connector(cid)
        assert cid not in _CONNECTOR_PUBLISHERS
        assert cid not in _PENDING_RETRY_AFTER_S
        assert cid not in _LAST_CLOSE_CODE


class TestPhaseBPrimeShim:
    """Egress-pool shim integration tests — pool reserve + 429 + 1015 paths.

    The shim now consults ``get_egress_pool()`` and, when a pool is
    configured, reserves a route, injects the proxy kwarg, and routes
    429 / 1015 captures to ``reservation.quarantine`` instead of the
    legacy global stash. With pool disabled, the original
    global-stash behaviour is preserved byte-for-byte.
    """

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        """Clear the egress-pool singleton + global stashes before/after each test."""
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()
        _CONNECTOR_PUBLISHERS.clear()
        yield
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()
        _CONNECTOR_PUBLISHERS.clear()

    @staticmethod
    def _enable_pool(
        *,
        on_all_quarantined: str = "wait",
        with_socks5: bool = False,
    ) -> Any:
        """Helper — configure the singleton with one or two routes."""
        routes = [RouteConfig(id="default", kind="direct", priority=0)]
        if with_socks5:
            routes.append(
                RouteConfig(
                    id="wg-uk-1",
                    kind="socks5",
                    proxy_url="socks5h://snapper-egress:1081",
                    priority=10,
                )
            )
        config = EgressPoolConfig(
            enabled=True,
            on_all_quarantined=on_all_quarantined,
            routes=routes,
        )
        return configure_egress_pool(config)

    def test_shim_no_pool_preserves_phase_a_kwargs(self) -> None:
        """Spec — pool disabled passes caller kwargs through, adds keepalive.

        Given get_egress_pool() returns None,
        When _ConnectShim is constructed with arbitrary kwargs,
        Then original_connect is called with no proxy override (preserving
        the websockets-16 ``proxy=True`` env auto-detect), the caller's
        ``ping_interval`` preserved, and the enforced transport keepalive
        (``ping_timeout``/``close_timeout``) merged in.
        """
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken", ping_interval=20)
        assert seen_kwargs == {
            "ping_interval": 20,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    def test_shim_injects_default_keepalive_when_absent(self) -> None:
        """Spec — keepalive defaults are injected when the caller omits them.

        Given get_egress_pool() returns None and no keepalive kwargs,
        When _ConnectShim is constructed,
        Then original_connect receives the default ping interval plus the
        enforced ping/close timeouts.
        """
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken")
        assert seen_kwargs == {
            "ping_interval": _WS_PING_INTERVAL_S,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    def test_shim_direct_route_injects_proxy_none(self) -> None:
        """Spec — direct route forces ``proxy=None`` to override env detection.

        Given the pool is enabled with only a direct route,
        When _ConnectShim is constructed,
        Then original_connect is called with ``proxy=None`` injected.
        The explicit None overrides the websockets-16 default of
        ``proxy=True`` so an inadvertently-set HTTPS_PROXY cannot
        activate.
        """
        self._enable_pool()
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken")
        assert seen_kwargs == {
            "proxy": None,
            "ping_interval": _WS_PING_INTERVAL_S,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    def test_shim_passes_explicit_private_traffic_class_to_pool(self) -> None:
        """Spec — explicit private identity is passed to ``pool.reserve``.

        Given an active executor egress identity,
        When _ConnectShim is constructed,
        Then the pool reserve call receives ``traffic_class="private"``.
        """

        class _ReservationDouble:
            route_id = "default"
            proxy_url: str | None = None

            def websocket_kwargs(self) -> dict[str, str | None]:
                """Return direct WebSocket kwargs.

                Returns:
                    Proxy kwargs for a direct route.
                """
                return {"proxy": None}

            def release(self) -> None:
                """Release the test reservation."""
                return None

        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.reserve.return_value = _ReservationDouble()

        def fake_connect(*args: object, **kwargs: object) -> MagicMock:
            return MagicMock()

        with patch(
            "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
            return_value=fake_pool,
        ), egress_identity(
            exchange="kraken",
            traffic_class="private",
            owner="executor",
            operation="order_ws",
        ):
            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls("wss://kraken")

        fake_pool.reserve.assert_called_once_with(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
            target_host="kraken",
            connection_kind="ws",
        )

    def test_shim_passes_keyword_uri_host_to_pool(self) -> None:
        """Spec — keyword URI hostname is passed to the egress pool.

        Given the pool is enabled and the connect URI is supplied as a kwarg,
        When _ConnectShim is constructed,
        Then the pool receives the lowercase URI hostname only.
        """

        class _ReservationDouble:
            route_id = "default"
            proxy_url: str | None = None

            def websocket_kwargs(self) -> dict[str, str | None]:
                """Return direct WebSocket kwargs.

                Returns:
                    Proxy kwargs for a direct route.
                """
                return {"proxy": None}

            def release(self) -> None:
                """Release the test reservation."""
                return None

        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.reserve.return_value = _ReservationDouble()

        def fake_connect(*args: object, **kwargs: object) -> MagicMock:
            return MagicMock()

        with patch(
            "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
            return_value=fake_pool,
        ):
            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls(uri="wss://FUTURES.KRAKEN.COM/ws/v1?token=secret")

        fake_pool.reserve.assert_called_once_with(
            exchange="kraken",
            purpose="websocket",
            traffic_class="public",
            target_host="futures.kraken.com",
            connection_kind="ws",
        )

    def test_shim_missing_uri_passes_none_target_host_to_pool(self) -> None:
        """Spec — missing connect URI leaves the egress target host unknown.

        Given the pool is enabled and no positional or keyword URI is present,
        When _ConnectShim is constructed,
        Then the pool receives ``target_host=None``.
        """

        class _ReservationDouble:
            route_id = "default"
            proxy_url: str | None = None

            def websocket_kwargs(self) -> dict[str, str | None]:
                """Return direct WebSocket kwargs.

                Returns:
                    Proxy kwargs for a direct route.
                """
                return {"proxy": None}

            def release(self) -> None:
                """Release the test reservation."""
                return None

        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.reserve.return_value = _ReservationDouble()

        def fake_connect(*args: object, **kwargs: object) -> MagicMock:
            return MagicMock()

        with patch(
            "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
            return_value=fake_pool,
        ):
            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls()

        fake_pool.reserve.assert_called_once_with(
            exchange="kraken",
            purpose="websocket",
            traffic_class="public",
            target_host=None,
            connection_kind="ws",
        )

    def test_shim_unparseable_uri_passes_none_target_host_to_pool(self) -> None:
        """Spec — URI without a hostname leaves the egress target host unknown.

        Given the pool is enabled and the connect URI has no URL hostname,
        When _ConnectShim is constructed,
        Then the pool receives ``target_host=None``.
        """

        class _ReservationDouble:
            route_id = "default"
            proxy_url: str | None = None

            def websocket_kwargs(self) -> dict[str, str | None]:
                """Return direct WebSocket kwargs.

                Returns:
                    Proxy kwargs for a direct route.
                """
                return {"proxy": None}

            def release(self) -> None:
                """Release the test reservation."""
                return None

        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.reserve.return_value = _ReservationDouble()

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            return MagicMock()

        with patch(
            "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
            return_value=fake_pool,
        ):
            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls("not-a-url")

        fake_pool.reserve.assert_called_once_with(
            exchange="kraken",
            purpose="websocket",
            traffic_class="public",
            target_host=None,
            connection_kind="ws",
        )

    def test_shim_socks5_route_injects_proxy_url(self) -> None:
        """Spec — when socks5 is preferred, proxy=socks5h://... is injected.

        Given a pool with a SOCKS5 route at lower priority than direct,
        And the direct route is quarantined so socks5 wins selection,
        When the shim runs,
        Then original_connect receives ``proxy="socks5h://..."``.
        """
        pool = self._enable_pool(with_socks5=True)

        pool._quarantine_route(
            "default",
            datetime.now(UTC) + timedelta(seconds=600),
            "http-429",
        )
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken")
        assert seen_kwargs == {
            "proxy": "socks5h://snapper-egress:1081",
            "ping_interval": _WS_PING_INTERVAL_S,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    def test_shim_sync_construct_failure_releases_reservation(self) -> None:
        """Spec — original_connect raising in __init__ releases the reservation.

        Given the pool is enabled,
        When original_connect raises synchronously,
        Then the reservation's in_use_count returns to 0 before the
        exception propagates.
        """
        pool = self._enable_pool()

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            raise RuntimeError("simulated sync raise")

        shim_cls = _wrap_connect_factory(fake_connect)
        with pytest.raises(RuntimeError, match="simulated sync raise"):
            shim_cls("wss://kraken")
        snap = pool.snapshot()[0]
        assert snap.in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_429_quarantines_active_route_pool_path(self) -> None:
        """Spec — 429 quarantines the borrowed route, NOT the global stash.

        Given the pool is enabled and one direct route is borrowed,
        When the handshake raises InvalidStatus 429 with Retry-After=600,
        Then the borrowed route is quarantined for 600 s with
        reason="http-429" AND ``_PENDING_RETRY_AFTER_S`` is NOT
        populated (pool quarantine replaces the global stash when
        pool is enabled).
        """
        pool = self._enable_pool()
        response = Response(
            status_code=429,
            reason_phrase="Too Many Requests",
            headers=Headers([("Retry-After", "600")]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        token = _CURRENT_CONNECTOR_ID.set(424242)
        try:
            with pytest.raises(InvalidStatus):
                await shim.__aenter__()
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)
        snap = pool.snapshot()[0]
        assert snap.quarantine_until is not None
        assert snap.last_handshake_429_at is not None
        assert 424242 not in _PENDING_RETRY_AFTER_S

    @pytest.mark.asyncio
    async def test_shim_429_legacy_stash_path_when_pool_disabled(self) -> None:
        """Spec — pool disabled keeps the global-stash behaviour.

        Given get_egress_pool() returns None,
        When the handshake raises InvalidStatus 429 with Retry-After=600,
        Then ``_PENDING_RETRY_AFTER_S[connector_id]`` is set to 600.
        """
        response = Response(
            status_code=429,
            reason_phrase="Too Many Requests",
            headers=Headers([("Retry-After", "600")]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        token = _CURRENT_CONNECTOR_ID.set(555555)
        try:
            with pytest.raises(InvalidStatus):
                await shim.__aenter__()
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)
        assert _PENDING_RETRY_AFTER_S[555555] == 600.0

    @pytest.mark.asyncio
    async def test_shim_429_releases_reservation_via_aenter(self) -> None:
        """Spec — 429 release path keeps in_use_count at 0 post-handshake.

        Given the pool is enabled and a route is reserved,
        When the handshake raises 429,
        Then in_use_count is back to 0 after the exception propagates.
        """
        pool = self._enable_pool()
        response = Response(
            status_code=429,
            reason_phrase="Too Many Requests",
            headers=Headers([("Retry-After", "600")]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(InvalidStatus):
            await shim.__aenter__()
        assert pool.snapshot()[0].in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_non_429_invalid_status_releases_reservation(self) -> None:
        """Spec — non-429 InvalidStatus also releases the reservation.

        Given the pool is enabled,
        When the handshake raises InvalidStatus with status_code=503,
        Then in_use_count is back to 0 AND no quarantine is recorded
        (only 429 quarantines the route).
        """
        pool = self._enable_pool()
        response = Response(
            status_code=503,
            reason_phrase="Service Unavailable",
            headers=Headers([]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(InvalidStatus):
            await shim.__aenter__()
        snap = pool.snapshot()[0]
        assert snap.in_use_count == 0
        assert snap.quarantine_until is None

    @pytest.mark.asyncio
    async def test_shim_connect_timeout_quarantines_proxy_route(self) -> None:
        """Spec — a WS connect timeout quarantines the borrowed SOCKS route.

        Given the pool has a SOCKS5 route reserved (direct quarantined so
            socks wins selection),
        When the handshake raises TimeoutError (a blackholed/timed-out tunnel),
        Then the SOCKS route is quarantined for ~_WS_CONNECT_ERROR_QUARANTINE_S
            and its in_use_count returns to 0, so the next reconnect fails over
            instead of re-dialing the dead tunnel.
        """
        pool = self._enable_pool(with_socks5=True)
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        async def fake_aenter(_: Any) -> Any:
            raise TimeoutError("handshake timed out")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        before = datetime.now(UTC)
        with pytest.raises(TimeoutError):
            await shim.__aenter__()
        socks = next(s for s in pool.snapshot() if s.id == "wg-uk-1")
        assert socks.quarantine_until is not None
        held_s = (socks.quarantine_until - before).total_seconds()
        assert 25.0 <= held_s <= 31.0
        assert socks.in_use_count == 0
        assert socks.last_close_1015_at is None

    @pytest.mark.asyncio
    async def test_shim_proxy_error_quarantines_proxy_route(self) -> None:
        """Spec — a websockets ProxyError quarantines the SOCKS route.

        Given the pool has a SOCKS5 route reserved,
        When the handshake raises ``websockets.exceptions.ProxyError`` — the
            InvalidHandshake subclass (NOT an OSError) that websockets-16 wraps
            every non-OSError SOCKS connect failure into, and therefore the type
            actually reaching the shim,
        Then the SOCKS route is quarantined — proving the connect-error handler
            catches the real exposed ProxyError, not only OSError/TimeoutError.
        """
        pool = self._enable_pool(with_socks5=True)
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        async def fake_aenter(_: Any) -> Any:
            raise ProxyError("failed to connect to SOCKS proxy")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(ProxyError):
            await shim.__aenter__()
        socks = next(s for s in pool.snapshot() if s.id == "wg-uk-1")
        assert socks.quarantine_until is not None
        assert socks.in_use_count == 0
        assert socks.last_close_1015_at is None

    @pytest.mark.asyncio
    async def test_shim_cancelled_during_aenter_releases_without_quarantine(self) -> None:
        """Spec — a cancellation during the handshake never quarantines.

        Given the pool has a SOCKS5 route reserved,
        When the handshake is cancelled (process shutdown),
        Then the reservation is released and the SOCKS route is NOT quarantined —
            ``CancelledError`` is a ``BaseException`` that bypasses the
            connect-error clause and only hits the catch-all release path, so a
            clean shutdown does not penalize an otherwise-healthy route.
        """
        pool = self._enable_pool(with_socks5=True)
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        async def fake_aenter(_: Any) -> Any:
            raise asyncio.CancelledError()

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(asyncio.CancelledError):
            await shim.__aenter__()
        socks = next(s for s in pool.snapshot() if s.id == "wg-uk-1")
        assert socks.quarantine_until is None
        assert socks.in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_connect_error_on_direct_route_does_not_quarantine(self) -> None:
        """Spec — a connect error on a DIRECT route does not quarantine it.

        Given the pool has only the direct route reserved,
        When the handshake raises ConnectionRefusedError,
        Then the direct route is NOT quarantined (a no-proxy connect error means
            the exchange or local uplink is down, and the pool's only fallback is
            that same direct route) but the reservation is still released.
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            raise ConnectionRefusedError("connection refused")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(ConnectionRefusedError):
            await shim.__aenter__()
        snap = pool.snapshot()[0]
        assert snap.quarantine_until is None
        assert snap.in_use_count == 0

    @pytest.mark.asyncio
    async def test_private_direct_connect_error_quarantines_direct_briefly(self) -> None:
        """Spec — private direct connect errors trigger PL fallback.

        Given the pool has direct plus a configured PL private fallback,
        When a private direct handshake raises a connect error,
        Then direct is briefly quarantined and the next private reserve
            selects PL.
        """
        config = EgressPoolConfig(
            enabled=True,
            private_fallback_route_id="pl",
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://snapper-egress:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        pool = configure_egress_pool(config)
        assert pool is not None

        async def fake_aenter(_: Any) -> Any:
            raise ConnectionRefusedError("connection refused")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        with egress_identity(
            exchange="kraken",
            traffic_class="private",
            owner="executor",
            operation="order_ws",
        ):
            shim_cls = _wrap_connect_factory(fake_connect)
            shim = shim_cls("wss://kraken")
            before = datetime.now(UTC)
            with pytest.raises(ConnectionRefusedError):
                await shim.__aenter__()
            direct = next(s for s in pool.snapshot() if s.id == "default")
            assert direct.quarantine_until is not None
            held_s = (direct.quarantine_until - before).total_seconds()
            assert (
                _PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S - 2.0
                <= held_s
                <= (_PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S + 1.0)
            )
            assert direct.in_use_count == 0
            next_reservation = pool.reserve(
                exchange="kraken",
                purpose="websocket",
                traffic_class="private",
            )
            assert next_reservation.route_id == "pl"
            next_reservation.release()

    @pytest.mark.asyncio
    async def test_shim_connect_error_pool_disabled_just_propagates(self) -> None:
        """Spec — a connect error with no pool has nothing to quarantine.

        Given get_egress_pool() returns None (no reservation taken),
        When the handshake raises a connect error,
        Then it propagates unchanged with no quarantine attempt.
        """

        async def fake_aenter(_: Any) -> Any:
            raise TimeoutError("handshake timed out")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(TimeoutError):
            await shim.__aenter__()

    @pytest.mark.asyncio
    async def test_shim_generic_error_pool_disabled_just_propagates(self) -> None:
        """Spec — a generic error with no pool propagates via the catch-all.

        Given get_egress_pool() returns None (no reservation taken),
        When the handshake raises a generic non-connect exception,
        Then it propagates through the catch-all with no reservation to release.
        """

        async def fake_aenter(_: Any) -> Any:
            raise RuntimeError("unexpected handshake bug")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(RuntimeError):
            await shim.__aenter__()

    @pytest.mark.asyncio
    async def test_shim_non_invalid_status_exception_releases(self) -> None:
        """Spec — a generic exception in __aenter__ releases the reservation.

        Given the pool is enabled,
        When the handshake raises a generic exception that is neither an
            InvalidStatus 429 nor a connect-level OSError/ProxyError,
        Then in_use_count is back to 0 (the catch-all release path; connect
            errors are covered separately and do quarantine).
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            raise RuntimeError("unexpected handshake bug")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(RuntimeError):
            await shim.__aenter__()
        assert pool.snapshot()[0].in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_clean_handshake_releases_on_aexit(self) -> None:
        """Spec — successful enter + exit releases via __aexit__ finally.

        Given the pool is enabled and the handshake succeeds,
        When the caller's async-with body exits cleanly,
        Then in_use_count is back to 0 after __aexit__.
        """
        pool = self._enable_pool()
        ws_mock = MagicMock()

        async def fake_aenter(_: Any) -> Any:
            return ws_mock

        async def fake_aexit(_self: Any, *_args: Any) -> None:
            return None

        class FakeCm:
            __aenter__ = fake_aenter
            __aexit__ = fake_aexit

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        ws = await shim.__aenter__()
        assert ws is ws_mock
        await shim.__aexit__(None, None, None)
        assert pool.snapshot()[0].in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_close_1015_quarantines_route_on_aexit(self) -> None:
        """Spec — ConnectionClosed(rcvd.code=1015) quarantines the route.

        Given the pool is enabled and the handshake succeeded,
        When the async-with body exits with a ConnectionClosedError
        carrying rcvd.code=1015 (Cloudflare close-after-handshake),
        Then the route is quarantined for ``_CLOSE_1015_QUARANTINE_S``
        seconds AND last_close_1015_at is recorded.
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            return MagicMock()

        async def fake_aexit(_self: Any, *_args: Any) -> None:
            return None

        class FakeCm:
            __aenter__ = fake_aenter
            __aexit__ = fake_aexit

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        await shim.__aenter__()
        close = Close(code=1015, reason="rate-limited")
        exc = ConnectionClosedError(rcvd=close, sent=None)
        await shim.__aexit__(type(exc), exc, exc.__traceback__)
        snap = pool.snapshot()[0]
        assert snap.quarantine_until is not None
        assert snap.last_close_1015_at is not None
        assert snap.last_handshake_429_at is None
        assert snap.in_use_count == 0

    @pytest.mark.asyncio
    async def test_shim_close_1012_does_not_quarantine_route(self) -> None:
        """Spec — close code 1012 leaves the route healthy.

        Given the pool is enabled,
        When the async-with body exits with ConnectionClosed(rcvd.code=1012),
        Then quarantine is NOT applied (1012 is Kraken graceful
        restart; the per-close-code backoff owns the recovery).
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            return MagicMock()

        async def fake_aexit(_self: Any, *_args: Any) -> None:
            return None

        class FakeCm:
            __aenter__ = fake_aenter
            __aexit__ = fake_aexit

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        await shim.__aenter__()
        close = Close(code=1012, reason="restart")
        exc = ConnectionClosedError(rcvd=close, sent=None)
        await shim.__aexit__(type(exc), exc, exc.__traceback__)
        snap = pool.snapshot()[0]
        assert snap.quarantine_until is None
        assert snap.last_close_1015_at is None

    @pytest.mark.asyncio
    async def test_shim_clean_close_does_not_quarantine_route(self) -> None:
        """Spec — exit with no exception leaves the route healthy.

        Given the pool is enabled,
        When the async-with body exits normally,
        Then no quarantine is applied.
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            return MagicMock()

        async def fake_aexit(_self: Any, *_args: Any) -> None:
            return None

        class FakeCm:
            __aenter__ = fake_aenter
            __aexit__ = fake_aexit

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        await shim.__aenter__()
        await shim.__aexit__(None, None, None)
        snap = pool.snapshot()[0]
        assert snap.quarantine_until is None

    def test_shim_uses_publisher_exchange_name_via_context_var(self) -> None:
        """Spec — when ``_CURRENT_PUBLISHER`` is set, its ``_get_exchange_name()`` becomes the reservation tag.

        Given the pool has a SOCKS5 route pinned to ``allowed_exchanges=("kraken_equities",)``,
        And the only other route (``default``, direct) is quarantined,
        And ``_CURRENT_PUBLISHER`` is set to a publisher whose
        ``_get_exchange_name()`` returns ``"kraken_equities"``,
        When the shim runs,
        Then the SOCKS5 route is picked (the allow-list permits it)
        and the proxy URL is injected.

        This proves the connect-shim consults the publisher-scoped
        ContextVar rather than the legacy hardcoded ``"kraken"`` tag,
        which would have been rejected by the allow-list.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=0),
            RouteConfig(
                id="wg-us-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1085",
                priority=10,
                allowed_exchanges=("kraken_equities",),
            ),
        ]
        config = EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        pool = configure_egress_pool(config)
        assert pool is not None
        pool._quarantine_route(
            "default",
            datetime.now(UTC) + timedelta(seconds=600),
            "http-429",
        )

        mock_publisher = MagicMock()
        mock_publisher._get_exchange_name.return_value = "kraken_equities"
        token = _CURRENT_PUBLISHER.set(mock_publisher)
        try:
            seen_kwargs: dict[str, Any] = {}

            def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
                seen_kwargs.update(kwargs)
                return MagicMock()

            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls("wss://kraken")
        finally:
            _CURRENT_PUBLISHER.reset(token)

        assert seen_kwargs == {
            "proxy": "socks5h://snapper-egress:1085",
            "ping_interval": _WS_PING_INTERVAL_S,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    def test_shim_falls_back_to_kraken_when_no_publisher_context(self) -> None:
        """Spec — when ``_CURRENT_PUBLISHER`` is None, reservation tag falls back to ``"kraken"``.

        Given the pool is enabled with a SOCKS5 route,
        And ``_CURRENT_PUBLISHER`` is NOT set (default ``None``),
        When the shim runs,
        Then the reservation uses the legacy ``"kraken"`` tag,
        preserving back-compat for the Spot publisher path where
        ``KrakenMarketDataPublisher.start`` sets the ContextVar
        explicitly. (Other Kraken publishers that do not set
        ``_CURRENT_PUBLISHER`` still get pool routing under the
        legacy tag.)
        """
        self._enable_pool(with_socks5=True)
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken")
        assert "proxy" in seen_kwargs


class TestPhaseBPrimeGetReconnectWait:
    """Egress-pool precedence tests for ``_patched_get_reconnect_wait``.

    The design splits behaviour on ``get_egress_pool()``:

    * Pool=None — global-stash path: Retry-After stash → close-code → SDK exponential.
    * Pool enabled — close-code → pool wait → SDK exponential. The
      legacy Retry-After stash is intentionally NOT consulted; the
      shim routes 429 captures to ``reservation.quarantine`` instead.
    """

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        """Clear pool + stashes between tests."""
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()
        yield
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()

    @staticmethod
    def _enable_pool(*, with_socks5: bool = False) -> Any:
        """Helper matching the pool-enabled shim setup."""
        routes = [RouteConfig(id="default", kind="direct", priority=0)]
        if with_socks5:
            routes.append(
                RouteConfig(
                    id="wg-uk-1",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                )
            )
        config = EgressPoolConfig(enabled=True, routes=routes)
        return configure_egress_pool(config)

    def test_pool_disabled_honors_retry_after_stash(self) -> None:
        """Spec — pool disabled + stash present → return stashed value.

        Given pool is None and ``_PENDING_RETRY_AFTER_S`` holds 600,
        When _patched_get_reconnect_wait runs,
        Then it returns 600.0 and pops the stash.
        """
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        connector_id = id(connector)
        _PENDING_RETRY_AFTER_S[connector_id] = 600.0
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == 600.0
        assert connector_id not in _PENDING_RETRY_AFTER_S

    def test_pool_enabled_ignores_retry_after_stash(self) -> None:
        """Spec — pool enabled + stash present → pool wins (close-code-first then pool).

        Given pool is enabled and ``_PENDING_RETRY_AFTER_S`` somehow
        holds 600 (legacy state from a pre-pool reconnect),
        When _patched_get_reconnect_wait runs and the pool has a
        healthy route,
        Then it returns ``_RETRY_AFTER_MIN_SECONDS`` (1.0) and the
        legacy stash is NOT consumed.
        """
        self._enable_pool()
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        connector_id = id(connector)
        _PENDING_RETRY_AFTER_S[connector_id] = 600.0
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == _RETRY_AFTER_MIN_SECONDS
        assert _PENDING_RETRY_AFTER_S[connector_id] == 600.0

    def test_pool_enabled_with_close_code_still_takes_precedence(self) -> None:
        """Spec — close-code stash beats pool wait when pool is enabled.

        Given pool is enabled AND ``_LAST_CLOSE_CODE`` has 1012,
        When _patched_get_reconnect_wait runs,
        Then it returns 30.0 (the 1012 backoff) and pops the stash.
        """
        self._enable_pool()
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        connector_id = id(connector)
        _LAST_CLOSE_CODE[connector_id] = 1012
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == _CLOSE_CODE_BACKOFF_S[1012]
        assert connector_id not in _LAST_CLOSE_CODE

    def test_pool_enabled_any_route_healthy_returns_min(self) -> None:
        """Spec — pool with at least one healthy route returns 1.0.

        Given pool is enabled with a healthy direct route,
        When _patched_get_reconnect_wait runs with no stashes,
        Then it returns ``_RETRY_AFTER_MIN_SECONDS``.
        """
        self._enable_pool()
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == _RETRY_AFTER_MIN_SECONDS

    def test_pool_uses_registered_publisher_exchange(self) -> None:
        """Spec — pool reconnect-wait queries the connector's own exchange.

        Given a connector registered to a publisher whose exchange is
        ``kraken_equities`` and an enabled pool,
        When _patched_get_reconnect_wait consults the pool,
        Then the pool is queried for the connector's exchange route set, not
        the hardcoded Spot ``kraken`` set — so an Equities/direct connector
        cannot make reconnect decisions against Spot's quarantine state.
        """
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        connector_id = id(connector)
        publisher = MagicMock()
        publisher._get_exchange_name.return_value = "kraken_equities"
        _CONNECTOR_PUBLISHERS[connector_id] = publisher
        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.has_available.return_value = True
        try:
            with patch(
                "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
                return_value=fake_pool,
            ):
                wait = _patched_get_reconnect_wait(connector, 1)
            assert wait == _RETRY_AFTER_MIN_SECONDS
            fake_pool.has_available.assert_called_once_with(
                exchange="kraken_equities",
                traffic_class="public",
            )
        finally:
            _CONNECTOR_PUBLISHERS.pop(connector_id, None)

    def test_pool_reconnect_wait_uses_explicit_private_identity(self) -> None:
        """Spec — reconnect wait passes private traffic class to the pool.

        Given an explicit private executor identity and an enabled pool,
        When _patched_get_reconnect_wait consults route availability,
        Then it queries both availability and release time with
            ``traffic_class="private"``.
        """
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        fake_pool = MagicMock()
        fake_pool.size.return_value = 1
        fake_pool.has_available.return_value = False
        fake_pool.earliest_release_in_seconds.return_value = 12.5
        with patch(
            "snapper.infrastructure.exchanges.kraken_sdk_patches.get_egress_pool",
            return_value=fake_pool,
        ), egress_identity(
            exchange="kraken",
            traffic_class="private",
            owner="executor",
            operation="order_ws",
        ):
            wait = _patched_get_reconnect_wait(connector, 1)

        assert wait == 12.5
        fake_pool.has_available.assert_called_once_with(
            exchange="kraken",
            traffic_class="private",
        )
        fake_pool.earliest_release_in_seconds.assert_called_once_with(
            exchange="kraken",
            traffic_class="private",
        )

    def test_pool_enabled_all_quarantined_returns_earliest_release(self) -> None:
        """Spec — all routes quarantined returns earliest release deadline.

        Given pool is enabled and the direct route is quarantined for 600 s,
        When _patched_get_reconnect_wait runs,
        Then it returns approximately 600 (allow ±5 s for test latency).
        """
        pool = self._enable_pool()
        pool._quarantine_route(
            "default",
            datetime.now(UTC) + timedelta(seconds=600),
            "http-429",
        )
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        wait = _patched_get_reconnect_wait(connector, 1)
        assert 595.0 <= wait <= 600.5

    def test_pool_enabled_all_quarantined_clamps_to_min(self) -> None:
        """Spec — earliest release < 1.0 is clamped to ``_RETRY_AFTER_MIN_SECONDS``.

        Given pool with a quarantine deadline already passed,
        When _patched_get_reconnect_wait runs,
        Then the return value is at least ``_RETRY_AFTER_MIN_SECONDS``
        so the SDK does not no-op-sleep.
        """
        pool = self._enable_pool()
        pool._quarantine_route(
            "default",
            datetime.now(UTC) - timedelta(seconds=10),
            "http-429",
        )
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == _RETRY_AFTER_MIN_SECONDS

    def test_pool_empty_falls_through_to_sdk_exponential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — pool size==0 falls through to SDK exponential.

        Given an EgressPool configured with no routes (defensive — the
        Pydantic validator forbids this, but the runtime branch must
        also be safe),
        When _patched_get_reconnect_wait runs,
        Then the SDK exponential is invoked.
        """
        configure_egress_pool(EgressPoolConfig(enabled=False, routes=[]))
        captured = MagicMock(return_value=42.5)
        monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_GET_RECONNECT_WAIT", captured)
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        wait = _patched_get_reconnect_wait(connector, 3)
        assert wait == 42.5

    def test_429_failover_in_one_second_when_healthy_route_available(self) -> None:
        """Spec — egress-pool fast-failover acceptance test.

        Given pool enabled with direct (priority=0) + socks5 (priority=10),
        When the shim drives a 429 with Retry-After=600 on direct,
        Then:
          (a) direct.quarantine_until is set ~now+600s,
          (b) ``_PENDING_RETRY_AFTER_S`` is NOT populated,
          (c) ``_patched_get_reconnect_wait`` returns ~1.0 (not 600),
          (d) the next reservation picks the socks5 route.

        This is the central egress-pool guarantee: a route-scoped 429
        produces fast failover, not the legacy full-Retry-After wait.
        """
        pool = self._enable_pool(with_socks5=True)
        response = Response(
            status_code=429,
            reason_phrase="Too Many Requests",
            headers=Headers([("Retry-After", "600")]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        token = _CURRENT_CONNECTOR_ID.set(999999)
        try:
            asyncio.run(_raise_through_aenter(shim))
        finally:
            _CURRENT_CONNECTOR_ID.reset(token)
        direct_snap = next(s for s in pool.snapshot() if s.id == "default")
        assert direct_snap.quarantine_until is not None
        assert direct_snap.quarantine_until > datetime.now(UTC) + timedelta(seconds=590)
        assert 999999 not in _PENDING_RETRY_AFTER_S
        connector = MagicMock(spec=ConnectSpotWebsocketBase)
        wait = _patched_get_reconnect_wait(connector, 1)
        assert wait == _RETRY_AFTER_MIN_SECONDS
        next_reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert next_reservation.route_id == "wg-uk-1"
        next_reservation.release()


async def _raise_through_aenter(shim: Any) -> None:
    """Helper — drive shim.__aenter__ and expect InvalidStatus to propagate."""
    with contextlib.suppress(InvalidStatus):
        await shim.__aenter__()


class TestPhaseBPrimeBranchCoverage:
    """Branch-coverage tests for shim + reconnect_wait defensive paths.

    These pin behaviour for code paths the design must support but
    that don't fire on the happy paths: pool-disabled exceptions
    (no-reservation release branches), missing connector ContextVar
    in the legacy 429 stash, and the empty-pool fallback in
    _patched_get_reconnect_wait.
    """

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        """Clear pool + stashes between tests."""
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()
        yield
        reset_egress_pool()
        _PENDING_RETRY_AFTER_S.clear()
        _LAST_CLOSE_CODE.clear()

    def test_shim_sync_construct_failure_with_no_pool(self) -> None:
        """Spec — pool disabled + sync raise propagates without reservation work.

        Given pool is None and original_connect raises in __init__,
        When the shim is constructed,
        Then the exception propagates without calling
        ``reservation.release`` (no reservation exists). Pinned to
        cover the branch where ``self._reservation is None`` at the
        sync-raise site.
        """

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            raise RuntimeError("simulated sync raise")

        shim_cls = _wrap_connect_factory(fake_connect)
        with pytest.raises(RuntimeError, match="simulated sync raise"):
            shim_cls("wss://kraken")

    @pytest.mark.asyncio
    async def test_shim_aenter_exception_with_no_pool(self) -> None:
        """Spec — pool disabled + arbitrary exception in __aenter__ propagates.

        Given pool is None and the handshake raises OSError,
        When shim.__aenter__ is awaited,
        Then the exception propagates. Pinned to cover the branch
        where ``self._reservation is None`` at the
        non-InvalidStatus exception site.
        """

        async def fake_aenter(_: Any) -> Any:
            raise OSError("net down")

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        with pytest.raises(OSError):
            await shim.__aenter__()

    @pytest.mark.asyncio
    async def test_shim_429_legacy_stash_with_no_connector_var(self) -> None:
        """Spec — pool disabled + 429 + missing connector id is a no-op.

        Given pool is None, the handshake raises 429 with valid
        Retry-After, but ``_CURRENT_CONNECTOR_ID`` is unset,
        When _handle_handshake_429 runs,
        Then ``_PENDING_RETRY_AFTER_S`` stays empty (defensive — the
        SDK's ``__run`` normally stamps the ContextVar, but a stale
        path could miss it).
        """
        response = Response(
            status_code=429,
            reason_phrase="Too Many Requests",
            headers=Headers([("Retry-After", "600")]),
            body=b"",
        )
        exc = InvalidStatus(response=response)

        async def fake_aenter(_: Any) -> Any:
            raise exc

        class FakeCm:
            __aenter__ = fake_aenter

            async def __aexit__(self, *_: Any) -> None:
                return None

        def fake_connect(*args: Any, **kwargs: Any) -> FakeCm:
            return FakeCm()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim = shim_cls("wss://kraken")
        assert _CURRENT_CONNECTOR_ID.get() is None
        with pytest.raises(InvalidStatus):
            await shim.__aenter__()
        assert _PENDING_RETRY_AFTER_S == {}

    def test_get_reconnect_wait_pool_enabled_but_no_routes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — pool enabled with size==0 falls through to SDK exponential.

        Given a pool object whose ``size()`` returns 0 (defensive —
        the Pydantic validator prevents this for enabled=True configs,
        but the branch must still be safe),
        When _patched_get_reconnect_wait runs,
        Then ``has_available()`` returns False AND
        ``earliest_release_in_seconds()`` returns None, so the
        function falls through to SDK exponential.
        """
        empty_pool = EgressPool(EgressPoolConfig(enabled=False, routes=[]))
        _POOL_HOLDER[0] = empty_pool
        try:
            captured = MagicMock(return_value=17.5)
            monkeypatch.setattr(kraken_sdk_patches, "_ORIGINAL_GET_RECONNECT_WAIT", captured)
            connector = MagicMock(spec=ConnectSpotWebsocketBase)
            wait = _patched_get_reconnect_wait(connector, 2)
        finally:
            from snapper.infrastructure.network.egress_pool import reset_egress_pool

            reset_egress_pool()
        assert wait == 17.5


class TestApplyKrakenAlreadySubscribedFilter:
    """Installation of the Already-subscribed filter must be safe to call repeatedly."""

    def test_apply_rebinds_manage_subscriptions(self) -> None:
        """Spec — first call installs the rebind.

        Given the filter has not been applied yet (or already True from a
            previous test — the rebind is idempotent),
        When ``apply_kraken_already_subscribed_filter`` is called,
        Then ``_ALREADY_SUBSCRIBED_PATCH_APPLIED[0]`` is True AND
            ``ConnectSpotWebsocket._manage_subscriptions`` is
            :func:`_patched_manage_subscriptions`.
        """
        apply_kraken_already_subscribed_filter()
        assert _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] is True
        assert ConnectSpotWebsocket._manage_subscriptions is _patched_manage_subscriptions

    def test_second_apply_is_noop(self) -> None:
        """Spec — second call is a no-op.

        Given the filter is already installed,
        When ``apply_kraken_already_subscribed_filter`` is called again,
        Then the flag stays True and the bound method is unchanged.
        """
        apply_kraken_already_subscribed_filter()
        before = ConnectSpotWebsocket._manage_subscriptions
        apply_kraken_already_subscribed_filter()
        assert _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] is True
        assert ConnectSpotWebsocket._manage_subscriptions is before


class TestPatchedManageSubscriptions:
    """``_patched_manage_subscriptions`` must downgrade benign races but keep real errors visible."""

    def test_already_subscribed_logged_as_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — benign race is downgraded.

        Given an SDK subscribe response carrying ``error == 'Already subscribed'``,
        When ``_patched_manage_subscriptions`` is invoked,
        Then the message is emitted at DEBUG (not WARNING) and the SDK's
            private ``__append_subscription`` is NOT called (no result to append).
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        message: dict[str, Any] = {
            "method": "subscribe",
            "success": False,
            "error": "Already subscribed",
            "symbol": "BTC/USD",
        }
        handler_id = _logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            _patched_manage_subscriptions(connector, message)
        finally:
            _logger.remove(handler_id)
        assert any(
            r.levelname == "DEBUG" and "already subscribed" in r.getMessage().lower()
            for r in caplog.records
        )
        assert not any(r.levelname == "WARNING" for r in caplog.records)

    def test_other_subscribe_error_logged_as_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Spec — genuine subscribe failure stays visible.

        Given an SDK subscribe response carrying a non-``Already subscribed`` error
            (e.g. ``Invalid arguments``),
        When ``_patched_manage_subscriptions`` is invoked,
        Then the message is emitted at WARNING so operators see real failures.
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        message: dict[str, Any] = {
            "method": "subscribe",
            "success": False,
            "error": "Invalid arguments",
            "symbol": "BAD/SYM",
        }
        handler_id = _logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            _patched_manage_subscriptions(connector, message)
        finally:
            _logger.remove(handler_id)
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_subscribe_error_log_redacts_token_payload(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Spec — failed subscribe payload logging never exposes tokens.

        Given an SDK subscribe failure with a token key and a token-shaped
        value echoed inside the error string,
        When ``_patched_manage_subscriptions`` logs the failure,
        Then neither sensitive value appears in the emitted record.
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        known_token = "short-active-token"
        token_like_value = "S" * 43
        message: JsonObject = {
            "method": "subscribe",
            "success": False,
            "error": f"Invalid token {known_token} echoed {token_like_value}",
            "params": {
                "token": known_token,
                "symbol": ["CLM6.NYMEX"],
            },
        }
        handler_id = _logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            _patched_manage_subscriptions(connector, message)
        finally:
            _logger.remove(handler_id)
        logged = "\n".join(record.message for record in caplog.records)
        assert known_token not in logged
        assert token_like_value not in logged
        assert "***REDACTED***" in logged

    def test_redact_ws_log_payload_handles_tuple_values(self) -> None:
        """Spec — SDK payload redaction recurses through tuple values."""
        known_token = "tuple-active-token"
        payload = ({"token": known_token}, f"echoed {known_token}")

        result = kraken_sdk_patches._redact_ws_log_payload(payload)

        rendered = str(result)
        assert known_token not in rendered
        assert "***REDACTED***" in rendered

    def test_redact_ws_log_payload_masks_token_query_params(self) -> None:
        """Spec — SDK payload redaction masks sensitive query-parameter values."""
        query_token = "query-token"
        api_key = "query-api-key"
        api_secret = "query-api-secret"
        payload: JsonObject = {
            "method": "subscribe",
            "success": False,
            "error": (
                f"failed wss://example.test/ws?token={query_token}&api_key={api_key}"
                f"&api_secret={api_secret}&x=1"
            ),
        }

        result = kraken_sdk_patches._redact_ws_log_payload(payload)

        rendered = str(result)
        assert query_token not in rendered
        assert api_key not in rendered
        assert api_secret not in rendered
        assert "token=***REDACTED***" in rendered
        assert "api_key=***REDACTED***" in rendered
        assert "api_secret=***REDACTED***" in rendered

    def test_sdk_raw_debug_log_filter_redacts_token_payload(self) -> None:
        """Spec — SDK raw run-loop DEBUG records are redacted before handlers format them.

        ``apply_kraken_ws_teardown_hardening`` installs the redaction filter on the
        SDK connector logger, and that filter masks token/api_key/api_secret material
        in both nested mapping values and free-text fields of one record. The filter
        is exercised directly on a constructed record rather than through a live
        ``logger.debug`` emission, which is fragile under the global logging state
        (``logging.disable`` / ``Logger.disabled``) other suite tests can leave behind.
        """
        known_token = "raw-debug-token"
        api_key = "raw-api-key"
        api_secret = "raw-api-secret"
        kraken_sdk_patches.apply_kraken_ws_teardown_hardening()
        sdk_logger = kraken_sdk_patches._kraken_connectors.LOG
        assert any(
            isinstance(existing, kraken_sdk_patches._KrakenSdkWsLogRedactionFilter)
            for existing in sdk_logger.filters
        )
        record = logging.LogRecord(
            name="kraken.spot.websocket.connectors",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg={
                "method": "subscribe",
                "params": {
                    "token": known_token,
                    "api_key": api_key,
                    "api_secret": api_secret,
                },
                "error": (f"Invalid token {known_token} api_key={api_key} api_secret={api_secret}"),
            },
            args=None,
            exc_info=None,
        )
        log_filter = kraken_sdk_patches._KrakenSdkWsLogRedactionFilter()

        assert log_filter.filter(record) is True
        rendered = record.getMessage()
        assert known_token not in rendered
        assert api_key not in rendered
        assert api_secret not in rendered
        assert "***REDACTED***" in rendered

    def test_sdk_raw_debug_log_filter_handles_record_without_tuple_args(self) -> None:
        """Spec — SDK log redaction also handles records without positional args."""
        known_token = "record-token"
        record = logging.LogRecord(
            name="kraken.spot.websocket.connectors",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg={"params": {"token": known_token}},
            args=None,
            exc_info=None,
        )
        log_filter = kraken_sdk_patches._KrakenSdkWsLogRedactionFilter()

        assert log_filter.filter(record) is True
        rendered = record.getMessage()
        assert known_token not in rendered
        assert "***REDACTED***" in rendered

    def test_sdk_raw_debug_log_filter_redacts_mapping_args(self) -> None:
        """Spec — SDK log redaction handles mapping-style format args."""
        api_key = "mapping-api-key"
        api_secret = "mapping-api-secret"
        record = logging.LogRecord(
            name="kraken.spot.websocket.connectors",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg="api_key=%(api_key)s api_secret=%(api_secret)s plain=%(plain)s",
            args={"api_key": api_key, "api_secret": api_secret, "plain": "visible"},
            exc_info=None,
        )
        log_filter = kraken_sdk_patches._KrakenSdkWsLogRedactionFilter()

        assert log_filter.filter(record) is True
        rendered = record.getMessage()
        assert api_key not in rendered
        assert api_secret not in rendered
        assert "plain=visible" in rendered
        assert "***REDACTED***" in rendered

    def test_sdk_raw_debug_log_filter_redacts_tuple_args(self) -> None:
        """Spec — SDK log redaction masks secrets in tuple-style positional args.

        Uses a two-element ``args`` tuple so ``LogRecord`` keeps it as a tuple:
        a single-element tuple whose only item is a mapping is auto-unwrapped to
        that mapping, which would route through the mapping branch instead.
        """
        known_token = "tuple-token"
        record = logging.LogRecord(
            name="kraken.spot.websocket.connectors",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg="subscribe rejected: %s %s",
            args=({"params": {"token": known_token}}, "context"),
            exc_info=None,
        )
        log_filter = kraken_sdk_patches._KrakenSdkWsLogRedactionFilter()

        assert log_filter.filter(record) is True
        assert isinstance(record.args, tuple)
        rendered = str(record.args)
        assert known_token not in rendered
        assert "***REDACTED***" in rendered

    def test_successful_subscribe_appends_subscription(self) -> None:
        """Spec — successful subscribe still delegates to SDK helpers.

        Given an SDK subscribe response with ``success == True`` and a ``result`` block,
        When ``_patched_manage_subscriptions`` is invoked,
        Then ``__transform_subscription`` is called with the raw message AND
            ``__append_subscription`` is called with the transformed ``result``.
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        transformed_result = {"channel": "ticker", "symbol": ["BTC/USD"]}
        connector._ConnectSpotWebsocket__transform_subscription = MagicMock(
            return_value={"result": transformed_result}
        )
        connector._ConnectSpotWebsocket__append_subscription = MagicMock()
        message: dict[str, Any] = {
            "method": "subscribe",
            "success": True,
            "result": {"channel": "ticker", "symbol": "BTC/USD"},
        }
        _patched_manage_subscriptions(connector, message)
        connector._ConnectSpotWebsocket__transform_subscription.assert_called_once_with(
            subscription=message
        )
        connector._ConnectSpotWebsocket__append_subscription.assert_called_once_with(
            subscription=transformed_result
        )

    def test_successful_unsubscribe_removes_subscription(self) -> None:
        """Spec — successful unsubscribe still delegates to SDK helpers.

        Given an SDK unsubscribe response with ``success == True`` and a ``result`` block,
        When ``_patched_manage_subscriptions`` is invoked,
        Then ``__transform_subscription`` is called AND
            ``__remove_subscription`` is called with the transformed ``result``.
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        transformed_result = {"channel": "ticker", "symbol": ["BTC/USD"]}
        connector._ConnectSpotWebsocket__transform_subscription = MagicMock(
            return_value={"result": transformed_result}
        )
        connector._ConnectSpotWebsocket__remove_subscription = MagicMock()
        message: dict[str, Any] = {
            "method": "unsubscribe",
            "success": True,
            "result": {"channel": "ticker", "symbol": "BTC/USD"},
        }
        _patched_manage_subscriptions(connector, message)
        connector._ConnectSpotWebsocket__remove_subscription.assert_called_once_with(
            subscription=transformed_result
        )

    def test_failed_unsubscribe_logged_as_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — failed unsubscribe still emits WARNING.

        Given an SDK unsubscribe response with ``success == False``,
        When ``_patched_manage_subscriptions`` is invoked,
        Then a WARNING is emitted so operators see the failure (we have no
            equivalent benign-race path for unsubscribes today).
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        message: dict[str, Any] = {
            "method": "unsubscribe",
            "success": False,
            "error": "Not subscribed",
        }
        handler_id = _logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            _patched_manage_subscriptions(connector, message)
        finally:
            _logger.remove(handler_id)
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_unknown_method_is_ignored(self) -> None:
        """Spec — unknown / non-subscribe methods are silently passed through.

        Given an SDK message whose ``method`` is neither ``subscribe`` nor
            ``unsubscribe`` (e.g. a heartbeat, a status frame),
        When ``_patched_manage_subscriptions`` is invoked,
        Then no SDK helpers are called and no log record is emitted —
            the method is not responsible for those messages.
        """
        connector = MagicMock(spec=ConnectSpotWebsocket)
        connector._ConnectSpotWebsocket__transform_subscription = MagicMock()
        connector._ConnectSpotWebsocket__append_subscription = MagicMock()
        connector._ConnectSpotWebsocket__remove_subscription = MagicMock()
        _patched_manage_subscriptions(connector, {"method": "heartbeat"})
        connector._ConnectSpotWebsocket__transform_subscription.assert_not_called()
        connector._ConnectSpotWebsocket__append_subscription.assert_not_called()
        connector._ConnectSpotWebsocket__remove_subscription.assert_not_called()


class TestLogKrakenSdkPatchesStatus:
    """Confirmation logging is decoupled from patch installation.

    The ``apply_*`` functions can run at publisher module import time,
    BEFORE loguru's file sink is configured by ``setup_logging``. The
    ``log_kraken_sdk_patches_status`` helper bridges the gap: it gates
    emission on :func:`snapper.utils.logging.is_file_sink_ready` so any
    invocation that happens too early no-ops, and it tracks per-patch
    ``_*_PATCH_LOGGED`` flags so the explicit hook in
    :func:`snapper.__main__.main` (and the equivalent in
    :func:`snapper.server.process_runner.main`) plus the
    ``apply_*``-internal call never emit duplicates. Tests cover the
    happy paths plus the two gating mechanisms.
    """

    @pytest.fixture(autouse=True)
    def _save_and_restore_flags(self) -> Any:
        """Snapshot and restore the three pairs of patch flags around each test.

        The flags are process-global module state that other tests may
        mutate; this fixture ensures each test in this class sees a
        clean baseline (all APPLIED + LOGGED flags False, sink not
        ready) and leaves the suite untouched on exit. ``_FILE_SINK_READY``
        from :mod:`snapper.utils.logging` is included because the gate
        in ``log_kraken_sdk_patches_status`` reads it.
        """
        saved_applied = (
            _PATCH_APPLIED[0],
            _FUTURES_PATCH_APPLIED[0],
            _ALREADY_SUBSCRIBED_PATCH_APPLIED[0],
            _RESUBSCRIBE_PACE_PATCH_APPLIED[0],
            _TEARDOWN_PATCH_APPLIED[0],
        )
        saved_logged = (
            _PATCH_LOGGED[0],
            _FUTURES_PATCH_LOGGED[0],
            _ALREADY_SUBSCRIBED_PATCH_LOGGED[0],
            _RESUBSCRIBE_PACE_PATCH_LOGGED[0],
            _TEARDOWN_PATCH_LOGGED[0],
        )
        saved_sink_ready = _FILE_SINK_READY[0]
        _PATCH_APPLIED[0] = False
        _FUTURES_PATCH_APPLIED[0] = False
        _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = False
        _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = False
        _TEARDOWN_PATCH_APPLIED[0] = False
        _PATCH_LOGGED[0] = False
        _FUTURES_PATCH_LOGGED[0] = False
        _ALREADY_SUBSCRIBED_PATCH_LOGGED[0] = False
        _RESUBSCRIBE_PACE_PATCH_LOGGED[0] = False
        _TEARDOWN_PATCH_LOGGED[0] = False
        _FILE_SINK_READY[0] = True
        yield
        _PATCH_APPLIED[0] = saved_applied[0]
        _FUTURES_PATCH_APPLIED[0] = saved_applied[1]
        _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = saved_applied[2]
        _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = saved_applied[3]
        _TEARDOWN_PATCH_APPLIED[0] = saved_applied[4]
        _PATCH_LOGGED[0] = saved_logged[0]
        _FUTURES_PATCH_LOGGED[0] = saved_logged[1]
        _ALREADY_SUBSCRIBED_PATCH_LOGGED[0] = saved_logged[2]
        _RESUBSCRIBE_PACE_PATCH_LOGGED[0] = saved_logged[3]
        _TEARDOWN_PATCH_LOGGED[0] = saved_logged[4]
        _FILE_SINK_READY[0] = saved_sink_ready

    def test_all_flags_false_emits_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — no patches active means no log records.

        Given the sink is ready and all three ``_*_PATCH_APPLIED`` flags
            are False,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then no INFO log record is emitted — the helper is a pure
            reporter; it never claims patches that are not installed.
        """
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        assert not [r for r in caplog.records if "applied" in r.getMessage()]

    def test_single_flag_emits_matching_line(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — Retry-After flag drives exactly one INFO line.

        Given the sink is ready and only ``_PATCH_APPLIED[0] = True``,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then exactly one INFO record matching
            ``"kraken-sdk Retry-After honoring applied"`` is emitted —
            independent reporting per flag, no cross-talk.
        """
        _PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        messages = [r.getMessage() for r in caplog.records]
        assert "kraken-sdk Retry-After honoring applied" in messages
        assert "kraken-sdk futures pool routing applied" not in messages
        assert "kraken-sdk Already-subscribed filter applied" not in messages
        assert _PATCH_LOGGED[0] is True

    def test_teardown_flag_emits_matching_line(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — teardown-hardening flag drives exactly one INFO line.

        Given the sink is ready and only ``_TEARDOWN_PATCH_APPLIED[0] = True``,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then exactly one INFO record matching
            ``"kraken-sdk WS teardown hardening applied"`` is emitted and the
            logged flag flips so repeats never duplicate it.
        """
        _TEARDOWN_PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        messages = [r.getMessage() for r in caplog.records]
        assert "kraken-sdk WS teardown hardening applied" in messages
        assert _TEARDOWN_PATCH_LOGGED[0] is True

    def test_resubscribe_pacing_flag_emits_line(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — the re-subscribe pacing flag drives its own INFO line.

        Given the sink is ready and only ``_RESUBSCRIBE_PACE_PATCH_APPLIED[0]``
            is True,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then exactly the pacing ``applied`` line is emitted and its LOGGED
            flag flips True, with no cross-talk to the other patches.
        """
        _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        messages = [r.getMessage() for r in caplog.records]
        assert "kraken-sdk reconnect re-subscribe pacing applied" in messages
        assert "kraken-sdk Retry-After honoring applied" not in messages
        assert _RESUBSCRIBE_PACE_PATCH_LOGGED[0] is True

    def test_all_flags_emit_three_lines_in_documented_order(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Spec — all three patches active yields three INFO records.

        Given the sink is ready and every patch flag is True,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then three INFO records are emitted in the documented order
            (Retry-After → futures pool routing → Already-subscribed),
            providing a deterministic boot-time observability snapshot
            that operators can grep without worrying about order drift,
            and all three LOGGED flags become True.
        """
        _PATCH_APPLIED[0] = True
        _FUTURES_PATCH_APPLIED[0] = True
        _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        applied_messages = [r.getMessage() for r in caplog.records if "applied" in r.getMessage()]
        assert applied_messages == [
            "kraken-sdk Retry-After honoring applied",
            "kraken-sdk futures pool routing applied",
            "kraken-sdk Already-subscribed filter applied",
        ]
        assert _PATCH_LOGGED[0] is True
        assert _FUTURES_PATCH_LOGGED[0] is True
        assert _ALREADY_SUBSCRIBED_PATCH_LOGGED[0] is True

    def test_call_does_not_mutate_applied_flags(self) -> None:
        """Spec — the helper is read-only with respect to APPLIED flags.

        Given a known starting APPLIED configuration with sink ready,
        When ``log_kraken_sdk_patches_status`` is invoked twice,
        Then the APPLIED flag values are unchanged after each call —
            the helper reports installation state but never installs
            or uninstalls patches. This protects against accidental
            coupling: the boot-time observer must not become a hidden
            patch source.
        """
        _PATCH_APPLIED[0] = True
        _FUTURES_PATCH_APPLIED[0] = False
        _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = True
        log_kraken_sdk_patches_status()
        log_kraken_sdk_patches_status()
        assert _PATCH_APPLIED[0] is True
        assert _FUTURES_PATCH_APPLIED[0] is False
        assert _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] is True

    def test_skip_when_file_sink_not_ready(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — pre-sink invocations are silent no-ops.

        Given ``_FILE_SINK_READY[0] = False`` (the state that exists
            before ``setup_logging`` runs) and all APPLIED flags True,
        When ``log_kraken_sdk_patches_status`` is invoked,
        Then NO INFO record is emitted AND no LOGGED flag is set — so
            a later post-sink invocation will still emit each line
            exactly once. This is the eager-import path: ``apply_*``
            calls at publisher module import time happen before
            ``setup_logging`` wires the file sink, and we must not let
            those early invocations consume the LOGGED budget for the
            real post-sink emission.
        """
        _FILE_SINK_READY[0] = False
        _PATCH_APPLIED[0] = True
        _FUTURES_PATCH_APPLIED[0] = True
        _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        assert not [r for r in caplog.records if "applied" in r.getMessage()]
        assert _PATCH_LOGGED[0] is False
        assert _FUTURES_PATCH_LOGGED[0] is False
        assert _ALREADY_SUBSCRIBED_PATCH_LOGGED[0] is False

    def test_double_call_does_not_duplicate(self, caplog: pytest.LogCaptureFixture) -> None:
        """Spec — LOGGED tracking suppresses duplicates across invocations.

        Given the sink is ready and one APPLIED flag is True,
        When ``log_kraken_sdk_patches_status`` is invoked twice in a row,
        Then the INFO record is emitted exactly once — the second call
            sees ``_PATCH_LOGGED[0] is True`` and skips the emission.
            This is the contract that makes it safe for both the
            ``apply_*``-internal call and the explicit post-sink hook
            in ``main()`` to invoke the function without producing two
            ``applied`` lines per patch.
        """
        _PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        applied_messages = [r.getMessage() for r in caplog.records if "applied" in r.getMessage()]
        assert applied_messages == ["kraken-sdk Retry-After honoring applied"]

    def test_pre_sink_then_post_sink_emits_exactly_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Spec — eager-then-explicit flow emits each line exactly once.

        Given the sink starts NOT ready and only Retry-After is APPLIED,
        When ``log_kraken_sdk_patches_status`` is called (simulating the
            ``apply_*``-internal call at module import time, pre-sink),
        And THEN the sink becomes ready and the helper is invoked again
            (simulating the explicit hook from
            :func:`snapper.__main__.main` after ``setup_logging``),
        Then exactly one INFO record lands in the file-bound stream and
            the LOGGED flag is set — the canonical recovery path that
            this whole decoupling exists to protect.
        """
        _FILE_SINK_READY[0] = False
        _PATCH_APPLIED[0] = True
        handler_id = _logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            log_kraken_sdk_patches_status()
            assert _PATCH_LOGGED[0] is False
            _FILE_SINK_READY[0] = True
            log_kraken_sdk_patches_status()
        finally:
            _logger.remove(handler_id)
        applied_messages = [r.getMessage() for r in caplog.records if "applied" in r.getMessage()]
        assert applied_messages == ["kraken-sdk Retry-After honoring applied"]
        assert _PATCH_LOGGED[0] is True


class TestKrakenResubscribePacing:
    """Tests for the paced reconnect re-subscribe SDK patch."""

    @staticmethod
    def _connector(subscriptions: list[Any], *, is_auth: bool = False) -> MagicMock:
        """Build a fake ConnectSpotWebsocket exposing the recover-loop surface."""
        fake = MagicMock()
        fake.is_auth = is_auth
        fake._subscriptions = subscriptions
        fake.client.subscribe = AsyncMock()
        return fake

    def test_recover_paces_subscribes_between_sends(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Recovery re-subscribes every tracked sub, sleeping between sends.

        Given: A public connector with three tracked subscriptions and a
            ready event,
        When: the patched _recover_subscriptions runs,
        Then: subscribe is awaited once per subscription in order, and sleep
            is awaited at the pacing interval between consecutive sends only
            (n-1 times).
        """
        subs: list[Any] = [
            {"channel": "ticker", "symbol": ["A"]},
            {"channel": "ticker", "symbol": ["B"]},
            {"channel": "trade", "symbol": ["C"]},
        ]
        fake = self._connector(subs)
        sleep_mock = AsyncMock()
        monkeypatch.setattr(kraken_sdk_patches.asyncio, "sleep", sleep_mock)
        event = asyncio.Event()
        event.set()
        asyncio.run(_patched_recover_subscriptions(fake, event))
        sent = [call.kwargs["params"] for call in fake.client.subscribe.await_args_list]
        assert sent == subs
        assert sleep_mock.await_count == 2
        assert sleep_mock.await_args_list[0].args == (_RESUBSCRIBE_PACE_S,)

    def test_recover_empty_cache_sends_and_sleeps_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty subscription cache is a no-op.

        Given: An authenticated connector with no tracked subscriptions,
        When: the patched _recover_subscriptions runs,
        Then: no subscribe and no sleep are awaited.
        """
        fake = self._connector([], is_auth=True)
        sleep_mock = AsyncMock()
        monkeypatch.setattr(kraken_sdk_patches.asyncio, "sleep", sleep_mock)
        event = asyncio.Event()
        event.set()
        asyncio.run(_patched_recover_subscriptions(fake, event))
        assert fake.client.subscribe.await_count == 0
        assert sleep_mock.await_count == 0

    def test_recover_single_sub_does_not_sleep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A single tracked subscription is re-sent without a trailing sleep.

        Given: A connector with exactly one tracked subscription,
        When: the patched _recover_subscriptions runs,
        Then: subscribe is awaited once and sleep is never awaited.
        """
        fake = self._connector([{"channel": "ticker", "symbol": ["A"]}])
        sleep_mock = AsyncMock()
        monkeypatch.setattr(kraken_sdk_patches.asyncio, "sleep", sleep_mock)
        event = asyncio.Event()
        event.set()
        asyncio.run(_patched_recover_subscriptions(fake, event))
        assert fake.client.subscribe.await_count == 1
        assert sleep_mock.await_count == 0

    def test_recover_snapshots_cache_before_iterating(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cache is snapshotted so a mid-sweep append is not re-sent.

        Given: A connector whose subscribe callback appends a new entry to
            the live subscription cache,
        When: the patched _recover_subscriptions runs,
        Then: only the originally-tracked subscriptions are re-sent — the
            concurrent append is not picked up by this sweep.
        """
        subs: list[Any] = [
            {"channel": "ticker", "symbol": ["A"]},
            {"channel": "trade", "symbol": ["B"]},
        ]
        fake = self._connector(subs)

        async def _append(*_args: Any, **_kwargs: Any) -> None:
            fake._subscriptions.append({"channel": "ticker", "symbol": ["LATE"]})

        fake.client.subscribe = AsyncMock(side_effect=_append)
        monkeypatch.setattr(kraken_sdk_patches.asyncio, "sleep", AsyncMock())
        event = asyncio.Event()
        event.set()
        asyncio.run(_patched_recover_subscriptions(fake, event))
        assert fake.client.subscribe.await_count == 2

    def test_apply_sets_flag_and_rebinds(self) -> None:
        """apply_kraken_resubscribe_pacing installs the patched method.

        Given: The pacing patch flag reset to False,
        When: apply_kraken_resubscribe_pacing is called,
        Then: the flag is True and ConnectSpotWebsocket._recover_subscriptions
            is the patched coroutine.
        """
        _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = False
        try:
            apply_kraken_resubscribe_pacing()
            assert _RESUBSCRIBE_PACE_PATCH_APPLIED[0] is True
            assert ConnectSpotWebsocket._recover_subscriptions is _patched_recover_subscriptions
        finally:
            _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = True

    def test_second_apply_is_noop(self) -> None:
        """A second apply is a no-op once the flag is set.

        Given: The pacing patch already installed (flag True),
        When: apply_kraken_resubscribe_pacing is called again,
        Then: it returns early, leaving the flag True.
        """
        _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = True
        apply_kraken_resubscribe_pacing()
        assert _RESUBSCRIBE_PACE_PATCH_APPLIED[0] is True

    def test_recover_waits_for_ready_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Recovery blocks on the readiness event before any re-subscribe.

        Given: A connector with one tracked subscription and an event that is
            not yet set,
        When: the patched _recover_subscriptions is scheduled and the loop
            yields once,
        Then: no subscribe is sent until the event is set, after which the
            sweep completes with exactly one send.
        """
        fake = self._connector([{"channel": "ticker", "symbol": ["A"]}])
        monkeypatch.setattr(kraken_sdk_patches.asyncio, "sleep", AsyncMock())
        event = asyncio.Event()

        async def _drive() -> int:
            task = asyncio.ensure_future(_patched_recover_subscriptions(fake, event))
            await asyncio.sleep(0)
            before_count: int = fake.client.subscribe.await_count
            event.set()
            await task
            return before_count

        before = asyncio.run(_drive())
        assert before == 0
        assert fake.client.subscribe.await_count == 1


class TestInterruptibleBackoff:
    """The hardened backoff sleeps in keep-alive-aware increments."""

    @pytest.mark.asyncio
    async def test_sleeps_full_duration_while_keep_alive(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — undisturbed backoff serves its full duration.

        Given a connector whose ``keep_alive`` stays True,
        When ``_interruptible_backoff`` runs for a short wait,
        Then it returns only after at least that duration elapsed.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_BACKOFF_POLL_S", 0.01)
        connector = SimpleNamespace(keep_alive=True)
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        await _interruptible_backoff(connector, 0.05)
        assert loop.time() - started_at >= 0.04

    @pytest.mark.asyncio
    async def test_returns_early_on_keep_alive_flip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — the #143 teardown-latency bound.

        Given a connector sleeping a long backoff,
        When ``keep_alive`` flips False mid-sleep,
        Then the backoff returns within roughly one poll interval instead of
        serving the remaining minutes.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_BACKOFF_POLL_S", 0.01)
        connector = SimpleNamespace(keep_alive=True)
        loop = asyncio.get_running_loop()

        async def _flip() -> None:
            await asyncio.sleep(0.03)
            connector.keep_alive = False

        flipper = asyncio.create_task(_flip())
        started_at = loop.time()
        await _interruptible_backoff(connector, 60.0)
        elapsed = loop.time() - started_at
        await flipper
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_zero_wait_returns_immediately(self) -> None:
        """Spec — a zero-length backoff never enters the sleep loop.

        Given a zero reconnect wait,
        When ``_interruptible_backoff`` runs,
        Then it returns without sleeping.
        """
        connector = SimpleNamespace(keep_alive=True)
        await asyncio.wait_for(_interruptible_backoff(connector, 0.0), timeout=0.5)


class TestReapReconnectChildren:
    """Child reaping cancels, drains, and consumes exceptions."""

    @pytest.mark.asyncio
    async def test_cancels_pending_children(self) -> None:
        """Spec — hanging children are cancelled and awaited.

        Given a child task stuck on an event,
        When ``_reap_reconnect_children`` runs,
        Then the child ends cancelled.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        child = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        await _reap_reconnect_children([child])
        assert child.cancelled()

    @pytest.mark.asyncio
    async def test_consumes_finished_child_exception(self) -> None:
        """Spec — finished children never leave an unretrieved exception.

        Given a child task that already failed,
        When ``_reap_reconnect_children`` runs,
        Then the exception is retrieved (no ``Task exception was never
        retrieved`` on garbage collection) and the reap returns cleanly.
        """

        async def _boom() -> None:
            raise OSError("simulated blackout")

        child = asyncio.create_task(_boom())
        await asyncio.sleep(0)
        assert child.done()
        await _reap_reconnect_children([child])
        assert isinstance(child.exception(), OSError)

    @pytest.mark.asyncio
    async def test_warns_on_uncancellable_child(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — a child that survives cancellation is logged, not awaited forever.

        Given a child that swallows the first cancellation and keeps waiting,
        When ``_reap_reconnect_children`` runs with a tiny drain bound,
        Then the reap returns (bounded) while the stubborn child is reported;
        the test then kills the child to keep the loop clean.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_CHILD_REAP_TIMEOUT_S", 0.05)

        async def _stubborn() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.Event().wait()

        child = asyncio.create_task(_stubborn())
        await asyncio.sleep(0)
        await asyncio.wait_for(_reap_reconnect_children([child]), timeout=1.0)
        assert not child.done()
        child.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await child


class TestDriveReconnectChildren:
    """The wait-loop driver tolerates externally cancelled children."""

    @pytest.mark.asyncio
    async def test_externally_cancelled_child_is_skipped(self) -> None:
        """Spec — a cancelled child is not treated as a child exception.

        Given one child cancelled externally and one completing cleanly,
        When ``_drive_reconnect_children`` runs,
        Then the loop exits via the all-done break without invoking the
        exception handler for the cancelled child.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        async def _instant() -> None:
            return None

        cancelled_child = asyncio.create_task(_hang())
        clean_child = asyncio.create_task(_instant())
        await asyncio.sleep(0)
        cancelled_child.cancel()
        handled: list[Any] = []
        connector = SimpleNamespace(keep_alive=True)
        await asyncio.wait_for(
            _drive_reconnect_children(connector, [cancelled_child, clean_child], handled.append),
            timeout=1.0,
        )
        assert handled == []

    @pytest.mark.asyncio
    async def test_cancelled_child_with_pending_sibling_loops_again(self) -> None:
        """Spec — a lone cancelled child does not end the wait loop.

        Given one child cancelled externally while its sibling keeps
        running,
        When ``_drive_reconnect_children`` observes the cancelled child,
        Then the loop waits again for the live sibling (no premature exit,
        no exception handling) and exits once the sibling completes.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        release = asyncio.Event()

        async def _until_released() -> None:
            await release.wait()

        cancelled_child = asyncio.create_task(_hang())
        live_child = asyncio.create_task(_until_released())
        await asyncio.sleep(0)
        handled: list[Any] = []
        connector = SimpleNamespace(keep_alive=True)
        driver = asyncio.create_task(
            _drive_reconnect_children(connector, [cancelled_child, live_child], handled.append)
        )
        await asyncio.sleep(0.02)
        cancelled_child.cancel()
        await asyncio.sleep(0.02)
        assert not driver.done()
        release.set()
        await asyncio.wait_for(driver, timeout=1.0)
        assert handled == []

    @pytest.mark.asyncio
    async def test_keep_alive_false_at_entry_still_reaps(self) -> None:
        """Spec — a dead connector at loop entry still drains its children.

        Given ``keep_alive`` already False when the driver starts (a teardown
        racing the end of the backoff),
        When ``_drive_reconnect_children`` runs with hanging children,
        Then the wait loop is skipped and the children are cancelled by the
        ``finally`` reap — the stock loop orphaned them on this exact path.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        first = asyncio.create_task(_hang())
        second = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        connector = SimpleNamespace(keep_alive=False)
        await asyncio.wait_for(
            _drive_reconnect_children(connector, [first, second], lambda task: None),
            timeout=1.0,
        )
        assert first.cancelled()
        assert second.cancelled()


class _FakeSession:
    """Minimal aiohttp-session stand-in with a closable flag."""

    def __init__(self, *, closed: bool = False, fail: bool = False) -> None:
        self.closed = closed
        self.fail = fail
        self.close_calls = 0

    async def close(self) -> None:
        """Record the call; optionally raise to exercise the guard branch."""
        self.close_calls += 1
        if self.fail:
            raise RuntimeError("session close boom")
        self.closed = True


class _FakeConnector:
    """Minimal connector stand-in carrying ``keep_alive`` and a run task."""

    def __init__(self, task: Any = None) -> None:
        self.keep_alive = True
        if task is not None:
            self.task = task


class _ExplodingConnector:
    """Connector whose ``keep_alive`` setter raises, for the guard branch."""

    @property
    def keep_alive(self) -> bool:
        """Always-True flag whose setter raises."""
        return True

    @keep_alive.setter
    def keep_alive(self, value: bool) -> None:
        raise RuntimeError("keep_alive boom")


class TestForceCloseWsClient:
    """Last-resort teardown cancels run tasks and closes leaked sessions."""

    @pytest.mark.asyncio
    async def test_spot_shape_cancels_task_and_closes_session(self) -> None:
        """Spec — the spot-shaped #143 leak is fully reaped.

        Given a spot-shaped client with a hanging public-connector run task
        and an open session,
        When ``force_close_ws_client`` runs,
        Then ``keep_alive`` is flipped, the run task ends cancelled, and the
        session is closed.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        run_task = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        connector = _FakeConnector(task=run_task)
        session = _FakeSession()
        client = SimpleNamespace(_pub_conn=connector, _priv_conn=None)
        client._SpotAsyncClient__session = session
        await force_close_ws_client(client)
        assert connector.keep_alive is False
        assert run_task.cancelled()
        assert session.closed is True

    @pytest.mark.asyncio
    async def test_futures_shape_cancels_task_and_closes_session(self) -> None:
        """Spec — the futures-shaped #143 leak is fully reaped.

        Given a futures-shaped client (single ``_conn`` slot, futures-mangled
        session attribute),
        When ``force_close_ws_client`` runs,
        Then the run task ends cancelled and the session is closed.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        run_task = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        connector = _FakeConnector(task=run_task)
        session = _FakeSession()
        client = SimpleNamespace(_conn=connector)
        client._FuturesAsyncClient__session = session
        await force_close_ws_client(client)
        assert run_task.cancelled()
        assert session.closed is True

    @pytest.mark.asyncio
    async def test_connector_failure_does_not_block_other_slots(self) -> None:
        """Spec — one broken connector slot cannot shield the others.

        Given a client whose public connector raises on ``keep_alive``
        assignment while the private connector holds a hanging run task,
        When ``force_close_ws_client`` runs,
        Then the failure is swallowed (logged) and the private run task is
        still cancelled.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        run_task = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        healthy = _FakeConnector(task=run_task)
        client = SimpleNamespace(_pub_conn=_ExplodingConnector(), _priv_conn=healthy)
        client._SpotAsyncClient__session = _FakeSession(closed=True)
        await force_close_ws_client(client)
        assert run_task.cancelled()

    @pytest.mark.asyncio
    async def test_non_task_and_done_task_are_skipped(self) -> None:
        """Spec — only live ``asyncio.Task`` instances are cancelled.

        Given connectors carrying a non-task attribute and an already-done
        task,
        When ``force_close_ws_client`` runs,
        Then neither is cancelled nor drained and the call returns cleanly.
        """

        async def _instant() -> None:
            return None

        done_task = asyncio.create_task(_instant())
        await asyncio.sleep(0)
        assert done_task.done()
        client = SimpleNamespace(
            _pub_conn=_FakeConnector(task=MagicMock()),
            _priv_conn=_FakeConnector(task=done_task),
        )
        client._SpotAsyncClient__session = _FakeSession(closed=True)
        await force_close_ws_client(client)
        assert not done_task.cancelled()

    @pytest.mark.asyncio
    async def test_session_close_failure_is_swallowed(self) -> None:
        """Spec — a failing session close cannot explode the teardown path.

        Given a client whose session raises on ``close()``,
        When ``force_close_ws_client`` runs,
        Then the error is swallowed (logged) and the call returns.
        """
        session = _FakeSession(fail=True)
        client = SimpleNamespace(_pub_conn=None, _priv_conn=None)
        client._SpotAsyncClient__session = session
        await force_close_ws_client(client)
        assert session.close_calls == 1

    @pytest.mark.asyncio
    async def test_closed_or_absent_session_is_skipped(self) -> None:
        """Spec — already-closed and absent sessions require no action.

        Given a client whose spot session is already closed and which has no
        futures session attribute,
        When ``force_close_ws_client`` runs,
        Then no close call is made.
        """
        session = _FakeSession(closed=True)
        client = SimpleNamespace(_pub_conn=None, _priv_conn=None)
        client._SpotAsyncClient__session = session
        await force_close_ws_client(client)
        assert session.close_calls == 0

    @pytest.mark.asyncio
    async def test_drain_consumes_exception_from_cancel_refusing_task(self) -> None:
        """Spec — a run task dying with its own error is consumed, not leaked.

        Given a run task that converts cancellation into a ``RuntimeError``
        (so it finishes done-with-exception rather than cancelled),
        When ``force_close_ws_client`` drains it,
        Then the exception is retrieved (no ``Task exception was never
        retrieved`` on garbage collection) and the session is still closed.
        """

        async def _raise_on_cancel() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise RuntimeError("died during cancellation") from None

        run_task = asyncio.create_task(_raise_on_cancel())
        await asyncio.sleep(0)
        connector = _FakeConnector(task=run_task)
        session = _FakeSession()
        client = SimpleNamespace(_pub_conn=connector, _priv_conn=None)
        client._SpotAsyncClient__session = session
        await force_close_ws_client(client)
        assert run_task.done()
        assert isinstance(run_task.exception(), RuntimeError)
        assert session.closed is True

    @pytest.mark.asyncio
    async def test_warns_on_run_task_surviving_drain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — a run task that survives cancellation is reported, not awaited forever.

        Given a run task that swallows the first cancellation,
        When ``force_close_ws_client`` runs with a tiny drain bound,
        Then the call still returns (bounded) and the session is closed; the
        test then kills the stubborn task to keep the loop clean.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_CHILD_REAP_TIMEOUT_S", 0.05)

        async def _stubborn() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.Event().wait()

        run_task = asyncio.create_task(_stubborn())
        await asyncio.sleep(0)
        connector = _FakeConnector(task=run_task)
        session = _FakeSession()
        client = SimpleNamespace(_pub_conn=connector, _priv_conn=None)
        client._SpotAsyncClient__session = session
        await asyncio.wait_for(force_close_ws_client(client), timeout=1.0)
        assert session.closed is True
        assert not run_task.done()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task

    @pytest.mark.asyncio
    async def test_caller_cancellation_mid_drain_still_closes_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — a cancelled force-close cannot reintroduce the #143 leak.

        Given a run task that survives cancellation (keeping the drain
        waiting) and a caller that cancels ``force_close_ws_client``
        mid-drain,
        When the cancellation propagates,
        Then the aiohttp session is STILL closed by the ``finally`` — without
        it, the named adversarial teardown path would leak the session this
        helper exists to stop.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_CHILD_REAP_TIMEOUT_S", 5.0)

        async def _stubborn() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.Event().wait()

        run_task = asyncio.create_task(_stubborn())
        await asyncio.sleep(0)
        connector = _FakeConnector(task=run_task)
        session = _FakeSession()
        client = SimpleNamespace(_pub_conn=connector, _priv_conn=None)
        client._SpotAsyncClient__session = session
        force_task = asyncio.create_task(force_close_ws_client(client))
        await asyncio.sleep(0.05)
        force_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await force_task
        assert session.closed is True
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task

    @pytest.mark.asyncio
    async def test_caller_cancellation_mid_session_close_still_closes(self) -> None:
        """Spec — cancellation DURING the session close cannot abort it.

        Given a session whose ``close()`` is slow (in flight when the caller
        is cancelled),
        When ``force_close_ws_client`` is cancelled mid-close,
        Then the detached close keeps running in the background and the
        session still ends closed — a plain ``await session.close()`` died
        half-way and left the session open (adversarial reviewer's repro).
        """

        class _SlowSession:
            def __init__(self) -> None:
                self.closed = False
                self.entered = asyncio.Event()
                self.release = asyncio.Event()

            async def close(self) -> None:
                self.entered.set()
                await self.release.wait()
                self.closed = True

        session = _SlowSession()
        client = SimpleNamespace(_pub_conn=None, _priv_conn=None)
        client._SpotAsyncClient__session = session
        force_task = asyncio.create_task(force_close_ws_client(client))
        await asyncio.wait_for(session.entered.wait(), timeout=1.0)
        force_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await force_task
        assert session.closed is False
        session.release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert session.closed is True

    @pytest.mark.asyncio
    async def test_background_close_failure_after_cancel_is_fully_drained(self) -> None:
        """Spec — no loop-level exception escapes the cancelled-close path.

        Given a session whose ``close()`` fails AFTER the caller was
        cancelled mid-close,
        When the background close task finishes,
        Then the failure is drained by ``_consume_task_result`` alone and the
        event loop's exception handler receives NOTHING — the earlier
        ``asyncio.shield`` shape leaked a loop-level ``exception in shielded
        future`` report on exactly this path (adversarial reviewer's repro).
        """

        class _FailingSlowSession:
            def __init__(self) -> None:
                self.closed = False
                self.entered = asyncio.Event()

            async def close(self) -> None:
                self.entered.set()
                await asyncio.sleep(0.02)
                raise RuntimeError("connector close failed after cancellation")

        contexts: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda lp, context: contexts.append(dict(context)))
        try:
            session = _FailingSlowSession()
            client = SimpleNamespace(_pub_conn=None, _priv_conn=None)
            client._SpotAsyncClient__session = session
            force_task = asyncio.create_task(force_close_ws_client(client))
            await asyncio.wait_for(session.entered.wait(), timeout=1.0)
            force_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await force_task
            await asyncio.sleep(0.05)
            gc.collect()
            assert contexts == []
        finally:
            loop.set_exception_handler(previous_handler)

    @pytest.mark.asyncio
    async def test_internally_cancelled_session_close_is_swallowed(self) -> None:
        """Spec — a self-cancelling close cannot explode the teardown path.

        Given a session whose ``close()`` raises ``CancelledError``
        internally (the close task ends cancelled without any caller
        cancellation),
        When ``force_close_ws_client`` runs,
        Then the helper logs and returns normally instead of propagating the
        cancellation to the venue teardown path.
        """

        class _SelfCancellingSession:
            closed = False

            async def close(self) -> None:
                raise asyncio.CancelledError

        client = SimpleNamespace(_pub_conn=None, _priv_conn=None)
        client._SpotAsyncClient__session = _SelfCancellingSession()
        await asyncio.wait_for(force_close_ws_client(client), timeout=1.0)

    @pytest.mark.asyncio
    async def test_consume_task_result_logs_background_failure(self) -> None:
        """Spec — a failed background close is drained, not left unretrieved.

        Given a detached task that finished with an exception,
        When ``_consume_task_result`` runs,
        Then the exception is retrieved and logged without raising.
        """

        async def _boom() -> None:
            raise RuntimeError("background close boom")

        task = asyncio.create_task(_boom())
        await asyncio.sleep(0)
        assert task.done()
        _consume_task_result(task)

    @pytest.mark.asyncio
    async def test_consume_task_result_ignores_cancelled_task(self) -> None:
        """Spec — a cancelled background close needs no draining.

        Given a detached task that ended cancelled,
        When ``_consume_task_result`` runs,
        Then it returns without touching the (unretrievable) exception.
        """

        async def _hang() -> None:
            await asyncio.Event().wait()

        task = asyncio.create_task(_hang())
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        _consume_task_result(task)

    @pytest.mark.asyncio
    async def test_end_to_end_real_spot_client_leak_is_reaped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — the full #143 scenario against a REAL ``SpotWSClient``.

        Given a real spot client whose connector hangs in a long reconnect
        backoff behind a blackout-simulating ``connect`` (so a tightly
        bounded ``close()`` times out exactly as in production),
        When the venue-layer pattern runs (bounded close → timeout →
        ``force_close_ws_client``),
        Then the aiohttp session is closed and the connector run task is
        cancelled — the pre-fix behaviour leaked both.
        """
        monkeypatch.setattr(kraken_sdk_patches, "_RECONNECT_BACKOFF_POLL_S", 5.0)

        class _BlackoutCM:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                return None

            async def __aenter__(self) -> Any:
                raise OSError("simulated blackout")

            async def __aexit__(self, *exc: object) -> bool:
                return False

        monkeypatch.setattr(
            kraken_sdk_patches._kraken_connectors,
            "connect",
            lambda *a, **k: _BlackoutCM(),
        )

        async def _callback(message: Any) -> None:
            return None

        client = SpotWSClient(callback=_callback)
        connector = client._pub_conn
        assert connector is not None
        connector._ConnectSpotWebsocketBase__get_reconnect_wait = lambda attempts: 30.0
        with contextlib.suppress(TimeoutError, Exception):
            async with asyncio.timeout(0.2):
                await client.start()
        try:
            async with asyncio.timeout(0.05):
                await client.close()
        except TimeoutError:
            await force_close_ws_client(client)
        session = getattr(client, "_SpotAsyncClient__session")
        assert session.closed is True
        run_task = getattr(connector, "task", None)
        if isinstance(run_task, asyncio.Task):
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(run_task, timeout=1.0)


class TestApplyKrakenWsTeardownHardening:
    """Installation of the teardown hardening must be idempotent."""

    def test_apply_rebinds_both_connector_classes(self) -> None:
        """Spec — first call installs both hardened reconnect loops.

        Given the teardown flag is reset,
        When ``apply_kraken_ws_teardown_hardening`` is called,
        Then the Spot class carries ``_patched_reconnect``, the Futures class
        carries ``_patched_futures_reconnect``, and the flag is True.
        """
        saved = _TEARDOWN_PATCH_APPLIED[0]
        _TEARDOWN_PATCH_APPLIED[0] = False
        try:
            apply_kraken_ws_teardown_hardening()
            assert _TEARDOWN_PATCH_APPLIED[0] is True
            assert (
                getattr(ConnectSpotWebsocketBase, "_ConnectSpotWebsocketBase__reconnect")
                is _patched_reconnect
            )
            assert (
                getattr(ConnectFuturesWebsocket, "_ConnectFuturesWebsocket__reconnect")
                is _patched_futures_reconnect
            )
        finally:
            _TEARDOWN_PATCH_APPLIED[0] = saved

    def test_second_apply_is_noop(self) -> None:
        """Spec — second call is a no-op.

        Given the hardening is already installed,
        When ``apply_kraken_ws_teardown_hardening`` is called again,
        Then the flag stays True and nothing is re-bound.
        """
        apply_kraken_ws_teardown_hardening()
        before = _TEARDOWN_PATCH_APPLIED[0]
        apply_kraken_ws_teardown_hardening()
        assert _TEARDOWN_PATCH_APPLIED[0] is True
        assert before is True
