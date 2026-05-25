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
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from kraken.spot.websocket.connectors import ConnectSpotWebsocket
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from loguru import logger as _logger
from websockets.exceptions import ConnectionClosedError
from websockets.exceptions import InvalidStatus
from websockets.frames import Close
from websockets.http11 import Headers
from websockets.http11 import Response

from snapper.infrastructure.exchanges import kraken_sdk_patches
from snapper.infrastructure.exchanges.kraken_sdk_patches import _ALREADY_SUBSCRIBED_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CLOSE_CODE_BACKOFF_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CONNECTOR_PUBLISHERS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_CONNECTOR_ID
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import _FUTURES_PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _LAST_CLOSE_CODE
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PENDING_RETRY_AFTER_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MAX_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MIN_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _kraken_futures_ws
from snapper.infrastructure.exchanges.kraken_sdk_patches import _parse_retry_after
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_get_reconnect_wait
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_init
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_manage_subscriptions
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_reconnect
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_run
from snapper.infrastructure.exchanges.kraken_sdk_patches import _unregister_connector
from snapper.infrastructure.exchanges.kraken_sdk_patches import _wrap_connect_factory
from snapper.infrastructure.exchanges.kraken_sdk_patches import _ws_client
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    apply_kraken_already_subscribed_filter,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_futures_pool_routing
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_retry_after_honoring
from snapper.infrastructure.exchanges.kraken_sdk_patches import get_registered_publisher
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import _POOL_HOLDER
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool


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
    async def test_context_var_set_during_run_and_reset_after(self) -> None:
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
        original = kraken_sdk_patches._ORIGINAL_RUN
        kraken_sdk_patches._ORIGINAL_RUN = fake_run
        try:
            await _patched_run(connector, event)
        finally:
            kraken_sdk_patches._ORIGINAL_RUN = original
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

    def test_init_registers_when_publisher_set(self) -> None:
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

            original = kraken_sdk_patches._ORIGINAL_INIT
            kraken_sdk_patches._ORIGINAL_INIT = fake_original
            try:
                _patched_init(connector)
            finally:
                kraken_sdk_patches._ORIGINAL_INIT = original
            assert _CONNECTOR_PUBLISHERS[id(connector)] is publisher
            assert get_registered_publisher(id(connector)) is publisher
        finally:
            _CURRENT_PUBLISHER.reset(token)
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)

    def test_init_does_not_register_when_no_publisher(self) -> None:
        """Spec — full Given/When/Then below.

        Given no ``_CURRENT_PUBLISHER`` set,
        When ``_patched_init`` runs,
        Then no registry entry is created.
        """
        assert _CURRENT_PUBLISHER.get() is None
        connector = ConnectSpotWebsocketBase.__new__(ConnectSpotWebsocketBase)

        def fake_original(self_obj: Any, *args: Any, **kwargs: Any) -> None:
            return None

        original = kraken_sdk_patches._ORIGINAL_INIT
        kraken_sdk_patches._ORIGINAL_INIT = fake_original
        try:
            _patched_init(connector)
        finally:
            kraken_sdk_patches._ORIGINAL_INIT = original
        assert id(connector) not in _CONNECTOR_PUBLISHERS

    def test_weakref_finalize_unregisters_on_gc(self) -> None:
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

            original = kraken_sdk_patches._ORIGINAL_INIT
            kraken_sdk_patches._ORIGINAL_INIT = fake_original
            try:
                _patched_init(connector)
            finally:
                kraken_sdk_patches._ORIGINAL_INIT = original
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


class TestPatchedReconnect:
    """The ``__reconnect`` override notifies the owning publisher."""

    @pytest.mark.asyncio
    async def test_notifies_publisher_then_delegates(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connector mapped to a publisher,
        When ``_patched_reconnect`` is awaited,
        Then ``publisher._on_sdk_reconnect_attempt`` is called and the
        original SDK reconnect coroutine is awaited.
        """
        publisher = MagicMock()
        publisher._on_sdk_reconnect_attempt = MagicMock()
        connector = MagicMock()
        _CONNECTOR_PUBLISHERS[id(connector)] = publisher
        delegate_called: dict[str, bool] = {"called": False}

        async def fake_original(self_obj: Any) -> None:
            delegate_called["called"] = True

        original = kraken_sdk_patches._ORIGINAL_RECONNECT
        kraken_sdk_patches._ORIGINAL_RECONNECT = fake_original
        try:
            await _patched_reconnect(connector)
        finally:
            kraken_sdk_patches._ORIGINAL_RECONNECT = original
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        publisher._on_sdk_reconnect_attempt.assert_called_once_with()
        assert delegate_called["called"] is True

    @pytest.mark.asyncio
    async def test_unregistered_connector_still_delegates(self) -> None:
        """Spec — full Given/When/Then below.

        Given a connector not in the publisher registry,
        When ``_patched_reconnect`` is awaited,
        Then no publisher notification occurs and the SDK reconnect runs.
        """
        connector = MagicMock()
        _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        delegate_called: dict[str, bool] = {"called": False}

        async def fake_original(self_obj: Any) -> None:
            delegate_called["called"] = True

        original = kraken_sdk_patches._ORIGINAL_RECONNECT
        kraken_sdk_patches._ORIGINAL_RECONNECT = fake_original
        try:
            await _patched_reconnect(connector)
        finally:
            kraken_sdk_patches._ORIGINAL_RECONNECT = original
        assert delegate_called["called"] is True

    @pytest.mark.asyncio
    async def test_publisher_without_reconnect_hook_still_delegates(self) -> None:
        """Spec — publishers without ``_on_sdk_reconnect_attempt`` do not break reconnect.

        Given a connector registered against a publisher that lacks
        the optional ``_on_sdk_reconnect_attempt`` watchdog hook
        (e.g. ``KrakenEquitiesMarketDataPublisher`` — which sets
        ``_CURRENT_PUBLISHER`` for egress tagging but does not need
        the reconnect-storm watchdog),
        When ``_patched_reconnect`` is awaited,
        Then no AttributeError is raised and the SDK reconnect still
        runs. Regression guard for the live failure where adding
        Equities to _CURRENT_PUBLISHER broke every reconnect cycle
        because the patch was calling the hook unconditionally.
        """
        connector = MagicMock()
        publisher_without_hook = MagicMock(spec=[])
        _CONNECTOR_PUBLISHERS[id(connector)] = publisher_without_hook
        delegate_called: dict[str, bool] = {"called": False}

        async def fake_original(self_obj: Any) -> None:
            delegate_called["called"] = True

        original = kraken_sdk_patches._ORIGINAL_RECONNECT
        kraken_sdk_patches._ORIGINAL_RECONNECT = fake_original
        try:
            await _patched_reconnect(connector)
        finally:
            kraken_sdk_patches._ORIGINAL_RECONNECT = original
            _CONNECTOR_PUBLISHERS.pop(id(connector), None)
        assert delegate_called["called"] is True


class TestCloseCodeBackoff:
    """Phase A.3 — Kraken WebSocket close codes drive custom reconnect backoff.

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
    async def test_connection_closed_with_code_stashes(self) -> None:
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

        original = kraken_sdk_patches._ORIGINAL_RUN
        kraken_sdk_patches._ORIGINAL_RUN = raising_run
        try:
            with pytest.raises(ConnectionClosedError):
                await kraken_sdk_patches._patched_run(connector, asyncio.Event())
        finally:
            kraken_sdk_patches._ORIGINAL_RUN = original
        assert _LAST_CLOSE_CODE.get(connector_id) == 1012
        _LAST_CLOSE_CODE.pop(connector_id, None)

    @pytest.mark.asyncio
    async def test_connection_closed_without_rcvd_does_not_stash(self) -> None:
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

        original = kraken_sdk_patches._ORIGINAL_RUN
        kraken_sdk_patches._ORIGINAL_RUN = raising_run
        try:
            with pytest.raises(ConnectionClosedError):
                await kraken_sdk_patches._patched_run(connector, asyncio.Event())
        finally:
            kraken_sdk_patches._ORIGINAL_RUN = original
        assert connector_id not in _LAST_CLOSE_CODE

    @pytest.mark.asyncio
    async def test_normal_completion_does_not_stash(self) -> None:
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

        original = kraken_sdk_patches._ORIGINAL_RUN
        kraken_sdk_patches._ORIGINAL_RUN = clean_run
        try:
            await kraken_sdk_patches._patched_run(connector, asyncio.Event())
        finally:
            kraken_sdk_patches._ORIGINAL_RUN = original
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
    """Phase B' shim integration tests — pool reserve + 429 + 1015 paths.

    The shim now consults ``get_egress_pool()`` and, when a pool is
    configured, reserves a route, injects the proxy kwarg, and routes
    429 / 1015 captures to ``reservation.quarantine`` instead of the
    legacy global stash. With pool disabled, the original Phase A.1
    behaviour is preserved byte-for-byte.
    """

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        """Clear the egress-pool singleton + Phase A stashes before/after each test."""
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
        """Spec — pool disabled passes kwargs through unchanged.

        Given get_egress_pool() returns None,
        When _ConnectShim is constructed with arbitrary kwargs,
        Then original_connect is called with those kwargs verbatim —
        no proxy override, no new keys (preserves the Phase A.1
        default of websockets-16 ``proxy=True`` env auto-detect).
        """
        seen_kwargs: dict[str, Any] = {}

        def fake_connect(*args: Any, **kwargs: Any) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        shim_cls = _wrap_connect_factory(fake_connect)
        shim_cls("wss://kraken", ping_interval=20)
        assert seen_kwargs == {"ping_interval": 20}

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
        assert seen_kwargs == {"proxy": None}

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
        assert seen_kwargs == {"proxy": "socks5h://snapper-egress:1081"}

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
        populated (the v4 acceptance criterion — pool quarantine
        replaces the global stash when pool is enabled).
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
        """Spec — pool disabled keeps Phase A.1 stash behaviour.

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
    async def test_shim_non_invalid_status_exception_releases(self) -> None:
        """Spec — arbitrary exception in __aenter__ releases the reservation.

        Given the pool is enabled,
        When the handshake raises OSError,
        Then in_use_count is back to 0.
        """
        pool = self._enable_pool()

        async def fake_aenter(_: Any) -> Any:
            raise OSError("net unreachable")

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
        restart; Phase A.3's per-close-code backoff owns the recovery).
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

        assert seen_kwargs == {"proxy": "socks5h://snapper-egress:1085"}

    def test_shim_falls_back_to_kraken_when_no_publisher_context(self) -> None:
        """Spec — when ``_CURRENT_PUBLISHER`` is None, reservation tag falls back to ``"kraken"``.

        Given the pool is enabled with a SOCKS5 route,
        And ``_CURRENT_PUBLISHER`` is NOT set (default ``None``),
        When the shim runs,
        Then the reservation uses the legacy ``"kraken"`` tag,
        preserving back-compat for the Spot publisher path where
        ``KrakenMarketDataPublisher.start`` sets the ContextVar
        explicitly. (Other Kraken publishers that have not yet been
        migrated to set ``_CURRENT_PUBLISHER`` still get pool routing
        under the legacy tag.)
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
    """Phase B' precedence tests for ``_patched_get_reconnect_wait``.

    The v4 design splits behaviour on ``get_egress_pool()``:

    * Pool=None — Phase A.3 path: Retry-After stash → close-code → SDK exponential.
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
        """Helper — same as TestPhaseBPrimeShim._enable_pool."""
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
        """Spec — pool disabled + stash present → return stashed value (Phase A.1).

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

    def test_pool_empty_falls_through_to_sdk_exponential(self) -> None:
        """Spec — pool size==0 falls through to SDK exponential.

        Given an EgressPool configured with no routes (defensive — the
        Pydantic validator forbids this, but the runtime branch must
        also be safe),
        When _patched_get_reconnect_wait runs,
        Then the SDK exponential is invoked.
        """
        configure_egress_pool(EgressPoolConfig(enabled=False, routes=[]))
        original = kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT
        captured = MagicMock(return_value=42.5)
        kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT = captured
        try:
            connector = MagicMock(spec=ConnectSpotWebsocketBase)
            wait = _patched_get_reconnect_wait(connector, 3)
        finally:
            kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT = original
        assert wait == 42.5

    def test_429_failover_in_one_second_when_healthy_route_available(self) -> None:
        """Spec — Phase B' acceptance test (Codex v3→v4 critical).

        Given pool enabled with direct (priority=0) + socks5 (priority=10),
        When the shim drives a 429 with Retry-After=600 on direct,
        Then:
          (a) direct.quarantine_until is set ~now+600s,
          (b) ``_PENDING_RETRY_AFTER_S`` is NOT populated,
          (c) ``_patched_get_reconnect_wait`` returns ~1.0 (not 600),
          (d) the next reservation picks the socks5 route.

        This is the central Phase B' guarantee: a route-scoped 429
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

    These pin behaviour for code paths the v4 design must support but
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

    def test_get_reconnect_wait_pool_enabled_but_no_routes(self) -> None:
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
            original = kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT
            captured = MagicMock(return_value=17.5)
            kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT = captured
            try:
                connector = MagicMock(spec=ConnectSpotWebsocketBase)
                wait = _patched_get_reconnect_wait(connector, 2)
            finally:
                kraken_sdk_patches._ORIGINAL_GET_RECONNECT_WAIT = original
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
