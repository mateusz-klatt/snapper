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
import gc
from typing import Any
from unittest.mock import MagicMock

import pytest
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from websockets.exceptions import ConnectionClosedError
from websockets.exceptions import InvalidStatus
from websockets.frames import Close
from websockets.http11 import Headers
from websockets.http11 import Response

from snapper.infrastructure.exchanges import kraken_sdk_patches
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CLOSE_CODE_BACKOFF_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CONNECTOR_PUBLISHERS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_CONNECTOR_ID
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import _LAST_CLOSE_CODE
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PATCH_APPLIED
from snapper.infrastructure.exchanges.kraken_sdk_patches import _PENDING_RETRY_AFTER_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MAX_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RETRY_AFTER_MIN_SECONDS
from snapper.infrastructure.exchanges.kraken_sdk_patches import _parse_retry_after
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_get_reconnect_wait
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_init
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_reconnect
from snapper.infrastructure.exchanges.kraken_sdk_patches import _patched_run
from snapper.infrastructure.exchanges.kraken_sdk_patches import _unregister_connector
from snapper.infrastructure.exchanges.kraken_sdk_patches import _wrap_connect_factory
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_retry_after_honoring
from snapper.infrastructure.exchanges.kraken_sdk_patches import get_registered_publisher


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
