"""Observe real Requests auth-hook ordering before sensitive redirect bodies are consumed.

These cases use the existing actual SDK and socket-free streaming adapter.
Their source-only preparation establishes no measured failure or repair.
"""

import pytest
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.auth import AuthBase

import snapper.infrastructure.exchanges.kraken_futures_account_receipts as account_transport
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _KEY
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _ORIGIN
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _PATH
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _PRIVATE_MARKER
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _Adapter
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_closed
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_safe_error
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _client
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _read


def _assert_original_request(adapter: _Adapter) -> None:
    """Require one actual original signed GET rather than a changed route or request-body refusal."""
    assert len(adapter.calls) == 1
    request = adapter.calls[0]
    assert request.method == "GET" and request.url == _ORIGIN + _PATH
    assert request.body is None and request.headers["APIKey"] == _KEY
    assert request.headers["Authent"] and "Authorization" not in request.headers


def test_auth_clearing_hooks_during_preparation_still_has_unread_redirect_body() -> None:
    """Requests may register the required original response hook after authentication completes.

    Given: The actual SDK and an AuthBase clearing its current prepared response-hook list,
    When: The real Requests preparation subsequently processes the original request hooks,
    Then: HTTP refusal keeps the original redirect body unread and closes its source.
    """
    events: list[str] = []

    class ClearDuringPreparation(AuthBase):
        """Change only the public response-hook list at the genuine authentication callback."""

        def __call__(self, request: PreparedRequest) -> PreparedRequest:
            """Leave method, URL, body and signing headers unchanged."""
            events.append("auth")
            request.hooks["response"].clear()
            return request

    adapter = _Adapter(body=_PRIVATE_MARKER.encode(), status=301)
    adapter.headers["Location"] = "https://different.invalid/private"
    with _client(adapter) as user:
        session = getattr(user, "_FuturesClient__session")
        assert isinstance(session, Session)
        session.auth = ClearDuringPreparation()
        with pytest.raises(NativeKeyIdentityTransportError, match="http_refused") as caught:
            _read(user)
    assert events == ["auth"]
    _assert_original_request(adapter)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)
    assert tuple(stream.read_calls for stream in adapter.streams) == (0,)


def test_auth_response_hook_cannot_remove_redirect_body_interception() -> None:
    """An actual AuthBase response hook must not bypass refusal before private body reads.

    Given: A real signed request whose AuthBase registers the first response callback,
    When: Requests processes the original redirect after that authentication callback was prepared,
    Then: The original HTTP refusal still leaves its sensitive body unread and closes the source.
    """
    events: list[str] = []

    def clear_response_hooks(response: Response, **kwargs: object) -> Response:
        """Clear the live dispatch list before the later registered required hook is visited."""
        request = response.request
        assert isinstance(request, PreparedRequest)
        assert account_transport._stop_redirect in request.hooks["response"]
        events.append("response_hook")
        request.hooks["response"].clear()
        return response

    class ClearFromFirstResponseHook(AuthBase):
        """Use the real Requests authentication extension without replacing transport or issuance."""

        def __call__(self, request: PreparedRequest) -> PreparedRequest:
            """Register only an actual response callback and preserve all signed request fields."""
            events.append("auth")
            request.register_hook("response", clear_response_hooks)
            assert request.hooks["response"] == [clear_response_hooks]
            return request

    adapter = _Adapter(body=_PRIVATE_MARKER.encode(), status=301)
    adapter.headers["Location"] = "https://different.invalid/private"
    with _client(adapter) as user:
        session = getattr(user, "_FuturesClient__session")
        assert isinstance(session, Session)
        session.auth = ClearFromFirstResponseHook()
        with pytest.raises(NativeKeyIdentityTransportError, match="http_refused") as caught:
            _read(user)
    assert events in (["auth"], ["auth", "response_hook"])
    _assert_original_request(adapter)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)
    assert tuple(stream.read_calls for stream in adapter.streams) == (0,)
