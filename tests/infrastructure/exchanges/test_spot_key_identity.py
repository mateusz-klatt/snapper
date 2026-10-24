"""Actual signed local HTTP and source-lifetime checks for the separate Spot identity boundary."""

import base64
import copy
import hashlib
import hmac
import json
import pickle
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from kraken.spot import User
from requests import PreparedRequest
from requests import RequestException
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.kraken_spot_key_identity import SpotKeyIdentityTransportError
from snapper.infrastructure.exchanges.kraken_spot_key_identity import (
    read_spot_prepared_key_identity,
)
from snapper.infrastructure.exchanges.prepared_spot_account_identity import (
    PreparedSpotAccountIdentity,
)
from snapper.infrastructure.exchanges.prepared_spot_account_identity import (
    SpotAccountIdentitySourceError,
)
from snapper.infrastructure.exchanges.spot_key_identity_projection import (
    SpotKeyIdentityProjectionError,
)
from snapper.infrastructure.network.egress_context import egress_identity
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _BodyStream

_KEY = "SYNTHETIC_SPOT_SELECTED_KEY"
_SECRET = "c3ludGhldGljLXNwb3Qtc2VjcmV0"
_PRIVATE = "SYNTHETIC_PRIVATE_SPOT_KEY_METADATA"
_IIBAN = "AA00 TEST ACCT 0001"
_PATH = "/0/private/GetApiKeyInfo"
_URL = "https://api.kraken.com" + _PATH


def _selection(**changes: object) -> ExecutionClientSelection:
    """Supply synthetic public provenance for the exact actual Spot SDK instance."""
    return ExecutionClientSelection.model_validate(
        {
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "exchange": "kraken",
            "mode": "live",
            "environment": "production",
            "credential_public_id": "00000000-0000-7000-8000-000000000002",
            "credential_timestamp": datetime(2026, 10, 23, tzinfo=UTC),
            "public_api_origin": "https://api.kraken.com",
            **changes,
        }
    )


def _body(key_field: str = "api_key", **changes: object) -> bytes:
    """Include realistic sensitive original fields which must never leave the closed projection."""
    return json.dumps(
        {
            "error": [],
            "result": {
                key_field: _KEY,
                "iban": _IIBAN,
                "permissions": ["query-funds", "modify-trades"],
                "nonce": "12345",
                "api_key_name": _PRIVATE,
                "ip_allowlist": ["192.0.2.0/24"],
                "unrecognized_sensitive": _PRIVATE,
                **changes,
            },
        }
    ).encode()


class _Adapter(HTTPAdapter):
    """Exercise actual Requests preparation with explicit zero retries and locally owned responses."""

    def __init__(self, body: bytes | None = None, status: int = 200) -> None:
        """Own only synthetic HTTP state; the real adapter pool is never asked to open a socket."""
        super().__init__(max_retries=0)
        self.body = _body() if body is None else body
        self.status = status
        self.calls: list[PreparedRequest] = []
        self.options: list[dict[str, object]] = []
        self.responses: list[Response] = []
        self.streams: list[_BodyStream] = []

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Return an actual streaming response to Requests' normal dispatch and hook handling.

        Args:
            request: Actual final private prepared request with synthetic signing fields.
            args: Actual adapter positional arguments.
            kwargs: Actual timeout, streaming, verification and proxy settings.

        Returns:
            Original locally supplied HTTP response and observable body stream.
        """
        self.calls.append(request)
        self.options.append(kwargs)
        stream = _BodyStream(self.body)
        response = Response()
        response.request = request
        response.url = cast(str, request.url)
        response.status_code = self.status
        if self.status in {301, 302, 303, 307, 308}:
            response.headers["Location"] = "https://unexpected.invalid/collect"
        response.raw = HTTPResponse(body=stream, status=self.status, preload_content=False)
        self.streams.append(stream)
        self.responses.append(response)
        return response


@contextmanager
def _client(adapter: _Adapter) -> Iterator[User]:
    """Own the pinned actual SDK and its original local HTTP Session through cleanup."""
    user = User(key=_KEY, secret=_SECRET)
    session = cast(Session, getattr(user, "_SpotClient__session"))
    session.trust_env = False
    session.mount("https://", adapter)
    try:
        yield user
    finally:
        session.close()


def _read(
    user: User, selected: ExecutionClientSelection | None = None
) -> PreparedSpotAccountIdentity:
    """Enter the same private egress context required by the actual closed read boundary."""
    with egress_identity(
        exchange="kraken", traffic_class="private", owner="test", operation="spot_key_identity"
    ):
        return read_spot_prepared_key_identity(user, selected or _selection())


def _closed(adapter: _Adapter) -> None:
    """Require actual original response and stream cleanup on every exercised source path."""
    assert adapter.responses
    assert all(response.raw.closed for response in adapter.responses)
    assert all(stream.closed for stream in adapter.streams)


def _safe(error: BaseException) -> None:
    """Inspect ordinary rendered errors without allowing source secrets or private metadata."""
    rendered = "".join(traceback.format_exception(error)) + str(error) + repr(error)
    for private in (_KEY, _SECRET, _PRIVATE, _IIBAN):
        assert private not in rendered
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("key_field", ("api_key", "apiKey"))
def test_actual_sdk_signature_and_original_key_info_issue_only_safe_account_source(
    key_field: str,
) -> None:
    """The safe observation is issued only after the actual signed original operation completes.

    Given: The real Spot SDK and a local original response using one documented key-echo spelling,
    When: Its fixed key-info POST traverses normal Requests preparation and closes the response,
    Then: The signature covers the exact nonce/body/path and only original safe identity is exposed.
    """
    adapter = _Adapter(_body(key_field))
    with _client(adapter) as user:
        source = _read(user)
        try:
            value = source.observation
            assert value.internal_account_id == _IIBAN
            assert value.client_selection == _selection()
            assert value.echoed_key_field == key_field
            assert value.permissions == ("query-funds", "modify-trades")
            assert value.request_started_at <= value.observed_at
            assert source.operation_public_id != value.client_selection.wallet_public_id
            for private in (_KEY, _SECRET, _PRIVATE, "nonce", "ip_allowlist"):
                assert private not in value.model_dump_json()
            assert _IIBAN not in repr(source)
            assert len(adapter.calls) == 1
            request = adapter.calls[0]
            assert request.method == "POST" and request.url == _URL
            assert isinstance(request.body, bytes)
            body = json.loads(request.body)
            assert set(body) == {"nonce"}
            hashed = hashlib.sha256(body["nonce"].encode() + request.body).digest()
            expected = base64.b64encode(
                hmac.new(
                    base64.b64decode(_SECRET), _PATH.encode() + hashed, hashlib.sha512
                ).digest()
            ).decode()
            assert request.headers["API-Key"] == _KEY
            assert request.headers["API-Sign"] == expected
            assert adapter.options[0]["stream"] is True
            assert adapter.options[0]["timeout"] == (5, 10)
            assert (
                source.assert_original_request(user, _selection(), _KEY, datetime.now(UTC)) == value
            )
        finally:
            source.close()
    _closed(adapter)


@pytest.mark.parametrize(
    "changes",
    (
        {"api_key": "different-key"},
        {"api_key": None},
        {"api_key": True},
        {"api_key": "SYNTHETIC_SPOT_SELECTED_KEY\n"},
        {"api_key": "\ud800"},
        {"apiKey": _KEY},
        {"apiKey": "different-key"},
        {"iban": None},
        {"iban": "AA00TESTACCT0001"},
        {"iban": _IIBAN + "\n"},
        {"permissions": None},
        {"permissions": "query-funds"},
        {"permissions": [_KEY]},
        {"permissions": [True]},
        {"permissions": ["future-unknown-permission"]},
    ),
)
def test_invalid_original_key_info_refuses_without_disclosing_private_source(
    changes: dict[str, object],
) -> None:
    """Actual source matching and strict projection cannot silently normalize unknown identity.

    Given: A real signed response with ambiguous, mismatched or malformed original identity fields,
    When: The actual transport attempts its safe whitelist projection,
    Then: No issued source escapes and the private original response is closed with a fixed error.
    """
    adapter = _Adapter(_body(**changes))
    with _client(adapter) as user, pytest.raises(SpotKeyIdentityProjectionError) as caught:
        _read(user)
    _safe(caught.value)
    _closed(adapter)


@pytest.mark.parametrize(
    "body",
    (
        b"[]",
        b"{",
        b'{"error":["SYNTHETIC_PRIVATE_SPOT_KEY_METADATA"]}',
        b'{"result":{}}',
        b'{"error":null,"result":{}}',
        b'{"error":[],"result":null}',
        b'{"error":[],"result":[]}',
        b'{"error":[],"result":{}}',
        _body()[:-1] + b',"error":[]}',
        _body().decode().encode("utf-16-le"),
        _body().decode().encode("utf-32-be"),
    ),
)
def test_original_response_shape_and_utf8_contract_are_required_before_issuance(
    body: bytes,
) -> None:
    """JSON/Spot success shape must be established before selecting any account identity field.

    Given: An actual local HTTP success containing invalid JSON, duplicate keys or incomplete response shape,
    When: The fixed original key-info operation consumes its bounded body,
    Then: It refuses source issuance, closes HTTP material and retains no sensitive error text.
    """
    adapter = _Adapter(body)
    with _client(adapter) as user, pytest.raises(SpotKeyIdentityProjectionError) as caught:
        _read(user)
    _safe(caught.value)
    _closed(adapter)


@pytest.mark.parametrize("status", (301, 302, 303, 307, 308, 401, 403, 429, 503))
def test_http_refusal_never_reads_private_body_or_follows_redirect(status: int) -> None:
    """HTTP errors are closed before any sensitive original body is consumed.

    Given: An original signed endpoint returning redirect or error status with private fixture data,
    When: The actual Requests response reaches the protected interception boundary,
    Then: Exactly one request occurred, every source body stays unread and the response closes.
    """
    adapter = _Adapter(status=status)
    with _client(adapter) as user, pytest.raises(
        SpotKeyIdentityTransportError, match="http_refused"
    ) as caught:
        _read(user)
    assert len(adapter.calls) == 1
    assert all(stream.read_calls == 0 for stream in adapter.streams)
    _safe(caught.value)
    _closed(adapter)


class _MutatingAuth(AuthBase):
    """Change the actual Requests-authentication boundary after the original SDK has signed."""

    def __init__(self, field: str) -> None:
        """Select one exact prepared-request change using only synthetic fixture values."""
        self.field = field

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Apply a real AuthBase mutation to the same object consumed by Session.prepare_request."""
        if self.field == "method":
            prepared.method = "GET"
        elif self.field == "url":
            prepared.url = "https://unexpected.invalid/key-info"
        elif self.field == "body":
            prepared.body = b'{"nonce":"changed"}'
        else:
            prepared.headers[self.field] = "SYNTHETIC_CHANGED_VALUE"
        return prepared


@pytest.mark.parametrize(
    "field",
    (
        "method",
        "url",
        "body",
        "API-Key",
        "API-Sign",
        "Content-Type",
        "Authorization",
        "Transfer-Encoding",
        "Content-Encoding",
    ),
)
def test_actual_final_request_mutation_refuses_before_physical_send(field: str) -> None:
    """A matching response cannot rescue signing material changed after SDK preparation.

    Given: The original SDK with an AuthBase mutator installed on its real Session,
    When: Authentication changes the actual final method, route, body or signed headers,
    Then: No physical local request occurs and no source is issued.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_SpotClient__session"))
        session.auth = _MutatingAuth(field)
        with pytest.raises(SpotKeyIdentityTransportError) as caught:
            _read(user)
        _safe(caught.value)
    assert not adapter.calls


@pytest.mark.parametrize("operation", ("pickle", "copy", "deepcopy"))
def test_actual_issued_source_cannot_be_serialized_or_duplicated(operation: str) -> None:
    """Generic operations cannot copy an original signing/source operation into another owner.

    Given: A genuine original issued Spot identity source,
    When: Generic serialization or shallow/deep copying traverses the opaque boundary,
    Then: It refuses before any private state can be serialized or duplicated.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        source = _read(user)
        try:
            with pytest.raises(TypeError, match="cannot be"):
                if operation == "pickle":
                    pickle.dumps(source)
                elif operation == "copy":
                    copy.copy(source)
                else:
                    copy.deepcopy(source)
        finally:
            source.close()


def test_caller_constructed_or_closed_source_cannot_prove_actual_spot_identity() -> None:
    """A public type or copied value cannot replace an original live source registry entry.

    Given: A caller-constructed receipt and an actually issued then closed source,
    When: Their observations and operation identities are requested,
    Then: Actual issuance checks refuse both, with repeated close remaining harmless.
    """
    forged = PreparedSpotAccountIdentity()
    adapter = _Adapter()
    with _client(adapter) as user:
        actual = _read(user)
        actual.close()
        for source in (forged, actual):
            for field in ("observation", "operation_public_id"):
                with pytest.raises(SpotAccountIdentitySourceError, match="not_transport_issued"):
                    getattr(source, field)
            source.close()


def test_detached_public_identity_mutation_cannot_relabel_original_source() -> None:
    """Nested public model copies cannot rewrite the real original registry-owned account binding.

    Given: A genuine source and its detached safe public identity value,
    When: Object-level mutation changes both account identifier and selected credential,
    Then: Subsequent reads and the original-key request check still return the original source.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        source = _read(user)
        try:
            original = source.observation
            changed = source.observation
            object.__setattr__(changed, "internal_account_id", "AA00 TEST ACCT 9999")
            object.__setattr__(changed.client_selection, "credential_public_id", "changed")
            assert source.observation == original
            assert source.observation != changed
            assert (
                source.assert_original_request(user, _selection(), _KEY, datetime.now(UTC))
                == original
            )
        finally:
            source.close()


@pytest.mark.parametrize(
    "change",
    (
        "other_user",
        "session",
        "origin",
        "selection",
        "user_key",
        "user_key_null",
        "user_key_unicode",
        "encoded_key",
        "encoded_key_null",
        "encoded_key_unicode",
    ),
)
def test_original_spot_source_cannot_follow_another_actual_signing_owner(
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """The original account observation cannot be rebound by changing its real request owner.

    Given: A genuine signed identity source and its exact original User/Session/key,
    When: Actual runtime owner, endpoint, public revision or final signing key changes,
    Then: The existing source refuses the following request instead of borrowing its earlier identity.
    """
    adapter = _Adapter()
    with _client(adapter) as user, _client(_Adapter()) as other_user, Session() as replacement:
        source = _read(user)
        try:
            actual_user = user
            selected = _selection()
            key: object = _KEY
            if change == "other_user":
                actual_user = other_user
            elif change == "session":
                monkeypatch.setattr(user, "_SpotClient__session", replacement)
            elif change == "origin":
                monkeypatch.setattr(user, "URL", "https://unexpected.invalid")
            elif change == "selection":
                selected = _selection(credential_public_id="00000000-0000-7000-8000-000000000099")
            elif change.startswith("user_key"):
                values: dict[str, object] = {
                    "user_key": "changed",
                    "user_key_null": None,
                    "user_key_unicode": "\ud800",
                }
                monkeypatch.setattr(user, "_key", values[change])
            else:
                values = {
                    "encoded_key": "changed",
                    "encoded_key_null": None,
                    "encoded_key_unicode": "\ud800",
                }
                key = values[change]
            with pytest.raises(SpotAccountIdentitySourceError, match="owner_or_key_changed"):
                source.assert_original_request(actual_user, selected, key, datetime.now(UTC))
        finally:
            source.close()


@pytest.mark.parametrize("naive", (False, True))
def test_source_cannot_bind_request_before_its_original_response_clock(naive: bool) -> None:
    """An original identity response cannot retrospectively prove an earlier request.

    Given: A genuine source and its original local response-completion clock,
    When: A following request presents an earlier or timezone-naive dispatch clock,
    Then: The live source refuses chronology without manufacturing time advancement.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        source = _read(user)
        try:
            clock = source.observation.observed_at - timedelta(microseconds=1)
            if naive:
                clock = clock.replace(tzinfo=None)
            with pytest.raises(SpotAccountIdentitySourceError, match="clock_unproven"):
                source.assert_original_request(user, _selection(), _KEY, clock)
        finally:
            source.close()


class _HookRemovingAuth(AuthBase):
    """Install a genuine response hook which would remove a later redirect barrier if retained."""

    def __init__(self) -> None:
        """Record whether the caller-owned callback was allowed into the private response chain."""
        self.invoked = False

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Register through actual AuthBase before Requests completes request hook preparation."""
        prepared.register_hook("response", self.clear_later_hooks)
        return prepared

    def clear_later_hooks(self, response: Response, **kwargs: object) -> Response:
        """Clear the actual dispatch list, which the new closed request must never inherit.

        Args:
            response: Actual local HTTP response passed by Requests.
            kwargs: Actual response-hook keyword context.

        Returns:
            The same original response after the observable source-boundary mutation.
        """
        self.invoked = True
        response.request.hooks["response"].clear()
        return response


def test_private_owned_response_hook_cannot_be_removed_by_authentication_callback() -> None:
    """The fixed new operation owns its response barrier after all public authentication hooks.

    Given: Actual AuthBase registering a first callback that would remove subsequent response hooks,
    When: The original signed key-info POST returns a redirect with sensitive fixture data,
    Then: Only the private barrier runs, no body is read and the original response closes.
    """
    adapter = _Adapter(status=301)
    auth = _HookRemovingAuth()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_SpotClient__session"))
        session.auth = auth
        with pytest.raises(SpotKeyIdentityTransportError, match="http_refused"):
            _read(user)
    assert not auth.invoked
    assert len(adapter.calls) == 1
    assert tuple(stream.read_calls for stream in adapter.streams) == (0,)
    _closed(adapter)


@pytest.mark.parametrize("retry_total", (None, 1, 2))
def test_actual_session_with_unbounded_or_enabled_retries_refuses_before_send(
    retry_total: int | None,
) -> None:
    """A successful signature cannot authorize hidden additional physical read attempts.

    Given: The actual selected Session adapter with automatic retry policy enabled or unbounded,
    When: The new closed operation reaches its final dispatch boundary,
    Then: It refuses before the first physical local request and creates no issued source.
    """
    adapter = _Adapter()
    adapter.max_retries = Retry(total=retry_total)
    with _client(adapter) as user, pytest.raises(
        SpotKeyIdentityTransportError, match="hidden_retry"
    ):
        _read(user)
    assert not adapter.calls


@pytest.mark.parametrize(
    "failure", ("request_before_response", "request_with_response", "unexpected_before_response")
)
def test_actual_transport_exception_cannot_retain_sensitive_context_or_owned_response(
    failure: str,
) -> None:
    """A private adapter error must not survive through the fixed public exception's context chain.

    Given: The actual signed Requests call and a local adapter failing with synthetic private details,
    When: Failure occurs before response or carries its actual original Response in RequestException,
    Then: The public error retains no sensitive exception context and any received response is closed.
    """

    class FailingAdapter(_Adapter):
        """Inject only the actual adapter failure after genuine SDK signing and final request checks."""

        def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
            """Fail at the actual local wire boundary with deliberately private fixture diagnostics."""
            if failure == "unexpected_before_response":
                raise RuntimeError(f"{_KEY} {_PRIVATE}")
            response = (
                super().send(request, *args, **kwargs)
                if failure == "request_with_response"
                else None
            )
            raise RequestException(f"{_KEY} {_PRIVATE}", request=request, response=response)

    adapter = FailingAdapter()
    with _client(adapter) as user, pytest.raises(
        SpotKeyIdentityTransportError, match="transport_failed"
    ) as caught:
        _read(user)
    _safe(caught.value)
    if failure == "request_with_response":
        _closed(adapter)
        assert tuple(stream.read_calls for stream in adapter.streams) == (0,)
    else:
        assert not adapter.responses
