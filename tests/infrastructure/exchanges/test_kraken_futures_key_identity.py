"""Selected-key checks expose only proven public identity through the actual signed SDK session."""

import base64
import hashlib
import hmac
import json
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from typing import cast

import pytest
from kraken.futures import User
from requests import PreparedRequest
from requests import RequestException
from requests import Response
from requests import Session
from requests.adapters import BaseAdapter
from requests.auth import AuthBase
from urllib3.response import HTTPResponse

import snapper.infrastructure.exchanges.kraken_futures_account_receipts as account_transport
import snapper.infrastructure.exchanges.kraken_futures_key_identity as transport
import snapper.infrastructure.exchanges.kraken_rest_egress as routing
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    NativeAccountsTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import read_futures_key_identity
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import egress_identity

_KEY = "synthetic-sensitive-signing-key"
_SECRET = "c3ludGhldGljLXNlY3JldA=="
_IIBAN = "synthetic-private-bank-identifier"
_PRIVATE_MARKER = "synthetic-private-response-extra"
_ACCOUNT_UID = "00000000-0000-7000-8000-000000000006"
_ORIGIN = "https://futures.kraken.com"
_PATH = "/api/auth/v1/api-keys/v3/check"
_NOW = datetime(2026, 10, 23, 13, tzinfo=UTC)


def _selection(*, demo: bool = False, **updates: object) -> ExecutionClientSelection:
    """Pair synthetic public credential metadata with the actual selected SDK endpoint."""
    return ExecutionClientSelection.model_validate(
        {
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "demo" if demo else "production",
            "credential_public_id": "00000000-0000-7000-8000-000000000002",
            "credential_timestamp": _NOW - timedelta(days=1),
            "public_api_origin": "https://demo-futures.kraken.com" if demo else _ORIGIN,
            **updates,
        }
    )


def _body(**updates: object) -> bytes:
    """Include realistic sensitive extras which must never leave the closed transport result."""
    return json.dumps(
        {
            "apiKey": _KEY,
            "accountUid": _ACCOUNT_UID,
            "permissions": {"general": "READ_ONLY", "transfer": "NO_ACCESS"},
            "iiban": _IIBAN,
            "allowedCidrBlocks": ["192.0.2.0/24"],
            "unrecognizedSensitive": _PRIVATE_MARKER,
            **updates,
        },
        separators=(",", ":"),
    ).encode()


class _BodyStream(BytesIO):
    """Record whether an HTTP failure body was touched before being safely closed."""

    def __init__(self, body: bytes) -> None:
        """Keep only synthetic body data and an explicit read counter inside the fixture."""
        super().__init__(body)
        self.read_calls = 0

    def read(self, size: int | None = -1) -> bytes:
        """Observe ordinary urllib3 reads without changing their actual byte behavior.

        Args:
            size: Requested read size, including the standard unbounded sentinel.

        Returns:
            The actual local fixture bytes requested by the HTTP response decoder.
        """
        self.read_calls += 1
        return super().read(size)


class _Adapter(BaseAdapter):
    """Use real requests preparation and response lifetime without opening any network socket."""

    def __init__(self, body: bytes | None = None, status: int = 200) -> None:
        """Retain synthetic response state and locally observed HTTP request metadata."""
        self.body = _body() if body is None else body
        self.status = status
        self.headers: dict[str, str] = {}
        self.calls: list[PreparedRequest] = []
        self.options: list[dict[str, object]] = []
        self.responses: list[Response] = []
        self.streams: list[_BodyStream] = []

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Supply a genuine streaming response to requests' normal transport and redirect logic.

        Args:
            request: The actual prepared request with synthetic signing headers only.
            args: Requests transport adapter positional arguments.
            kwargs: Actual stream, timeout and proxy choices from the routed session.

        Returns:
            Real response retaining an observable, socket-free source body stream.
        """
        self.calls.append(request)
        self.options.append(kwargs)
        stream = _BodyStream(self.body)
        response = Response()
        response.request = request
        response.url = cast(str, request.url)
        response.status_code = self.status
        response.headers.update(self.headers)
        response.raw = HTTPResponse(
            body=stream,
            headers=self.headers,
            status=self.status,
            preload_content=False,
        )
        self.streams.append(stream)
        self.responses.append(response)
        return response

    def close(self) -> None:
        """Own no external connections or background tasks."""


@contextmanager
def _client(adapter: _Adapter, *, demo: bool = False) -> Iterator[User]:
    """Construct the pinned SDK with synthetic credentials and its actual local HTTP session."""
    user = User(key=_KEY, secret=_SECRET, sandbox=demo)
    session = cast(Session, getattr(user, "_FuturesClient__session"))
    session.trust_env = False
    session.mount("https://", adapter)
    try:
        yield user
    finally:
        session.close()


def _read(
    user: User, selection: ExecutionClientSelection | None = None
) -> NativeAccountPublicIdentity:
    """Enter the same explicit private venue context required of the actual egress dispatcher."""
    with egress_identity(
        exchange="kraken_futures",
        traffic_class="private",
        owner="test",
        operation="key_identity",
    ):
        return read_futures_key_identity(user, selection or _selection())


def _assert_closed(adapter: _Adapter) -> None:
    """Require every actual response and source stream to be closed on success or refusal."""
    assert adapter.responses
    assert all(response.raw.closed for response in adapter.responses)
    assert all(stream.closed for stream in adapter.streams)


def _assert_safe_error(error: BaseException) -> None:
    """Check ordinary exception rendering cannot publish a sensitive key-check response."""
    rendered = "".join(traceback.format_exception(error)) + str(error) + repr(error)
    for private in (_KEY, _SECRET, _IIBAN, _PRIVATE_MARKER):
        assert private not in rendered
    assert error.__cause__ is None


@pytest.mark.parametrize("demo", (False, True))
def test_actual_signed_sdk_check_returns_only_whitelisted_public_identity(
    demo: bool,
) -> None:
    """Verify the actual authentication path and retain only selected account identity facts.

    Given: A production or demo SDK session and a response echoing its synthetic signing key,
    When: The fixed key-check request traverses the actual local HTTP adapter,
    Then: Its signature binds the exact path and its result omits the key and sensitive extras.
    """
    adapter = _Adapter()
    selected = _selection(demo=demo)
    with _client(adapter, demo=demo) as user:
        result = _read(user, selected)

    assert result.client_selection == selected
    assert result.account_uid == _ACCOUNT_UID
    assert (result.general_access, result.transfer_access) == ("READ_ONLY", "NO_ACCESS")
    assert result.request_started_at <= result.observed_at
    assert result.source_policy == "kraken_futures_key_check_redacted_v1"
    material = result.model_dump_json() + repr(result)
    for private in (
        _KEY,
        _SECRET,
        _IIBAN,
        _PRIVATE_MARKER,
        "apiKey",
        "allowedCidrBlocks",
    ):
        assert private not in material
    assert len(adapter.calls) == 1
    request = adapter.calls[0]
    assert request.method == "GET"
    assert request.url == cast(str, selected.public_api_origin) + _PATH
    assert request.body is None
    nonce = cast(str, request.headers["Nonce"])
    signed = hashlib.sha256((nonce + _PATH).encode()).digest()
    expected = base64.b64encode(
        hmac.new(base64.b64decode(_SECRET), signed, hashlib.sha512).digest()
    ).decode()
    assert request.headers["Authent"] == expected
    assert request.headers["APIKey"] == _KEY
    assert adapter.options[0]["stream"] is True
    assert adapter.options[0]["timeout"] == (5, 10)
    _assert_closed(adapter)


@pytest.mark.parametrize("general", ("NO_ACCESS", "READ_ONLY", "FULL_ACCESS"))
@pytest.mark.parametrize("transfer", ("NO_ACCESS", "READ_ONLY", "FULL_ACCESS"))
def test_observed_permissions_remain_explicit_facts_without_enabling_financial_capability(
    general: str, transfer: str
) -> None:
    """Identity capture must preserve source permission tiers without treating them as dispatch rights.

    Given: One observed account with valid independent general and transfer access tiers,
    When: The actual signed key check returns its public projection,
    Then: Exact tiers survive and no order, collateral or cash-completeness flag is added.
    """
    adapter = _Adapter(_body(permissions={"general": general, "transfer": transfer}))
    with _client(adapter) as user:
        result = _read(user)

    assert (result.general_access, result.transfer_access) == (general, transfer)
    assert not {"allow_order", "available_collateral", "cash_complete"} & set(
        result.model_fields_set
    )
    _assert_closed(adapter)


@pytest.mark.parametrize("returned", (None, False, 17, "", "different-key", "\ud800"))
def test_returned_key_must_match_the_actual_signed_request_without_echoing_it(
    returned: object,
) -> None:
    """An echoed key is checked in memory and cannot relabel another signed account response.

    Given: A successful HTTP response with an absent, malformed or different echoed signing key,
    When: Its original bytes reach the closed transport parser,
    Then: No identity is returned and the fixed refusal does not expose private response values.
    """
    adapter = _Adapter(_body(apiKey=returned))
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="selected_key_mismatch"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize(
    "body",
    (
        b"",
        b"not JSON",
        _body()[:-1] + b",",
        b"\xff",
        b"[]",
        b"null",
        b'{"apiKey":"first","apiKey":"second"}',
        b'{"accountUid":NaN}',
        b'{"accountUid":Infinity}',
    ),
)
def test_malformed_duplicate_or_nonfinite_json_cannot_escape_the_closed_parser(
    body: bytes,
) -> None:
    """Discard malformed sensitive responses through a fixed public error without retaining body hashes.

    Given: Invalid UTF-8, non-object JSON, duplicate fields or nonfinite values,
    When: The real HTTP response is decoded by the strict receipt codec,
    Then: The check refuses safely and always closes its source response.
    """
    adapter = _Adapter(body)
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="json_invalid"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    _assert_closed(adapter)


def test_decoder_recursion_refusal_has_a_fixed_private_free_error_and_closes_http_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normalize a decoder resource refusal without depending on platform-specific stack capacity.

    Given: A real signed HTTP response whose decoder raises a recursion error with private text,
    When: The closed key-check transport handles that parser failure,
    Then: Only its fixed public refusal escapes and the original response is closed.
    """
    body = _body(privateNested=[[{"private": _PRIVATE_MARKER}]])
    adapter = _Adapter(body)

    def refuse(original: bytes) -> dict[str, object]:
        assert original == body
        raise RecursionError(body.decode())

    monkeypatch.setattr(transport, "decode_private_execution_receipt", refuse)
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="json_invalid"
    ) as caught:
        _read(user)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize("permissions", (None, "READ_ONLY", [], 1))
def test_missing_permission_map_refuses_identity_without_retaining_private_fields(
    permissions: object,
) -> None:
    """A successful HTTP status cannot replace the documented structured permission evidence.

    Given: A matching signing key but a malformed permission map,
    When: The closed parser selects only allowed public fields,
    Then: It refuses the incomplete identity with a secret-free fixed error.
    """
    adapter = _Adapter(_body(permissions=permissions))
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="permissions_missing"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize(
    "updates",
    (
        {"accountUid": None},
        {"accountUid": "not-an-account-uuid"},
        {"accountUid": True},
        {"permissions": {}},
        {"permissions": {"general": "full_access", "transfer": "NO_ACCESS"}},
        {"permissions": {"general": True, "transfer": "NO_ACCESS"}},
        {"permissions": {"general": "READ_ONLY", "transfer": 1}},
    ),
)
def test_invalid_public_account_or_permission_fields_are_redacted_before_refusal(
    updates: dict[str, object],
) -> None:
    """Strict public identity validation cannot turn malformed source values into retained authority.

    Given: A matching signing key and invalid account UID or permission tiers,
    When: Only whitelisted fields reach the public DTO validator,
    Then: Its error stays fixed and excludes the sensitive source body.
    """
    adapter = _Adapter(_body(**updates))
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="public_fields_invalid"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize("status", (301, 302, 303, 307, 308, 401, 403, 429, 503))
def test_redirect_or_non_success_never_reads_sensitive_body_or_forwards_headers(
    status: int,
) -> None:
    """Refuse HTTP failures before consuming a body or following any authenticated redirect.

    Given: A signed endpoint response with a redirect or non-success status and private body,
    When: Requests delivers the actual response to the closed check,
    Then: Only the original host is contacted, body bytes remain unread and the response closes.
    """
    adapter = _Adapter(_body(), status)
    adapter.headers["Location"] = "https://different.invalid/collect"
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="http_refused"
    ) as caught:
        _read(user)

    assert [request.url for request in adapter.calls] == [_ORIGIN + _PATH]
    assert all(stream.read_calls == 0 for stream in adapter.streams)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize(
    "index,value",
    (
        (0, "POST"),
        (1, "https://different.invalid/key-check"),
        (2, {}),
        (2, {"APIKey": None}),
        (2, {"APIKey": 1}),
        (2, {"APIKey": ""}),
        (3, "payload"),
        (4, "query=1"),
    ),
)
def test_changed_prepared_request_is_refused_before_authenticated_transport(
    monkeypatch: pytest.MonkeyPatch, index: int, value: object
) -> None:
    """A signer result cannot widen the fixed method, path, key or empty request entity.

    Given: A prepared SDK request with a changed routing or signing-key boundary,
    When: The closed transport verifies it before dispatch,
    Then: No HTTP request reaches the local adapter.
    """
    adapter = _Adapter()
    prepared: list[object] = ["GET", _ORIGIN + _PATH, {"APIKey": _KEY}, "", ""]
    prepared[index] = value

    def changed_prepare(*args: object, **kwargs: object) -> tuple[object, ...]:
        return tuple(prepared)

    with _client(adapter) as user:
        monkeypatch.setattr(user, "_prepare_request", changed_prepare)
        with pytest.raises(NativeKeyIdentityTransportError, match="prepared_request_mismatch"):
            _read(user)

    assert adapter.calls == []


@pytest.mark.parametrize("fault", ("url", "history", "session", "client_origin"))
def test_changed_response_or_sdk_route_refuses_public_identity_and_closes_source(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Late response or client changes cannot relabel an identity under the originally selected route.

    Given: A genuine local HTTP response followed by changed URL, history, session or SDK origin,
    When: The closed check revalidates original source ownership,
    Then: It refuses identity and closes every response and replacement session.
    """
    adapter = _Adapter()
    replacement: Session | None = None
    try:
        with _client(adapter) as user:
            session = cast(Session, getattr(user, "_FuturesClient__session"))
            original_send = session.send

            def changed_send(request: PreparedRequest, **kwargs: object) -> Response:
                nonlocal replacement
                response = original_send(request, **kwargs)
                if fault == "url":
                    response.url = "https://different.invalid/key-check"
                elif fault == "history":
                    response.history = [Response()]
                elif fault == "session":
                    replacement = Session()
                    monkeypatch.setattr(user, "_FuturesClient__session", replacement)
                else:
                    monkeypatch.setattr(user, "url", "https://different.invalid")
                return response

            monkeypatch.setattr(session, "send", changed_send)
            with pytest.raises((NativeKeyIdentityTransportError, NativeAccountsTransportError)):
                _read(user)
    finally:
        if replacement is not None:
            replacement.close()

    _assert_closed(adapter)


def test_network_exception_cannot_echo_its_sensitive_request_details() -> None:
    """Normalize network failure without retaining a response body or sensitive exception text.

    Given: A local HTTP adapter raising an exception containing synthetic private values,
    When: The actual signed request fails before a response exists,
    Then: Only the fixed public transport error escapes and no identity is returned.
    """

    class FailingAdapter(_Adapter):
        """Fail at the real requests adapter boundary with intentionally private fixture text."""

        def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
            raise RequestException(f"{_KEY} {_IIBAN} {_PRIVATE_MARKER}")

    adapter = FailingAdapter()
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="transport_failed"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    assert adapter.responses == []


def test_future_credential_revision_cannot_start_key_check() -> None:
    """An identity check cannot precede the exact selected public credential revision.

    Given: A public credential revision later than the actual local request clock,
    When: The actual SDK prepares a key-check request,
    Then: The helper refuses it before any HTTP request is sent.
    """
    adapter = _Adapter()
    selected = _selection(credential_timestamp=datetime.now(UTC) + timedelta(days=1))
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="credential_not_yet_known"
    ):
        _read(user, selected)

    assert adapter.calls == []


@pytest.mark.parametrize("body", (b"", b"private body chunk"))
def test_body_read_deadline_failure_closes_sensitive_response(
    monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    """An elapsed read failure cannot leak a partial sensitive body through the identity result.

    Given: A response whose next actual read checkpoint exceeds the fixed budget,
    When: The empty or partial body reaches the bounded shared reader,
    Then: The failure propagates without source values and the response always closes.
    """
    ticks = iter((0.0, 31.0))
    monkeypatch.setattr(transport, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(account_transport, "monotonic", lambda: next(ticks))
    adapter = _Adapter(body)
    with _client(adapter) as user, pytest.raises(
        NativeAccountsTransportError, match="read_deadline"
    ) as caught:
        _read(user)

    _assert_safe_error(caught.value)
    _assert_closed(adapter)


async def test_actual_dispatcher_owns_key_check_session_lock_and_restores_proxy_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the real worker dispatcher to prove private routing and session ownership without sockets.

    Given: A selected SDK session with existing proxy state and ambient trust enabled,
    When: The key check runs through the actual Futures private REST dispatcher,
    Then: The same locked session sends privately and its original state is restored afterward.
    """
    client = KrakenFuturesExchangeClient()
    monkeypatch.setattr(routing, "get_egress_pool", lambda: None)

    class InspectingAdapter(_Adapter):
        """Observe actual worker-local private scope and proxy ownership at its HTTP boundary."""

        def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
            identity = current_egress_identity()
            assert identity is not None
            assert (identity.exchange, identity.traffic_class) == (
                "kraken_futures",
                "private",
            )
            assert getattr(client._rest_egress_proxy_lock, "_is_owned")()
            assert session.trust_env is False
            assert session.proxies == {}
            assert getattr(user, "_FuturesClient__proxy") is None
            return super().send(request, *args, **kwargs)

    adapter = InspectingAdapter()
    try:
        with _client(adapter) as user:
            session = cast(Session, getattr(user, "_FuturesClient__session"))
            session.trust_env = True
            session.proxies = {"https": "http://previous.invalid:8080"}
            user._FuturesClient__proxy = "http://previous.invalid:8080"
            result = await client._dispatch_routed_rest(
                operation="get_native_key_identity",
                kind="private_idempotent_read",
                target=futures_sdk_proxy_target(user),
                sync_call=lambda: read_futures_key_identity(user, _selection()),
            )
            assert result.account_uid == _ACCOUNT_UID
            assert session.trust_env is True
            assert session.proxies == {"https": "http://previous.invalid:8080"}
            assert getattr(user, "_FuturesClient__proxy") == "http://previous.invalid:8080"
    finally:
        await client.disconnect()

    _assert_closed(adapter)


class _ChangedFinalAuth(AuthBase):
    """Mutate the actual Requests object after SDK signing, with synthetic local values only."""

    def __init__(self, fault: str) -> None:
        """Select one observable final prepared request mutation."""
        self.fault = fault

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Exercise Requests AuthBase at the actual post-SDK signing boundary."""
        if self.fault == "method":
            prepared.method = "POST"
        elif self.fault == "url":
            prepared.url = "https://different.invalid/key-check"
        elif self.fault == "body":
            prepared.body = b"unexpected"
        else:
            prepared.headers[self.fault] = "SYNTHETIC_CHANGED_AUTH"
        return prepared


@pytest.mark.parametrize(
    "fault", ("APIKey", "Authent", "Nonce", "Authorization", "method", "url", "body")
)
def test_final_requests_auth_mutation_refuses_before_physical_key_check_send(
    fault: str,
) -> None:
    """A safe returned key echo cannot compensate for an unchecked final HTTP request.

    Given: The actual selected SDK and a Requests authentication handler that changes signed material,
    When: The key-check transport prepares the final request through that actual session,
    Then: It refuses before physical send with a fixed secret-free error and no claimed identity.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_FuturesClient__session"))
        session.auth = _ChangedFinalAuth(fault)
        with pytest.raises(NativeKeyIdentityTransportError) as caught:
            _read(user)
    assert adapter.calls == []
    _assert_safe_error(caught.value)


@pytest.mark.parametrize("phase", ("body", "close"))
def test_unexpected_sensitive_response_failures_remain_fixed_and_private_free(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Unexpected source I/O failures cannot escape with provider or signing material in their text.

    Given: A real local signed response whose body or cleanup raises a synthetic sensitive exception,
    When: The closed key transport encounters that exact failure boundary,
    Then: Only a fixed secret-free transport refusal is exposed and the real source is closed.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        if phase == "body":

            def fail_body(*args: object) -> bytes:
                """Represent an unexpected source iterator failure containing only synthetic private data."""
                raise ValueError(_PRIVATE_MARKER)

            monkeypatch.setattr(transport, "_entity_bytes", fail_body)
        else:
            actual_close = Response.close

            def close_then_fail(response: Response) -> None:
                """Close the real original response before simulating a private-text cleanup failure."""
                actual_close(response)
                raise ValueError(_PRIVATE_MARKER)

            monkeypatch.setattr(Response, "close", close_then_fail)
        with pytest.raises(NativeKeyIdentityTransportError) as caught:
            _read(user)
    _assert_closed(adapter)
    _assert_safe_error(caught.value)


def test_retained_auth_hook_alias_cannot_disable_original_redirect_body_interception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The isolated final request owns its hook lists as well as its signing headers and GET fields.

    Given: Real SDK preparation, AuthBase retaining its original object and a redirect response,
    When: Actual environment merging clears the retained original response-hook list,
    Then: The private dispatch still intercepts that redirect without reading its sensitive body.
    """
    adapter = _Adapter(body=_PRIVATE_MARKER.encode(), status=302)
    adapter.headers["Location"] = "https://different.invalid/private"
    retained: list[PreparedRequest] = []

    class RetainingAuth(AuthBase):
        """Keep only the actual original request alias exposed by Requests authentication preparation."""

        def __call__(self, request: PreparedRequest) -> PreparedRequest:
            """Return the original request untouched while retaining its live alias for the later hook."""
            retained.append(request)
            return request

    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_FuturesClient__session"))
        session.auth = RetainingAuth()
        actual_merge = session.merge_environment_settings

        def merge_then_clear_hooks(
            url: str,
            proxies: dict[str, str] | None,
            stream: bool | None,
            verify: bool | str | None,
            cert: str | tuple[str, str] | None,
        ) -> dict[str, object]:
            """Remove only the original auth-exposed hooks after the private dispatch should be copied."""
            settings: dict[str, object] = actual_merge(url, proxies, stream, verify, cert)
            assert retained
            retained[-1].hooks["response"].clear()
            return settings

        monkeypatch.setattr(session, "merge_environment_settings", merge_then_clear_hooks)
        with pytest.raises(NativeKeyIdentityTransportError) as caught:
            _read(user)
    assert len(adapter.calls) == 1
    assert all(stream.read_calls == 0 for stream in adapter.streams)
    _assert_closed(adapter)
    _assert_safe_error(caught.value)
