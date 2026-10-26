"""Exercise candidate inventory transport through actual SDK signing and controlled loopback HTTP.

These tests are unexecuted source candidates. The fixture binds an actual
transport-issued key token to a synthetic public proof; it does not claim a
real SQL commit or integration with the future durable inventory producer.
All HTTP sockets terminate at the fixture's loopback server.
"""

import base64
import copy
import hashlib
import hmac
import json
import pickle
import threading
import traceback
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from typing import cast
from urllib.parse import urlsplit

import pytest
from kraken.futures import User
from pydantic import ValidationError
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.adapters import BaseAdapter
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase

import snapper.infrastructure.exchanges.futures_inventory_wire as wire
import snapper.infrastructure.exchanges.kraken_futures_account_receipts as account_receipts
from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.infrastructure.exchanges.futures_inventory_wire import FuturesInventoryWireError
from snapper.infrastructure.exchanges.futures_inventory_wire import FuturesInventoryWireReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import read_futures_inventory_wire
from snapper.infrastructure.exchanges.futures_inventory_wire import (
    require_guarded_inventory_receipt,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    read_futures_prepared_key_identity,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.network.egress_context import egress_identity

_KEY = "synthetic-inventory-key-not-for-production"
_SECRET = "c3ludGhldGljLWludmVudG9yeS1zZWNyZXQ="
_ACCOUNT_UID = "00000000-0000-7000-8000-000000000006"
_KEY_PATH = "/api/auth/v1/api-keys/v3/check"
_ORDERS_PATH = "/derivatives/api/v3/openorders"
_POSITIONS_PATH = "/derivatives/api/v3/openpositions"
_PURPOSES: tuple[InventoryReadPurpose, ...] = (
    "kraken_futures.open_orders",
    "kraken_futures.open_positions",
)
_PATHS: dict[InventoryReadPurpose, str] = {
    "kraken_futures.open_orders": _ORDERS_PATH,
    "kraken_futures.open_positions": _POSITIONS_PATH,
}
_ARRAYS: dict[InventoryReadPurpose, str] = {
    "kraken_futures.open_orders": "openOrders",
    "kraken_futures.open_positions": "openPositions",
}
type _SignedRequest = tuple[str, str, dict[str, str], str, str]


def _uuid(number: int) -> str:
    """Allocate deterministic public fixture UUIDs without involving a live account."""
    return f"00000000-0000-7000-8000-{number:012x}"


def _selection(*, demo: bool = False) -> ExecutionClientSelection:
    """Retain an explicit synthetic credential revision and the exact supported provider origin."""
    return ExecutionClientSelection(
        wallet_public_id=_uuid(1),
        exchange="kraken_futures",
        mode="live",
        environment="demo" if demo else "production",
        credential_public_id=_uuid(2),
        credential_timestamp=datetime.now(UTC) - timedelta(days=1),
        public_api_origin=(
            "https://demo-futures.kraken.com" if demo else "https://futures.kraken.com"
        ),
    )


def _body(purpose: InventoryReadPurpose) -> bytes:
    """Provide complete purpose-specific inventory while preserving exact numeric lexemes."""
    if purpose == "kraken_futures.open_orders":
        return (
            b'{ "result":"success", "serverTime":"2026-10-25T00:00:00.123456Z",'
            b' "openOrders":[{"order_id":"00000000-0000-7000-8000-000000000010",'
            b'"cliOrdId":"original-command","symbol":"PF_XBTUSD","side":"buy",'
            b'"orderType":"lmt","status":"untouched","filledSize":0.125000000000000000001,'
            b'"unfilledSize":0.375,"limitPrice":12345.000000000000000000001,"reduceOnly":false,'
            b'"receivedTime":"2026-10-24T23:59:58Z","lastUpdateTime":"2026-10-24T23:59:59Z"}] }'
        )
    return (
        b'{ "result":"success", "serverTime":"2026-10-25T00:00:00.123456Z",'
        b' "openPositions":[{"symbol":"FF_XBTUSD_260925","side":"short",'
        b'"size":1.250000000000000000001,"price":12345.000000000000000000001,'
        b'"unrealizedPnl":-2.5,"unrealizedFunding":null,"pnlCurrency":"USD"}] }'
    )


def _empty_body(purpose: InventoryReadPurpose) -> dict[str, object]:
    """Construct only known inventory envelope fields for adversarial privacy extensions."""
    return {"result": "success", "serverTime": "2026-10-25T00:00:00Z", _ARRAYS[purpose]: []}


@dataclass
class _Source:
    """Controlled HTTP server data, containing synthetic request metadata only."""

    purpose: InventoryReadPurpose
    status: int = 200
    body: bytes | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    body_factory: Callable[[Mapping[str, str]], bytes] | None = None
    requests: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def response_body(self, headers: Mapping[str, str]) -> bytes:
        """Return request-dependent synthetic echo content or original fixture bytes."""
        if self.body_factory is not None:
            return self.body_factory(headers)
        return _body(self.purpose) if self.body is None else self.body


def _handler(source: _Source) -> type[BaseHTTPRequestHandler]:
    """Create a loopback-only provider handler without reaching any official endpoint."""

    class Handler(BaseHTTPRequestHandler):
        """Supply exact HTTP entities through the original SDK Session's local adapter."""

        def do_GET(self) -> None:
            """Answer actual key checks and the two fixed inventory paths with controlled HTTP."""
            headers = dict(self.headers.items())
            source.requests.append((self.path, headers))
            if self.path == _KEY_PATH:
                status, body = (
                    200,
                    json.dumps(
                        {
                            "apiKey": _KEY,
                            "accountUid": _ACCOUNT_UID,
                            "permissions": {"general": "READ_ONLY", "transfer": "NO_ACCESS"},
                        }
                    ).encode(),
                )
            else:
                assert self.path in {_ORDERS_PATH, _POSITIONS_PATH}
                status, body = source.status, source.response_body(headers)
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for name, value in source.response_headers.items():
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def log_message(self, format_string: str, *args: object) -> None:
            """Keep synthetic HTTP access metadata out of ordinary test output."""

    return Handler


class _LoopbackAdapter(BaseAdapter):
    """Preserve original PreparedRequest guards while sending its fixture copy only to loopback."""

    def __init__(self, address: tuple[str, int]) -> None:
        """Own a standard zero-retry HTTP adapter for controlled local socket I/O."""
        self.address = address
        self.inner = HTTPAdapter(max_retries=0)
        self.inventory_calls: list[PreparedRequest] = []
        self.responses: list[Response] = []
        self.after_inventory: Callable[[PreparedRequest, Response], None] | None = None

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Send a copy of the actually guarded signed request to the test-owned HTTP server.

        Args:
            request: Original final SDK request whose exact provider URL/key was already guarded.
            args: Original Requests adapter positional arguments.
            kwargs: Actual stream, timeout and proxy settings from the original routed Session.

        Returns:
            A genuine streaming HTTP response with its original logical provider request identity.
        """
        parsed = urlsplit(request.url or "")
        assert parsed.scheme == "https"
        assert parsed.netloc in {"futures.kraken.com", "demo-futures.kraken.com"}
        assert parsed.path in {_KEY_PATH, _ORDERS_PATH, _POSITIONS_PATH}
        assert request.method == "GET" and request.body is None and not parsed.query
        forwarded = request.copy()
        forwarded.url = f"http://{self.address[0]}:{self.address[1]}{parsed.path}"
        forwarded.hooks = {"response": []}
        response = self.inner.send(
            forwarded,
            stream=True,
            timeout=cast(tuple[int, int], kwargs["timeout"]),
            proxies={},
        )
        response.url = request.url or ""
        response.request = request
        self.responses.append(response)
        if parsed.path != _KEY_PATH:
            self.inventory_calls.append(request)
            if self.after_inventory is not None:
                self.after_inventory(request, response)
        return response

    def close(self) -> None:
        """Close every locally owned adapter pool after the original User Session closes."""
        self.inner.close()


@dataclass
class _Scenario:
    """Original SDK objects and actual issued token alongside controlled local HTTP state."""

    source: _Source
    user: User
    selection: ExecutionClientSelection
    session: Session
    adapter: _LoopbackAdapter
    token: PreparedKeyToken
    observation: NativeAccountPublicIdentity

    def read(self) -> GuardedFuturesInventoryReceipt:
        """Invoke the genuine candidate leaf for this fixture's exact selected endpoint."""
        return read_futures_inventory_wire(
            self.user, self.selection, self.token, _uuid(30), self.source.purpose
        )


def _bind_fixture_token(
    user: User, selected: ExecutionClientSelection
) -> tuple[PreparedKeyToken, NativeAccountPublicIdentity]:
    """Obtain an actual HTTP key token and bind fixture proof shape without claiming SQL."""
    attempt = NativeAccountReadAttemptPayload(
        client_selection=selected, family_sequence=1, started_at=datetime.now(UTC)
    )
    prepared = read_futures_prepared_key_identity(user, selected)
    proof = NativeAccountIdentityProofPayload(
        attempt_public_id=_uuid(20),
        attempt_content_digest=native_account_read_attempt_digest(attempt),
        attempt=attempt,
        observation=prepared.observation,
        recorded_at=datetime.now(UTC),
        owner_root=None,
    )
    prepared.token.bind_committed_proof(_uuid(21), proof)
    return prepared.token, prepared.observation


@contextmanager
def _scenario(purpose: InventoryReadPurpose, *, demo: bool = False) -> Iterator[_Scenario]:
    """Own the loopback server, original SDK objects and same-operation key token."""
    source = _Source(purpose)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(source))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    user = User(key=_KEY, secret=_SECRET, sandbox=demo)
    session = cast(Session, getattr(user, "_FuturesClient__session"))
    session.trust_env = False
    adapter = _LoopbackAdapter(("127.0.0.1", server.server_port))
    session.mount("https://", adapter)
    selected = _selection(demo=demo)
    token: PreparedKeyToken | None = None
    try:
        with egress_identity(
            exchange="kraken_futures", traffic_class="private", owner="test", operation="inventory"
        ):
            token, observation = _bind_fixture_token(user, selected)
            yield _Scenario(source, user, selected, session, adapter, token, observation)
    finally:
        if token is not None:
            token.close()
        session.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _assert_closed(scenario: _Scenario) -> None:
    """Require all actual local HTTP entities to be closed on either receipt issuance or refusal."""
    assert scenario.adapter.responses
    assert all(response.raw.closed for response in scenario.adapter.responses)


def _assert_safe_failure(error: FuturesInventoryWireError) -> None:
    """Ensure ordinary traceback formatting carries no synthetic known credential values."""
    rendered = "".join(traceback.format_exception(error)) + str(error) + repr(error)
    assert _KEY not in rendered and _SECRET not in rendered
    assert error.__cause__ is None
    assert error.reason in {"transport_error", "http_error", "source_invalid", "source_unsafe"}


@pytest.mark.parametrize("purpose", _PURPOSES)
@pytest.mark.parametrize("demo", (False, True))
def test_actual_read_only_sdk_http_preserves_original_inventory_source(
    purpose: InventoryReadPurpose, demo: bool
) -> None:
    """Issue source only after genuine final-request signing and loopback HTTP.

    Given: A real SDK User, READ_ONLY key proof and supported production/demo public selection.
    When: Its original Session performs the fixed signed inventory GET through controlled HTTP.
    Then: One inventory GET issues original bytes/clocks without an order-access prerequisite.
    """
    with _scenario(purpose, demo=demo) as scenario:
        actual = scenario.read()
        receipt = require_guarded_inventory_receipt(actual)
        assert receipt.entity_bytes == _body(purpose)
        assert receipt.inventory_attempt_public_id == _uuid(30)
        assert receipt.purpose == purpose and receipt.client_selection == scenario.selection
        assert receipt.key_proof == scenario.token.require_proof_reference()
        assert receipt.key_proof.recorded_at <= receipt.request_started_at <= receipt.observed_at
        assert scenario.observation.general_access == "READ_ONLY"
        assert len(scenario.adapter.inventory_calls) == 1
        request = scenario.adapter.inventory_calls[0]
        assert request.url == cast(str, scenario.selection.public_api_origin) + _PATHS[purpose]
        signed = hashlib.sha256(
            (
                cast(str, request.headers["Nonce"]) + _PATHS[purpose].removeprefix("/derivatives")
            ).encode()
        ).digest()
        expected = base64.b64encode(
            hmac.new(base64.b64decode(_SECRET), signed, hashlib.sha512).digest()
        ).decode()
        assert request.headers["Authent"] == expected
        assert request.headers["APIKey"] == _KEY
        if purpose == "kraken_futures.open_orders":
            orders = parse_futures_open_orders(receipt.entity_bytes).orders
            assert len(orders) == 1
            assert orders[0].side == "buy"
            assert orders[0].filled_size == Decimal("0.125000000000000000001")
            assert orders[0].limit_price.value == Decimal("12345.000000000000000000001")
        else:
            positions = parse_futures_open_positions(receipt.entity_bytes).positions
            assert len(positions) == 1
            assert positions[0].side == "short"
            assert positions[0].size == Decimal("1.250000000000000000001")
            assert positions[0].average_entry_price == Decimal("12345.000000000000000000001")
        public = receipt.model_dump_json() + repr(actual)
        assert _KEY not in public and _SECRET not in public and expected not in public
        _assert_closed(scenario)


@pytest.mark.parametrize("purpose", _PURPOSES)
@pytest.mark.parametrize("secret_kind", ("key", "signature", "secret", "nonce"))
@pytest.mark.parametrize("escaped", (False, True))
def test_actual_known_auth_echo_under_innocent_nested_values_never_issues_receipt(
    purpose: InventoryReadPurpose, secret_kind: str, escaped: bool
) -> None:
    """Classify same-operation credentials before source receipt creation or retention.

    Given: Actual same-key HTTP returns credential text under an unknown nested description member.
    When: Key, signature, nonce or secret text returns literally or through JSON escapes.
    Then: The complete response is closed and only source_unsafe failure metadata leaves the leaf.
    """
    with _scenario(purpose) as scenario:

        def echoed_body(headers: Mapping[str, str]) -> bytes:
            """Use the actual request signature without guessing a fixture signature value."""
            value = {
                "key": headers["APIKey"],
                "signature": headers["Authent"],
                "secret": _SECRET,
                "nonce": headers["Nonce"],
            }[secret_kind]
            encoded = (
                "".join(f"\\u{ord(character):04x}" for character in value) if escaped else value
            )
            prefix = json.dumps(_empty_body(purpose), separators=(",", ":")).encode()[:-1]
            return prefix + b',"safe":[{"description":"prefix-' + encoded.encode() + b'-suffix"}]}'

        scenario.source.body_factory = echoed_body
        before = len(wire._ISSUED_INVENTORY_RECEIPTS)
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_unsafe"
        assert len(wire._ISSUED_INVENTORY_RECEIPTS) == before
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


@pytest.mark.parametrize(
    ("body", "reason"),
    (
        (b"<html>not inventory</html>", "source_invalid"),
        (b"\xff", "source_invalid"),
        (b"[]", "source_invalid"),
        (
            b'{"result":"success","serverTime":"x","openOrders":[],"safe":"\\u0000"}',
            "source_invalid",
        ),
        (b'{"result":"success","serverTime":"x","openOrders":[],"n":NaN}', "source_invalid"),
        (
            b'{"result":"success","serverTime":"x","openOrders":[],"safe":1,"safe":2}',
            "source_invalid",
        ),
        (
            (b'{"result":"success","serverTime":"x","openOrders":[],"nested":{"x":1,"x":2}}'),
            "source_invalid",
        ),
        (b'{"result":"success","serverTime":"x","openPositions":[]}', "source_invalid"),
        (
            (
                b'{"result":"success","serverTime":"x","openOrders":[],'
                b'"nested":{"aPi\\u004bey":"x"}}'
            ),
            "source_unsafe",
        ),
        (
            (
                b'{"result":"success","serverTime":"x","openOrders":[],'
                b'"nested":{"method":"subscribe"}}'
            ),
            "source_unsafe",
        ),
        (
            (
                b'{"result":"success","serverTime":"x","openOrders":[],'
                b'"nested":{"safe":"\\ud800"}}'
            ),
            "source_invalid",
        ),
    ),
)
def test_unknown_duplicate_malformed_or_auth_inventory_never_becomes_public_source(
    body: bytes, reason: str
) -> None:
    """Reject complete bodies rather than retain partially checked or overwritten content.

    Given: Controlled HTTP returns malformed, duplicate, wrong-purpose or all-depth control content.
    When: Actual inventory transport inspects the original entity before receipt construction.
    Then: Its enumerated refusal exposes neither source bytes nor a retained body digest.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        scenario.source.body = body
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == reason
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


@pytest.mark.parametrize("limit", ("_MAX_ENTITY_BYTES", "_MAX_FIELDS", "_MAX_NODES", "_MAX_DEPTH"))
def test_actual_http_body_cannot_bypass_complete_inventory_privacy_resource_bounds(
    monkeypatch: pytest.MonkeyPatch, limit: str
) -> None:
    """Refuse every unchecked suffix when complete body inspection exceeds one explicit bound.

    Given: Real inventory HTTP returns through one deliberately small classification bound.
    When: The candidate classifies the whole original HTTP entity.
    Then: No partial receipt issues and the response closes with safe failure metadata.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        monkeypatch.setattr(wire, limit, 1)
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_invalid"
        _assert_closed(scenario)


@pytest.mark.parametrize("status", (301, 401, 429, 500))
def test_http_errors_or_redirects_never_issue_original_inventory_success(status: int) -> None:
    """Refuse status and redirect responses before promoting their sensitive error bodies.

    Given: Actual local HTTP returns an error or redirect with credential-bearing provider content.
    When: The fixed endpoint read completes its sole physical request.
    Then: No redirect target is contacted and the closed response yields only http_error metadata.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        scenario.source.status = status
        scenario.source.body = _KEY.encode()
        scenario.source.response_headers = {"Location": "https://different.invalid/private"}
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "http_error"
        assert len(scenario.adapter.inventory_calls) == 1
        assert len(scenario.source.requests) == 2
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


@pytest.mark.parametrize(
    ("index", "replacement"),
    ((0, "POST"), (1, "https://different.invalid/x"), (2, {}), (3, "body=x"), (4, "query=x")),
)
def test_sdk_signer_changes_refuse_before_inventory_http(
    monkeypatch: pytest.MonkeyPatch, index: int, replacement: str | dict[str, str]
) -> None:
    """Do not widen the fixed transport when the original SDK returns changed request coordinates.

    Given: Genuine same-key preparation already issued the original owner token.
    When: SDK signing changes the method, path/origin, authentication inputs, payload or query.
    Then: The actual HTTP adapter receives no inventory request.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        original_prepare = scenario.user._prepare_request

        def altered_prepare(
            method: str,
            uri: str,
            post_params: dict[str, object],
            query_params: dict[str, object],
            **options: object,
        ) -> _SignedRequest:
            """Alter only one genuine SDK boundary result after preserving real signing behavior."""
            assert options == {"auth": True}
            values = list(original_prepare(method, uri, post_params, query_params, auth=True))
            values[index] = replacement
            return cast(_SignedRequest, tuple(values))

        monkeypatch.setattr(scenario.user, "_prepare_request", altered_prepare)
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        assert not scenario.adapter.inventory_calls
        _assert_safe_failure(caught.value)


class _ChangedFinalRequest(AuthBase):
    """Mutate the actual Requests-prepared object after SDK signing and session merging."""

    def __init__(self, field_name: str) -> None:
        """Retain only the selected fixture fault, without constructing another request path."""
        self.field_name = field_name

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Change one protected request field before the original final-key guard."""
        if self.field_name == "method":
            request.method = "POST"
        elif self.field_name == "url":
            request.url = "https://different.invalid/private"
        elif self.field_name == "body":
            request.body = b"changed=body"
        else:
            request.headers[self.field_name] = "different-synthetic-value"
        return request


@pytest.mark.parametrize(
    "field_name", ("APIKey", "Authent", "Nonce", "Authorization", "method", "url", "body")
)
def test_final_requests_authentication_cannot_change_the_guarded_inventory_request(
    monkeypatch: pytest.MonkeyPatch, field_name: str
) -> None:
    """Check actual final request preparation rather than trusting the earlier SDK header tuple.

    Given: An original token and SDK Session whose Requests auth callback mutates a protected field.
    When: The inventory request reaches the final prepared-object guard.
    Then: No altered request reaches physical local HTTP.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        monkeypatch.setattr(scenario.session, "auth", _ChangedFinalRequest(field_name))
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        assert not scenario.adapter.inventory_calls
        _assert_safe_failure(caught.value)


class _SwapSession(AuthBase):
    """Replace the SDK Session while Requests still prepares the original inventory request."""

    def __init__(self, user: User, replacement: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        """Retain fixture-owned runtime objects only for the exact pre-send mutation seam."""
        self.user = user
        self.replacement = replacement
        self.monkeypatch = monkeypatch

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Leave encoded key unchanged while moving the mutable SDK Session pointer."""
        self.monkeypatch.setattr(self.user, "_FuturesClient__session", self.replacement)
        return request


def test_original_session_is_required_immediately_before_physical_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unchanged API key cannot substitute for ownership of the original routed Session.

    Given: Actual token/key proof and original SDK Session exist.
    When: Requests authentication swaps only the SDK Session before the final guard.
    Then: The original HTTP adapter receives no inventory request.
    """
    with _scenario("kraken_futures.open_orders") as scenario, Session() as replacement:
        monkeypatch.setattr(
            scenario.session, "auth", _SwapSession(scenario.user, replacement, monkeypatch)
        )
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        assert not scenario.adapter.inventory_calls
        _assert_safe_failure(caught.value)


@pytest.mark.parametrize(
    "fault", ("response_url", "response_history", "session", "key", "token_closed")
)
def test_response_completion_rechecks_original_route_session_and_token(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """A genuine HTTP response cannot issue success after same-operation provenance changes.

    Given: Original inventory GET physically returned through the genuine local HTTP adapter.
    When: Its response route/history, original SDK Session, key or token changes before issuance.
    Then: The actual response closes and fixed failure metadata replaces receipt success.
    """
    with _scenario("kraken_futures.open_orders") as scenario, Session() as replacement:

        def change_after_response(request: PreparedRequest, response: Response) -> None:
            """Mutate the actual synchronous response boundary without replacing its HTTP bytes."""
            if fault == "response_url":
                response.url = "https://different.invalid/response"
            elif fault == "response_history":
                response.history = [Response()]
            elif fault == "session":
                monkeypatch.setattr(scenario.user, "_FuturesClient__session", replacement)
            elif fault == "key":
                monkeypatch.setattr(scenario.user, "_key", "different-synthetic-key")
            else:
                scenario.token.close()

        scenario.adapter.after_inventory = change_after_response
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


@pytest.mark.parametrize(
    "fault", ("different_user", "closed_token", "constructed_token", "selection")
)
def test_original_owner_identity_is_required_before_any_inventory_send(fault: str) -> None:
    """Refuse new SDK objects and unissued/closed tokens despite matching public source copies.

    Given: Genuine key HTTP issued and proof-bound the fixture's original token.
    When: A different User, closed/unissued token or changed credential revision is supplied.
    Then: No inventory GET reaches physical transport and only safe metadata is exposed.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        user, token, selected = scenario.user, scenario.token, scenario.selection
        other_user = User(key=_KEY, secret=_SECRET)
        other_session = cast(Session, getattr(other_user, "_FuturesClient__session"))
        other_session.trust_env = False
        other_session.mount("https://", scenario.adapter)
        try:
            if fault == "different_user":
                user = other_user
            elif fault == "closed_token":
                token.close()
            elif fault == "constructed_token":
                token = PreparedKeyToken(user, _KEY, scenario.observation)
            else:
                selected = ExecutionClientSelection.model_validate(
                    selected.model_dump(mode="python") | {"credential_public_id": _uuid(99)}
                )
            with pytest.raises(FuturesInventoryWireError) as caught:
                read_futures_inventory_wire(
                    user, selected, token, _uuid(30), scenario.source.purpose
                )
            assert caught.value.reason == "transport_error"
            assert not scenario.adapter.inventory_calls
            _assert_safe_failure(caught.value)
        finally:
            if token is not scenario.token:
                token.close()
            other_session.close()


@pytest.mark.parametrize(
    ("attempt_id", "purpose"),
    (("not-a-uuid", "kraken_futures.open_orders"), (_uuid(30), "accounts")),
)
def test_invalid_request_coordinates_refuse_before_sdk_or_http(
    attempt_id: str, purpose: str
) -> None:
    """Do not perform an inventory read whose caller coordinates cannot identify its own source.

    Given: A genuine prepared owner token remains available.
    When: The requested inventory attempt UUID or purpose is unsupported.
    Then: No additional physical HTTP call occurs.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        before = len(scenario.source.requests)
        with pytest.raises(FuturesInventoryWireError):
            read_futures_inventory_wire(
                scenario.user,
                scenario.selection,
                scenario.token,
                attempt_id,
                cast(InventoryReadPurpose, purpose),
            )
        assert len(scenario.source.requests) == before
        assert not scenario.adapter.inventory_calls


class _EqualityAlias(GuardedFuturesInventoryReceipt):
    """Represent an unissued caller subclass attempting WeakKeyDictionary equality aliasing."""

    original: GuardedFuturesInventoryReceipt

    def __init__(self, original: GuardedFuturesInventoryReceipt) -> None:
        """Copy public source shape and the original reference for the adversarial alias."""
        super().__init__(require_guarded_inventory_receipt(original))
        object.__setattr__(self, "original", original)

    def __hash__(self) -> int:
        """Match the existing issued object's hash without acquiring its runtime identity."""
        return hash(self.original)

    def __eq__(self, other: object) -> bool:
        """Try to compare equal to the genuine issued object while remaining another object."""
        return other is self.original or other is self


def test_only_the_unchanged_exact_issued_object_exposes_inventory_source() -> None:
    """Reject public shapes, equality aliases and unchecked changes at the actual issuance boundary.

    Given: Genuine guarded inventory HTTP issued one original opaque object.
    When: Callers reconstruct its shape, subclass equality or mutate a returned public copy.
    Then: Only the exact unchanged issued object supplies original isolated source bytes.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        actual = scenario.read()
        view = require_guarded_inventory_receipt(actual)
        for forged in (
            object(),
            view,
            GuardedFuturesInventoryReceipt(view),
            _EqualityAlias(actual),
        ):
            with pytest.raises(FuturesInventoryWireError):
                require_guarded_inventory_receipt(forged)
        with pytest.raises(TypeError, match="serialized"):
            pickle.dumps(actual)
        with pytest.raises(TypeError, match="copied"):
            copy.deepcopy(actual)
        with pytest.raises(AttributeError, match="immutable"):
            actual._receipt = view
        object.__setattr__(view, "entity_bytes", b"changed-copy")
        assert require_guarded_inventory_receipt(actual).entity_bytes == _body(
            scenario.source.purpose
        )
        altered = require_guarded_inventory_receipt(actual).model_copy(
            update={
                "inventory_attempt_public_id": _uuid(31),
                "purpose": "kraken_futures.open_positions",
            }
        )
        object.__setattr__(actual, "_receipt", altered)
        with pytest.raises(FuturesInventoryWireError):
            require_guarded_inventory_receipt(actual)
        _assert_closed(scenario)


@pytest.mark.parametrize("placement", ("key", "number"))
def test_actual_nonce_echo_cannot_hide_as_an_innocent_json_key_or_numeric_lexeme(
    placement: str,
) -> None:
    """Inspect original decoded keys and numeric text as well as ordinary string values.

    Given: Actual SDK inventory signing generated a same-operation nonce.
    When: Controlled HTTP echoes it as an innocent object key or exact numeric source value.
    Then: No public receipt exists and source_unsafe metadata replaces retention.
    """
    with _scenario("kraken_futures.open_orders") as scenario:

        def nonce_body(headers: Mapping[str, str]) -> bytes:
            """Use the actual outgoing nonce and preserve a JSON number when requested."""
            nonce = headers["Nonce"]
            assert nonce.isdigit()
            if placement == "key":
                return json.dumps(
                    _empty_body(scenario.source.purpose) | {"safe": {nonce: None}}
                ).encode()
            prefix = json.dumps(_empty_body(scenario.source.purpose)).encode()[:-1]
            return prefix + b',"safe":{"observation":' + nonce.encode() + b"}}"

        scenario.source.body_factory = nonce_body
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_unsafe"
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


def test_secret_visible_after_synchronous_response_mutation_is_classified_transiently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep token key/User identity proof separate from inspection of a changed SDK secret slot.

    Given: Actual HTTP was signed with the original User/key and original secret.
    When: Response completion changes and echoes the SDK secret under an innocent member.
    Then: Privacy refuses the value without claiming the token itself pins the secret.
    """
    changed_secret = "c3ludGhldGljLW5ldy1zZWNyZXQ="
    with _scenario("kraken_futures.open_orders") as scenario:
        scenario.source.body = json.dumps(
            _empty_body(scenario.source.purpose) | {"safe": {"note": changed_secret}}
        ).encode()

        def change_secret(request: PreparedRequest, response: Response) -> None:
            """Change only the User's transient secret after original HTTP returns."""
            monkeypatch.setattr(scenario.user, "_secret", changed_secret)

        scenario.adapter.after_inventory = change_secret
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_unsafe"
        assert changed_secret not in "".join(traceback.format_exception(caught.value))
        _assert_closed(scenario)


def test_actual_decoded_entity_limit_refuses_an_oversized_http_response() -> None:
    """Keep the actual bounded HTTP reader in the rejection path instead of mocking its result.

    Given: Controlled HTTP supplies more than eight MiB of original decoded response content.
    When: The genuine streaming read reaches its fixed entity limit.
    Then: The response closes without a source receipt or retained digest.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        scenario.source.body = b" " * (8 * 1024 * 1024 + 1)
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_invalid"
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_closed(scenario)


def test_original_stream_failure_closes_actual_response_and_sanitizes_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An HTTP entity read failure cannot leak provider-controlled exception text.

    Given: The original guarded request returned a genuine streaming local HTTP response.
    When: The body iterator fails after a partial chunk with credential-bearing error text.
    Then: Only transport_error metadata escapes and the owned original response closes.
    """
    with _scenario("kraken_futures.open_orders") as scenario:

        def fail_stream(chunk_size: int = 1, decode_unicode: bool = False) -> Iterator[bytes]:
            """Raise at the actual Response content boundary after yielding an incomplete entity."""
            yield b'{"result":"success"'
            raise ValueError(_KEY + _SECRET)

        def install_failure(request: PreparedRequest, response: Response) -> None:
            """Keep the actual local response while changing only its body-read failure boundary."""
            monkeypatch.setattr(response, "iter_content", fail_stream)

        scenario.adapter.after_inventory = install_failure
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


def test_actually_issued_but_unbound_owner_token_cannot_send_inventory() -> None:
    """Actual key HTTP alone cannot substitute for a proof-bound original runtime owner token.

    Given: Another genuine key check issued a token which has not been bound to a public proof.
    When: It is offered to the fixed inventory leaf without proof binding.
    Then: No inventory HTTP occurs and its unhanded token is explicitly closed.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        prepared = read_futures_prepared_key_identity(scenario.user, scenario.selection)
        try:
            with pytest.raises(FuturesInventoryWireError) as caught:
                read_futures_inventory_wire(
                    scenario.user,
                    scenario.selection,
                    prepared.token,
                    _uuid(30),
                    scenario.source.purpose,
                )
            assert caught.value.reason == "transport_error"
            assert not scenario.adapter.inventory_calls
            _assert_safe_failure(caught.value)
            _assert_closed(scenario)
        finally:
            prepared.token.close()


def test_actual_response_clock_before_committed_proof_is_a_source_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinguish contradictory response evidence from a failure to perform its actual HTTP.

    Given: The real SDK and guarded Requests session complete one signed inventory GET.
    When: Its transport clock reports request start before the original proof was recorded.
    Then: Receipt validation returns source_invalid and closes the genuine response.
    """
    actual_request = wire._request_guarded_original_response

    def reversed_clock(
        session: Session,
        expected_url: str,
        sdk_headers: Mapping[str, str],
        guard: Callable[[PreparedRequest, datetime], PreparedKeyProofReference],
    ) -> tuple[Response, PreparedKeyProofReference, datetime]:
        """Retain the real response and proof while exposing a contradictory source clock."""
        response, proof, _ = actual_request(session, expected_url, sdk_headers, guard)
        return response, proof, proof.recorded_at - timedelta(microseconds=1)

    with _scenario("kraken_futures.open_orders") as scenario:
        monkeypatch.setattr(wire, "_request_guarded_original_response", reversed_clock)
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "source_invalid"
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)


def _bounded_inventory_body(boundary: str, extra: int) -> bytes:
    """Place one original complete inventory at its resource bound, including field-heavy trees."""
    document = _empty_body("kraken_futures.open_orders")
    if boundary == "nodes":
        document["safe"] = [0] * (65531 + extra)
    elif boundary == "fields":
        document.update({f"safe_{index}": 0 for index in range(65532 + extra)})
    elif boundary == "depth":
        nested: object = "benign"
        for _ in range(63 + extra):
            nested = [nested]
        document["safe"] = nested
    encoded = json.dumps(document, separators=(",", ":")).encode()
    if boundary == "body":
        return encoded + b" " * (8 * 1024 * 1024 + extra - len(encoded))
    return encoded


@pytest.mark.parametrize("boundary", ("nodes", "fields", "depth", "body"))
@pytest.mark.parametrize("extra", (0, 1))
def test_complete_http_and_inventory_parser_share_resource_boundaries(
    boundary: str, extra: int
) -> None:
    """Keep privacy inspection and semantic parsing consistent at their actual full limits.

    Given: A complete empty inventory reaches its node, field-heavy tree, depth or byte bound.
    When: Real signed HTTP delivers that entity or exactly one additional element.
    Then: Both boundaries accept the exact limit and the transport refuses every oversized source.
    """
    body = _bounded_inventory_body(boundary, extra)
    with _scenario("kraken_futures.open_orders") as scenario:
        scenario.source.body = body
        if extra:
            with pytest.raises(FuturesInventoryWireError) as caught:
                scenario.read()
            assert caught.value.reason == "source_invalid"
        else:
            receipt = require_guarded_inventory_receipt(scenario.read())
            assert receipt.entity_bytes == body
            assert parse_futures_open_orders(body).orders == ()
        _assert_closed(scenario)


def test_public_inventory_view_cannot_rebind_another_selected_credential() -> None:
    """Validate the public source shape without treating it as a new issued transport capability.

    Given: Real signed HTTP has issued an inventory receipt for the original prepared key proof.
    When: A caller copies that public view with another selected credential UUID.
    Then: Shape validation refuses the contradiction and the original issued receipt stays unchanged.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        issued = scenario.read()
        original = require_guarded_inventory_receipt(issued)
        changed = original.client_selection.model_copy(update={"credential_public_id": _uuid(99)})
        with pytest.raises(ValidationError, match="source_invalid"):
            FuturesInventoryWireReceipt.model_validate(
                original.model_dump(mode="python") | {"client_selection": changed}
            )
        assert require_guarded_inventory_receipt(issued) == original
        _assert_closed(scenario)


def test_actual_stream_deadline_refuses_without_issuing_a_partial_inventory_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve the actual bounded-stream deadline as a sanitized transport refusal.

    Given: The real SDK request returned its actual loopback streaming response.
    When: The trusted monotonic clock crosses the read deadline before its first chunk is consumed.
    Then: The real entity reader refuses, no receipt issues and the response closes.
    """
    with _scenario("kraken_futures.open_orders") as scenario:

        def expire_deadline(request: PreparedRequest, response: Response) -> None:
            """Advance only the original entity reader's trusted clock after genuine HTTP returns."""
            monkeypatch.setattr(account_receipts, "monotonic", lambda: float("inf"))

        scenario.adapter.after_inventory = expire_deadline
        with pytest.raises(FuturesInventoryWireError) as caught:
            scenario.read()
        assert caught.value.reason == "transport_error"
        assert len(scenario.adapter.inventory_calls) == 1
        _assert_safe_failure(caught.value)
        _assert_closed(scenario)
