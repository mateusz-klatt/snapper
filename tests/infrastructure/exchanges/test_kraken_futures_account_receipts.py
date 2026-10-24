"""Signed native account reads retain exact entities and never follow authenticated redirects."""

import asyncio
import base64
import gzip
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
from requests import Response
from requests import Session
from requests.adapters import BaseAdapter
from urllib3.response import HTTPResponse

import snapper.infrastructure.exchanges.kraken_futures_account_receipts as transport
import snapper.infrastructure.exchanges.kraken_rest_egress as routing
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    NativeAccountsBodyPrivacyError,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    FuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    NativeAccountsTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    capture_futures_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    read_futures_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import egress_identity

_ORIGIN = "https://futures.kraken.com"
_PATH = "/derivatives/api/v3/accounts"
_WIRE = b'{"result":"success","accounts":{"cash":{"balances":{"USD":0.123456789123456789}}}}'
_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)


def _selection(*, demo: bool = False, **updates: object) -> ExecutionClientSelection:
    """Use public synthetic credential metadata paired with the selected real SDK route."""
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


class _Adapter(BaseAdapter):
    """Exercise actual requests preparation and redirect handling without opening a socket."""

    def __init__(self, body: bytes = _WIRE, status: int = 200) -> None:
        """Retain synthetic source bytes and request metadata solely inside the test fixture."""
        self.body = body
        self.status = status
        self.headers: dict[str, str] = {}
        self.calls: list[PreparedRequest] = []
        self.options: list[dict[str, object]] = []
        self.responses: list[Response] = []

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Supply a genuine streaming requests response to its normal redirect machinery.

        Args:
            request: Prepared authenticated request, containing synthetic fixture headers only.
            args: External transport adapter positional arguments.
            kwargs: External transport adapter timeout and stream settings.

        Returns:
            Real response whose body can be decompressed by the ordinary HTTP library.
        """
        self.calls.append(request)
        self.options.append(kwargs)
        response = Response()
        response.request = request
        response.url = cast(str, request.url)
        response.status_code = self.status
        response.headers.update(self.headers)
        response.raw = HTTPResponse(
            body=BytesIO(self.body),
            headers=self.headers,
            status=self.status,
            preload_content=False,
        )
        self.responses.append(response)
        return response

    def close(self) -> None:
        """Own no external connections or background resources."""


@contextmanager
def _client(adapter: _Adapter, *, demo: bool = False) -> Iterator[User]:
    """Construct the real pinned SDK with synthetic credentials and an entirely local transport."""
    user = User(key="synthetic-key", secret="c3ludGhldGljLXNlY3JldA==", sandbox=demo)
    session = cast(Session, getattr(user, "_FuturesClient__session"))
    session.trust_env = False
    session.mount("https://", adapter)
    try:
        yield user
    finally:
        session.close()


def _read(
    user: User, selection: ExecutionClientSelection | None = None
) -> FuturesAccountsWireReceipt:
    """Apply the same explicit private exchange context required from the real egress dispatcher."""
    with egress_identity(
        exchange="kraken_futures",
        traffic_class="private",
        owner="test",
        operation="accounts",
    ):
        return read_futures_accounts_wire(user, selection or _selection())


@pytest.mark.parametrize("demo", (False, True))
@pytest.mark.parametrize("compressed", (False, True))
def test_actual_signed_session_preserves_entity_precision_and_environment(
    demo: bool, compressed: bool
) -> None:
    """Real SDK authentication and HTTP decoding preserve exact JSON decimal text in both environments.

    Given: A production or demo SDK client and a local plain or gzip HTTP response,
    When: the actual signed request traverses the real requests session,
    Then: exact decimal text and selected environment survive unchanged.
    """
    adapter = _Adapter(gzip.compress(_WIRE) if compressed else _WIRE)
    if compressed:
        adapter.headers["Content-Encoding"] = "gzip"
    selected = _selection(demo=demo)
    with _client(adapter, demo=demo) as user:
        receipt = _read(user, selected)
    assert receipt.entity_bytes == _WIRE
    assert receipt.client_selection == selected
    assert receipt.request_started_at <= receipt.observed_at
    assert receipt.status_code == 200
    assert len(adapter.calls) == 1
    request = adapter.calls[0]
    assert request.url == cast(str, selected.public_api_origin) + _PATH
    assert request.method == "GET"
    assert request.headers["APIKey"] == "synthetic-key"
    assert request.headers["Authent"] and request.headers["Nonce"]
    assert request.body is None
    assert adapter.options[0]["timeout"] == (5, 10)
    assert adapter.options[0]["stream"] is True
    assert "synthetic-key" not in repr(receipt)
    assert "0.123456789123456789" not in repr(receipt)


@pytest.mark.parametrize("status", (301, 302, 303, 307, 308))
def test_signed_redirect_never_reaches_second_host(status: int) -> None:
    """An authenticated redirect is retained as HTTP failure without forwarding custom Kraken headers.

    Given: A signed account response redirecting to another host,
    When: the real requests redirect machinery receives it,
    Then: only the original host is contacted and failure source remains available.
    """
    adapter = _Adapter(b"redirect", status)
    adapter.headers["Location"] = "https://different.invalid/collect"
    with _client(adapter) as user:
        receipt = _read(user)
    assert [request.url for request in adapter.calls] == [_ORIGIN + _PATH]
    assert receipt.status_code == status
    assert receipt.entity_bytes == b"redirect"


@pytest.mark.parametrize("body", (b"", b"not JSON", b'{"result":"error","error":"synthetic"}'))
def test_invalid_financial_response_is_still_original_source(body: bytes) -> None:
    """Capture does not drop a malformed or refused HTTP 200 response before its durable source write.

    Given: An empty, malformed or provider-refused HTTP 200 body,
    When: the transport receives its original entity,
    Then: the exact bytes survive for durable capture and later parser refusal.
    """
    with _client(_Adapter(body)) as user:
        assert _read(user).entity_bytes == body


@pytest.mark.parametrize(
    "updates",
    (
        {"exchange": "kraken"},
        {"public_api_origin": "https://different.invalid"},
        {"environment": "demo"},
        {
            "exchange": "paper",
            "mode": "paper",
            "environment": "paper",
            "credential_public_id": None,
            "credential_timestamp": None,
            "public_api_origin": None,
        },
    ),
)
def test_unproven_selected_identity_is_refused_before_transport(
    updates: dict[str, object],
) -> None:
    """A caller's incompatible public selection cannot cause any authenticated HTTP call.

    Given: A client selection with incompatible exchange, environment or origin,
    When: the authenticated read is requested,
    Then: no HTTP request is sent.
    """
    adapter = _Adapter()
    with _client(adapter) as user, pytest.raises(
        NativeAccountsTransportError, match="client_unproven"
    ):
        _read(user, _selection(**updates))
    assert adapter.calls == []


@pytest.mark.parametrize("exchange,traffic", (("kraken", "private"), ("kraken_futures", "public")))
def test_wrong_egress_scope_is_refused(exchange: str, traffic: str) -> None:
    """Native account authentication cannot enter a different venue or public traffic context.

    Given: An egress context for another venue or public traffic,
    When: native account authentication is attempted,
    Then: the request is refused before transport.
    """
    adapter = _Adapter()
    with _client(adapter) as user, egress_identity(
        exchange=exchange,
        traffic_class="private" if traffic == "private" else "public",
        owner="test",
        operation="accounts",
    ), pytest.raises(NativeAccountsTransportError, match="private_egress_required"):
        read_futures_accounts_wire(user, _selection())
    assert adapter.calls == []


def test_missing_egress_scope_is_refused() -> None:
    """A standalone helper call cannot silently bypass the authenticated egress classification.

    Given: No explicit authenticated egress context,
    When: the standalone source helper is called,
    Then: no HTTP request bypasses the required classification.
    """
    adapter = _Adapter()
    with _client(adapter) as user, pytest.raises(
        NativeAccountsTransportError, match="private_egress"
    ):
        read_futures_accounts_wire(user, _selection())
    assert adapter.calls == []


@pytest.mark.parametrize(
    "field,value,reason",
    (
        ("url", "https://different.invalid", "endpoint"),
        ("_FuturesClient__session", None, "session"),
    ),
)
def test_actual_sdk_transport_must_match_selection(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object, reason: str
) -> None:
    """Actual mutable SDK routing/session state is verified before signing or sending.

    Given: A changed SDK origin or an unavailable actual HTTP session,
    When: the native account read starts,
    Then: source provenance fails before signing or sending.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        monkeypatch.setattr(user, field, value)
        with pytest.raises(NativeAccountsTransportError, match=reason):
            _read(user)
    assert adapter.calls == []


@pytest.mark.parametrize(
    "index,value",
    (
        (0, "POST"),
        (1, "https://different.invalid/path"),
        (3, "payload"),
        (4, "query=1"),
    ),
)
def test_prepared_request_is_checked_before_sending(
    monkeypatch: pytest.MonkeyPatch, index: int, value: str
) -> None:
    """A changed signer or mutable SDK URL cannot widen the one permitted HTTP request.

    Given: A signer returning a different method, URL, payload or query,
    When: the prepared account request is checked,
    Then: no widened request reaches the HTTP adapter.
    """
    adapter = _Adapter()
    prepared: list[object] = ["GET", _ORIGIN + _PATH, {}, "", ""]
    prepared[index] = value

    def changed_prepare(*args: object, **kwargs: object) -> tuple[object, ...]:
        return tuple(prepared)

    with _client(adapter) as user:
        monkeypatch.setattr(user, "_prepare_request", changed_prepare)
        with pytest.raises(NativeAccountsTransportError, match="prepared_request_mismatch"):
            _read(user)
    assert adapter.calls == []


@pytest.mark.parametrize("fault", ("decoded", "url", "history", "session"))
def test_unproven_response_cannot_acquire_source_receipt(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Decoded objects, redirected responses and replaced sessions fail source provenance checks.

    Given: A decoded object, redirected response or replaced session,
    When: the source transport checks the result,
    Then: no verified native source receipt is returned.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_FuturesClient__session"))
        original = session.request

        def changed_request(*args: object, **kwargs: object) -> object:
            if fault == "decoded":
                return {"result": "success"}
            response = original("GET", _ORIGIN + _PATH, allow_redirects=False, stream=True)
            if fault == "url":
                response.url = "https://different.invalid/accounts"
            elif fault == "history":
                response.history = [Response()]
            elif fault == "session":
                monkeypatch.setattr(user, "_FuturesClient__session", Session())
            return response

        monkeypatch.setattr(session, "request", changed_request)
        with pytest.raises(NativeAccountsTransportError):
            _read(user)
        if fault == "session":
            cast(Session, getattr(user, "_FuturesClient__session")).close()


@pytest.mark.parametrize("redirect", (False, True))
def test_decoded_body_size_is_bounded(monkeypatch: pytest.MonkeyPatch, redirect: bool) -> None:
    """An oversized decompressed entity cannot become a truncated but apparently valid snapshot.

    Given: An oversized compressed response or redirect body,
    When: the bounded reader decompresses the entity,
    Then: it refuses the complete source without returning a truncated snapshot.
    """
    monkeypatch.setattr(transport, "_MAX_ENTITY_BYTES", 16)
    adapter = _Adapter(gzip.compress(b"x" * 17), 302 if redirect else 200)
    adapter.headers["Content-Encoding"] = "gzip"
    if redirect:
        adapter.headers["Location"] = "https://different.invalid/collect"
    with _client(adapter) as user, pytest.raises(NativeAccountsTransportError, match="too_large"):
        _read(user)


@pytest.mark.parametrize("body", (b"", b"one chunk"))
def test_elapsed_read_deadline_covers_empty_and_partial_bodies(
    monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    """A timed-out source cannot pass as an empty or partially received native inventory.

    Given: A response whose elapsed clock exceeds its budget,
    When: the empty or partial body reaches a deadline checkpoint,
    Then: no apparently complete source receipt is returned.
    """
    ticks = iter((0.0, 31.0))
    monkeypatch.setattr(transport, "monotonic", lambda: next(ticks))
    with _client(_Adapter(body)) as user, pytest.raises(
        NativeAccountsTransportError, match="deadline"
    ):
        _read(user)


def _receipt(status: int = 200, **updates: object) -> FuturesAccountsWireReceipt:
    """Create explicitly timed public receipt evidence without any financial interpretation."""
    return FuturesAccountsWireReceipt.model_validate(
        {
            "client_selection": _selection(),
            "request_started_at": _NOW,
            "observed_at": _NOW + timedelta(seconds=1),
            "status_code": status,
            "entity_bytes": _WIRE,
            **updates,
        }
    )


async def test_capture_awaits_actual_committed_identity_before_return() -> None:
    """Source interpretation waits for the sink; HTTP failures receive a separately refused channel.

    Given: A source receipt and a durable sink paused before acknowledgement,
    When: capture waits while the sink commits,
    Then: only its actual UUID is returned and HTTP failures retain a separate channel.
    """
    entered = asyncio.Event()
    release = asyncio.Event()
    captured: list[PrivateExecutionObservation] = []
    public_id = "00000000-0000-7000-8000-000000000009"

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        entered.set()
        await release.wait()
        return public_id

    receipt = _receipt()
    task = asyncio.create_task(capture_futures_accounts_wire(receipt, sink))
    await entered.wait()
    assert not task.done()
    release.set()
    result = await task
    assert result.raw_receipt_public_id == public_id
    assert result.client_selection == receipt.client_selection
    assert result.request_started_at == receipt.request_started_at
    assert result.observed_at == receipt.observed_at
    assert result.status_code == 200
    assert base64.b64decode(captured[0].material) == _WIRE
    assert captured[0].representation == "wire_bytes_base64"
    assert captured[0].context.kind == "account_cash"
    assert captured[0].context.venue_account_id is None
    assert captured[0].context.target_json_pointer is None
    assert captured[0].context.channel == transport.FUTURES_NATIVE_ACCOUNTS_CHANNEL


async def test_failed_storage_cannot_return_a_fabricated_receipt() -> None:
    """A failed actual durable sink propagates instead of returning a caller-created snapshot UUID.

    Given: A durable source sink that raises a storage error,
    When: capture attempts to persist an original HTTP entity,
    Then: the error propagates without a manufactured receipt identity.
    """

    async def sink(observation: PrivateExecutionObservation) -> str:
        raise RuntimeError("storage unavailable")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await capture_futures_accounts_wire(_receipt(), sink)


@pytest.mark.parametrize(
    "updates",
    (
        {"client_selection": _selection(credential_timestamp=_NOW + timedelta(seconds=2))},
        {"request_started_at": _NOW + timedelta(seconds=2)},
    ),
)
def test_source_clocks_are_not_repaired(updates: dict[str, object]) -> None:
    """An impossible credential/capture sequence or regressed wall clock stays invalid.

    Given: A future credential revision or a regressed response clock,
    When: source receipt evidence is validated,
    Then: the chronology is rejected without substituting a timestamp.
    """
    with pytest.raises(ValueError):
        _receipt(**updates)


def test_future_credential_revision_is_refused_before_network() -> None:
    """An impossible selected credential revision cannot start an authenticated account read.

    Given: A selected credential revision later than the actual current clock,
    When: the authenticated account read is requested,
    Then: the request is refused before any HTTP call.
    """
    adapter = _Adapter()
    selected = _selection(credential_timestamp=datetime.now(UTC) + timedelta(days=1))
    with _client(adapter) as user, pytest.raises(NativeAccountsTransportError, match="credential"):
        _read(user, selected)
    assert adapter.calls == []


async def test_actual_dispatcher_owns_the_same_session_proxy_lock_and_restores_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real asynchronous dispatcher supplies the transport's actual private routing scope.

    Given: A selected SDK session with pre-existing proxy settings and ambient trust enabled,
    When: Its native read runs through the real Futures REST dispatcher and worker pool,
    Then: The same locked session sends privately and its original proxy state is restored.
    """
    client = KrakenFuturesExchangeClient()
    monkeypatch.setattr(routing, "get_egress_pool", lambda: None)

    class InspectingAdapter(_Adapter):
        """Observe the actual worker's routed session at its socket-free send boundary."""

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
            receipt = await client._dispatch_routed_rest(
                operation="get_native_accounts_source",
                kind="private_idempotent_read",
                target=futures_sdk_proxy_target(user),
                sync_call=lambda: read_futures_accounts_wire(user, _selection()),
            )
            assert receipt.entity_bytes == _WIRE
            assert session.trust_env is True
            assert session.proxies == {"https": "http://previous.invalid:8080"}
            assert getattr(user, "_FuturesClient__proxy") == "http://previous.invalid:8080"
    finally:
        await client.disconnect()


@pytest.mark.parametrize(
    "status,body",
    (
        (302, b"redirect"),
        (503, b"private error"),
        (200, b"invalid JSON"),
        (200, b'{"result":"success","accounts":{},"nested":{"ApiKey":"fixture"}}'),
    ),
)
async def test_unclassified_state_body_never_reaches_durable_sink(status: int, body: bytes) -> None:
    """State capture refuses unknown/control bodies before encoding or invoking a durable writer.

    Given: A failed or unclassified original endpoint body,
    When: The source-only capture helper is called,
    Then: Its fixed privacy refusal precedes every sink invocation and retained body digest.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        """Record any forbidden durable boundary invocation without inspecting body contents."""
        captured.append(observation)
        return "00000000-0000-7000-8000-000000000099"

    with pytest.raises(NativeAccountsBodyPrivacyError, match="not_classified"):
        await capture_futures_accounts_wire(_receipt(status, entity_bytes=body), sink)
    assert captured == []
