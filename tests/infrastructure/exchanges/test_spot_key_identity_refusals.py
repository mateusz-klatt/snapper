"""Original SDK, routing and chronology refusal cases after measured identity coverage gaps."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast

import pytest
from kraken.spot import User
from pydantic import ValidationError
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.auth import AuthBase

from snapper.core.spot_account_identity import SpotAccountPublicIdentity
from snapper.infrastructure.exchanges.kraken_spot_key_identity import SpotKeyIdentityTransportError
from snapper.infrastructure.exchanges.kraken_spot_key_identity import (
    read_spot_prepared_key_identity,
)
from snapper.infrastructure.network.egress_context import egress_identity
from tests.infrastructure.exchanges.test_spot_key_identity import _Adapter
from tests.infrastructure.exchanges.test_spot_key_identity import _client
from tests.infrastructure.exchanges.test_spot_key_identity import _closed
from tests.infrastructure.exchanges.test_spot_key_identity import _read
from tests.infrastructure.exchanges.test_spot_key_identity import _safe
from tests.infrastructure.exchanges.test_spot_key_identity import _selection

type _SignedRequest = tuple[str, str, dict[str, object], dict[str, object], str]


def test_changed_actual_sdk_origin_refuses_before_original_signing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A selected public revision cannot authorize a different actual SDK endpoint.

    Given: The real original Spot SDK with an altered runtime origin,
    When: Its source reader checks the actual owner before signing,
    Then: It refuses without a physical request or a source receipt.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        monkeypatch.setattr(user, "URL", "https://unexpected.invalid")
        with pytest.raises(SpotKeyIdentityTransportError, match="actual_route") as caught:
            _read(user)
    _safe(caught.value)
    assert not adapter.calls


@pytest.mark.parametrize("context", ("absent", "other_exchange", "public"))
def test_original_read_requires_the_actual_private_egress_context(context: str) -> None:
    """A valid signer cannot replace the required actual private routing ownership.

    Given: The real SDK outside private egress or inside another routing classification,
    When: Its original key-info operation starts,
    Then: It refuses before a local physical request and exposes only a fixed category.
    """
    adapter = _Adapter()
    with (
        _client(adapter) as user,
        pytest.raises(SpotKeyIdentityTransportError, match="private_egress") as caught,
    ):
        if context == "absent":
            read_spot_prepared_key_identity(user, _selection())
        else:
            with egress_identity(
                exchange="kraken_futures" if context == "other_exchange" else "kraken",
                traffic_class="public" if context == "public" else "private",
                owner="test",
                operation="original_spot_identity",
            ):
                read_spot_prepared_key_identity(user, _selection())
    _safe(caught.value)
    assert not adapter.calls


@pytest.mark.parametrize("change", ("environment_proxy", "missing_session"))
def test_actual_session_must_be_owned_before_source_preparation(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """Implicit environment routing and a missing actual Session cannot issue an identity.

    Given: The real SDK with environment proxy lookup enabled or its Session removed,
    When: Its closed original read validates the actual transport,
    Then: No physical request is sent and no public response is invented.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_SpotClient__session"))
        if change == "environment_proxy":
            session.trust_env = True
        else:
            monkeypatch.setattr(user, "_SpotClient__session", None)
        with pytest.raises(SpotKeyIdentityTransportError, match="actual_session") as caught:
            _read(user)
    _safe(caught.value)
    assert not adapter.calls


def _changed_signed_request(original: _SignedRequest, change: str) -> _SignedRequest:
    """Change one actual SDK output after original nonce generation and signature calculation."""
    method, url, headers, parameters, query = original
    headers, parameters = dict(headers), dict(parameters)
    if change == "method":
        method = "GET"
    elif change == "url":
        url = "https://unexpected.invalid/key-info"
    elif change == "query":
        query = "unexpected=value"
    elif change == "parameters":
        parameters["unexpected"] = "value"
    elif change.startswith("nonce_"):
        nonces: dict[str, object] = {
            "nonce_type": 123,
            "nonce_ascii": "１２３",
            "nonce_text": "12x",
            "nonce_length": "1" * 33,
        }
        parameters["nonce"] = nonces[change]
    elif change == "header_extra":
        headers["Authorization"] = "unexpected"
    elif change == "header_value":
        headers["API-Sign"] = ""
    else:
        assert change == "header_content_type"
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    return method, url, headers, parameters, query


@pytest.mark.parametrize(
    "change",
    (
        "method",
        "url",
        "query",
        "parameters",
        "nonce_type",
        "nonce_ascii",
        "nonce_text",
        "nonce_length",
        "header_extra",
        "header_value",
        "header_content_type",
    ),
)
def test_original_sdk_output_drift_is_refused_before_physical_dispatch(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """A changed SDK request contract cannot silently widen the fixed private operation.

    Given: Real SDK signing followed by an observed mutation of one returned request operand,
    When: The original reader validates its fixed method, body and signing-header contract,
    Then: It refuses before Session dispatch and never reconstructs a successful source.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        original = user._prepare_request

        def prepare(**arguments: Any) -> _SignedRequest:
            """Forward the external SDK keyword boundary and mutate only its actual returned tuple."""
            signed = cast(_SignedRequest, original(**arguments))
            return _changed_signed_request(signed, change)

        monkeypatch.setattr(user, "_prepare_request", prepare)
        with pytest.raises(SpotKeyIdentityTransportError, match="sdk_") as caught:
            _read(user)
    _safe(caught.value)
    assert not adapter.calls


class _OwnerChangingAuth(AuthBase):
    """Change the actual signing owner after the SDK has prepared its original request."""

    def __init__(
        self, user: User, replacement: Session | None, patches: pytest.MonkeyPatch
    ) -> None:
        """Keep only synthetic actual SDK/session objects owned by the local fixture."""
        self.user = user
        self.replacement = replacement
        self.patches = patches

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Use the real Requests AuthBase seam without changing the final encoded request."""
        if self.replacement is None:
            self.user._key = "SYNTHETIC_CHANGED_OWNER_KEY"
        else:
            self.patches.setattr(self.user, "_SpotClient__session", self.replacement)
        return prepared


@pytest.mark.parametrize("change", ("key", "session"))
def test_authentication_owner_change_cannot_borrow_the_original_signature(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """The final physical dispatch still belongs to the same actual User and Session.

    Given: Real SDK signing and AuthBase changing the original User key or Session,
    When: Requests finishes preparing otherwise unchanged signed bytes,
    Then: The owned reader refuses before any physical send despite the valid old signature.
    """
    adapter = _Adapter()
    with _client(adapter) as user, Session() as replacement:
        replacement.trust_env = False
        session = cast(Session, getattr(user, "_SpotClient__session"))
        session.auth = _OwnerChangingAuth(
            user, replacement if change == "session" else None, monkeypatch
        )
        with pytest.raises(SpotKeyIdentityTransportError, match="final_owner_or_key") as caught:
            _read(user)
    _safe(caught.value)
    assert not adapter.calls


def test_future_selected_credential_refuses_at_the_actual_dispatch_clock() -> None:
    """A not-yet-known credential cannot retrospectively authorize the physical request.

    Given: A real original signer and public credential revision later than current UTC,
    When: The fixed operation samples its actual dispatch time,
    Then: It refuses before local HTTP and cannot create a future-dated identity source.
    """
    adapter = _Adapter()
    with (
        _client(adapter) as user,
        pytest.raises(SpotKeyIdentityTransportError, match="credential_not_yet_known") as caught,
    ):
        _read(user, _selection(credential_timestamp=datetime.now(UTC) + timedelta(days=1)))
    _safe(caught.value)
    assert not adapter.calls


def test_actual_merged_environment_cannot_disable_stream_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Request preparation cannot silently turn bounded private body reading into eager reading.

    Given: The actual Session merger returning its original settings with streaming disabled,
    When: The source reader validates the final dispatch environment,
    Then: It refuses before physical HTTP and no private body is eagerly consumed.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_SpotClient__session"))
        original = session.merge_environment_settings

        def merge(*args: Any, **kwargs: Any) -> dict[str, Any]:
            """Observe the actual external Requests merger before changing one ownership setting."""
            settings = original(*args, **kwargs)
            settings["stream"] = False
            return settings

        monkeypatch.setattr(session, "merge_environment_settings", merge)
        with pytest.raises(SpotKeyIdentityTransportError, match="streaming_boundary") as caught:
            _read(user)
    _safe(caught.value)
    assert not adapter.calls


def test_discarded_actual_response_cannot_be_replaced_by_an_absent_session_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Session extension cannot issue original-response authority from an absent return value.

    Given: Actual SDK/Requests HTTP whose Session observer closes and discards the original response,
    When: That observer returns no Response to the closed source reader,
    Then: The operation refuses issuance and the actually received sensitive stream remains closed.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_SpotClient__session"))
        original = session.send

        def discard(request: PreparedRequest, **kwargs: Any) -> None:
            """Call the actual external Session boundary and discard only its closed response."""
            original(request, **kwargs).close()

        monkeypatch.setattr(session, "send", discard)
        with pytest.raises(
            SpotKeyIdentityTransportError, match="original_response_required"
        ) as caught:
            _read(user)
    _safe(caught.value)
    assert len(adapter.calls) == 1
    _closed(adapter)


@pytest.mark.parametrize("change", ("url", "history"))
def test_original_response_route_is_checked_before_reading_sensitive_body(change: str) -> None:
    """Response metadata cannot attribute another route to the fixed original key-info operation.

    Given: The actual local adapter supplying an original Response with changed route metadata,
    When: Normal Requests dispatch returns that Response to the fixed source reader,
    Then: It refuses before body consumption and closes the original HTTP response.
    """

    class ChangedRouteAdapter(_Adapter):
        """Change only original local response metadata after constructing the real stream."""

        def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
            """Preserve actual source bytes while making their route attribution contradictory."""
            response = super().send(request, *args, **kwargs)
            if change == "url":
                response.url = "https://unexpected.invalid/key-info"
            else:
                response.history = [Response()]
            return response

    adapter = ChangedRouteAdapter()
    with (
        _client(adapter) as user,
        pytest.raises(SpotKeyIdentityTransportError, match="response_route_changed") as caught,
    ):
        _read(user)
    _safe(caught.value)
    assert all(stream.read_calls == 0 for stream in adapter.streams)
    _closed(adapter)


def test_session_change_during_original_body_read_refuses_issuance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed original response cannot hand off ownership to a replacement Session.

    Given: Real original HTTP whose body stream swaps the SDK Session after reading its actual bytes,
    When: The source reader finishes projecting the otherwise valid identity,
    Then: It refuses before issuance while closing the original response and preserving its old owner.
    """
    adapter = _Adapter()
    with _client(adapter) as user, Session() as replacement:
        replacement.trust_env = False
        original_send = adapter.send

        def send(request: PreparedRequest, *args: object, **kwargs: object) -> Response:
            """Wrap only the real local body's actual read, leaving source creation unchanged."""
            response = original_send(request, *args, **kwargs)
            stream = adapter.streams[-1]
            original_read = stream.read

            def read(size: int = -1) -> bytes:
                """Swap the actual User Session after obtaining original bytes from its true stream."""
                material = original_read(size)
                monkeypatch.setattr(user, "_SpotClient__session", replacement)
                return material

            monkeypatch.setattr(stream, "read", read)
            return response

        monkeypatch.setattr(adapter, "send", send)
        with pytest.raises(
            SpotKeyIdentityTransportError, match="session_changed_during_read"
        ) as caught:
            _read(user)
    _safe(caught.value)
    _closed(adapter)


@pytest.mark.parametrize("change", ("route", "chronology"))
def test_detached_public_identity_still_validates_its_own_route_and_chronology(change: str) -> None:
    """Strict public-value validation does not silently normalize contradictory source projections.

    Given: A detached public observation from a genuine actual original key-info operation,
    When: Its plain model data is changed to another venue or a response before its request,
    Then: Public reconstruction refuses the contradiction and the actual issued source stays unchanged.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        source = _read(user)
        try:
            original = source.observation
            values = original.model_dump()
            if change == "route":
                values["client_selection"] = _selection(
                    exchange="kraken_futures", public_api_origin="https://futures.kraken.com"
                )
            else:
                values["observed_at"] = original.request_started_at - timedelta(seconds=1)
            with pytest.raises(ValidationError, match="Spot identity source"):
                SpotAccountPublicIdentity.model_validate(values)
            assert source.observation == original
        finally:
            source.close()
    _closed(adapter)
