"""Retain exact native account HTTP entities without decoding financial numbers.

The pinned SDK prepares authentication; its already routed HTTP session sends
the request with redirects disabled. The SDK's normal request method cannot
disable redirects and parses non-success responses before returning them.
No header, signature or credential secret belongs to a retained receipt.
"""

import base64
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from time import monotonic
from typing import Never
from typing import Self
from typing import SupportsIndex
from typing import cast
from weakref import WeakKeyDictionary

from kraken.futures import User
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from requests import PreparedRequest
from requests import Request
from requests import Response
from requests import Session

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    NativeAccountsBodyPrivacyError,
)
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    native_accounts_body_is_classified,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.network.egress_context import current_egress_identity

FUTURES_NATIVE_ACCOUNTS_CHANNEL = "kraken_futures.rest.accounts.v1"
FUTURES_PREPARED_ACCOUNTS_CHANNEL = "kraken_futures.rest.accounts.prepared-key.v2"
_ACCOUNTS_PATH = "/derivatives/api/v3/accounts"
_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_READ_SECONDS = 30
_ORIGINS = {
    "production": "https://futures.kraken.com",
    "demo": "https://demo-futures.kraken.com",
}


class NativeAccountsTransportError(ValueError):
    """A transport cannot establish the selected native account response's public provenance."""


class _RedirectResponseError(Exception):
    """Return a redirect response before requests eagerly reads its body or prepares another request."""

    def __init__(self, response: Response) -> None:
        """Retain the local response without copying authentication or provider material to the error."""
        super().__init__("native_accounts_redirect_source")
        self.response = response


def _stop_redirect(response: Response, **kwargs: object) -> Response:
    """Stop requests before its disabled-redirect path still consumes an unbounded response body."""
    if response.is_redirect:
        raise _RedirectResponseError(response)
    return response


class _TransportEvidence(BaseModel):
    """Freeze public transport evidence and reject unchecked instance updates."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


def _selected_origin(selection: ExecutionClientSelection) -> str:
    """Resolve only the exact supported live production or demo client identity."""
    origin = _ORIGINS.get(selection.environment)
    if (
        origin is None
        or selection.exchange != "kraken_futures"
        or selection.public_api_origin != origin
    ):
        raise NativeAccountsTransportError("native_accounts_transport_client_unproven")
    return origin


class FuturesAccountsWireReceipt(_TransportEvidence):
    """Original decoded HTTP entity bytes and actual public client/capture provenance.

    HTTP content decompression precedes these bytes; JSON numeric decoding does
    not. A non-200 status remains source evidence but cannot use the success
    parser's channel. No account-read freshness or financial authority is implied.
    """

    client_selection: ExecutionClientSelection
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant
    status_code: int = Field(ge=100, le=599)
    entity_bytes: bytes = Field(repr=False)

    @model_validator(mode="after")
    def validate_public_source(self) -> Self:
        """Keep authenticated environment and credential chronology explicit.

        Returns:
            The same exact immutable response evidence.
        """
        _selected_origin(self.client_selection)
        revision = cast(datetime, self.client_selection.credential_timestamp)
        if revision > self.request_started_at:
            raise NativeAccountsTransportError("native_accounts_transport_credential_not_yet_known")
        if self.request_started_at > self.observed_at:
            raise NativeAccountsTransportError("native_accounts_transport_clock_regressed")
        return self


class CapturedFuturesAccountsReceipt(_TransportEvidence):
    """Actual committed source identity; no caller-generated account snapshot identity."""

    raw_receipt_public_id: EvidenceUUID
    client_selection: ExecutionClientSelection
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant
    status_code: int = Field(ge=100, le=599)


def _routed_session(client: User, selection: ExecutionClientSelection) -> Session:
    """Use the exact SDK session whose proxy state the existing egress router owns."""
    origin = _selected_origin(selection)
    identity = current_egress_identity()
    if (
        identity is None
        or identity.exchange != "kraken_futures"
        or identity.traffic_class != "private"
    ):
        raise NativeAccountsTransportError("native_accounts_private_egress_required")
    if client.url != origin:
        raise NativeAccountsTransportError("native_accounts_transport_endpoint_mismatch")
    session = getattr(client, "_FuturesClient__session", None)
    if not isinstance(session, Session):
        raise NativeAccountsTransportError("native_accounts_transport_session_unproven")
    return session


def _entity_bytes(response: Response, deadline: float) -> bytes:
    """Bound decoded content and check elapsed time between delivered chunks.

    Connect/read socket timeouts remain independently enforced. This elapsed
    check does not interrupt a slow peer while its current chunk is being read.
    """
    body = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if monotonic() > deadline:
            raise NativeAccountsTransportError("native_accounts_transport_read_deadline")
        if len(body) + len(chunk) > _MAX_ENTITY_BYTES:
            raise NativeAccountsTransportError("native_accounts_transport_entity_too_large")
        body.extend(chunk)
    if monotonic() > deadline:
        raise NativeAccountsTransportError("native_accounts_transport_read_deadline")
    return bytes(body)


def _request_original_response(session: Session, url: str, headers: Mapping[str, str]) -> Response:
    """Send once with explicit timeouts and stop redirect handling before it consumes the body."""
    try:
        response = session.request(
            method="GET",
            url=url,
            headers=headers,
            timeout=(5, 10),
            allow_redirects=False,
            stream=True,
            hooks={"response": _stop_redirect},
        )
    except _RedirectResponseError as redirect:
        response = redirect.response
    if not isinstance(response, Response):
        raise NativeAccountsTransportError("native_accounts_original_http_response_required")
    return response


def read_futures_accounts_wire(
    client: User, selection: ExecutionClientSelection
) -> FuturesAccountsWireReceipt:
    """Perform one signed read inside the existing per-client egress/proxy lock.

    Args:
        client: Actual selected authenticated SDK User, already routed and locked.
        selection: Frozen public identity of that same instantiated credential revision.

    Returns:
        Original HTTP entity, including malformed JSON or non-success HTTP bodies.
        Network exceptions propagate; the caller's durable read attempt remains pending.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    session = _routed_session(client, selected)
    expected_url = _selected_origin(selected) + _ACCOUNTS_PATH
    method, url, headers, payload, query = client._prepare_request(
        method="GET", uri=_ACCOUNTS_PATH, post_params={}, query_params={}, auth=True
    )
    if method != "GET" or url != expected_url or payload or query:
        raise NativeAccountsTransportError("native_accounts_prepared_request_mismatch")
    started_at = datetime.now(UTC)
    if cast(datetime, selected.credential_timestamp) > started_at:
        raise NativeAccountsTransportError("native_accounts_transport_credential_not_yet_known")
    deadline = monotonic() + _READ_SECONDS
    response = _request_original_response(session, url, headers)
    try:
        if response.url != expected_url or response.history:
            raise NativeAccountsTransportError("native_accounts_http_response_route_mismatch")
        entity = _entity_bytes(response, deadline)
        observed_at = datetime.now(UTC)
        if _routed_session(client, selected) is not session:
            raise NativeAccountsTransportError("native_accounts_transport_changed_during_read")
        return FuturesAccountsWireReceipt(
            client_selection=selected,
            request_started_at=started_at,
            observed_at=observed_at,
            status_code=response.status_code,
            entity_bytes=entity,
        )
    finally:
        response.close()


async def capture_futures_accounts_wire(
    receipt: FuturesAccountsWireReceipt,
    sink: Callable[[PrivateExecutionObservation], Awaitable[str]],
) -> CapturedFuturesAccountsReceipt:
    """Await the actual durable source writer before allowing any account interpretation.

    Args:
        receipt: Original HTTP entity produced by the selected authenticated client.
        sink: Installed account-bound writer returning its committed receipt UUID.

    Returns:
        Committed source reference and public provenance, never a fabricated read authority.
    """
    source = FuturesAccountsWireReceipt.model_validate(receipt)
    if source.status_code != 200:
        raise NativeAccountsBodyPrivacyError("http_error")
    if not native_accounts_body_is_classified(source.entity_bytes):
        raise NativeAccountsBodyPrivacyError("source_invalid")
    channel = (
        FUTURES_NATIVE_ACCOUNTS_CHANNEL
        if source.status_code == 200
        else "kraken_futures.rest.accounts.http_error.v1"
    )
    observation = PrivateExecutionObservation(
        context=ExecutionObservationContext(
            kind="account_cash", channel=channel, observed_at=source.observed_at
        ),
        representation="wire_bytes_base64",
        material=base64.b64encode(source.entity_bytes).decode("ascii"),
    )
    public_id = await sink(observation)
    return CapturedFuturesAccountsReceipt(
        raw_receipt_public_id=public_id,
        client_selection=source.client_selection,
        request_started_at=source.request_started_at,
        observed_at=source.observed_at,
        status_code=source.status_code,
    )


def _request_guarded_original_response[GuardResultT](
    session: Session,
    expected_url: str,
    sdk_headers: Mapping[str, str],
    guard: Callable[[PreparedRequest, datetime], GuardResultT],
) -> tuple[Response, GuardResultT, datetime]:
    """Guard the final Requests object after auth/environment merging, immediately before send.

    The caller owns this closed response and must retain no request headers or
    authentication material. The same routed Session settings, hooks and redirect
    interception apply as the established unbound request path.
    """
    try:
        request = Request(
            method="GET",
            url=expected_url,
            headers=dict(sdk_headers),
            hooks={"response": _stop_redirect},
        )
        prepared = session.prepare_request(request).copy()
        prepared.hooks = {"response": [_stop_redirect]}
        if (
            prepared.method != "GET"
            or prepared.url != expected_url
            or prepared.body is not None
            or "Authorization" in prepared.headers
            or any(
                prepared.headers.get(name) != sdk_headers.get(name)
                for name in ("APIKey", "Authent", "Nonce")
            )
        ):
            raise NativeAccountsTransportError("prepared_futures_actual_request_changed")
        settings = session.merge_environment_settings(expected_url, {}, True, None, None)
        actual_send_at = datetime.now(UTC)
        try:
            proof = guard(prepared, actual_send_at)
        except Exception:
            raise NativeAccountsTransportError("prepared_futures_actual_key_unproven") from None
        try:
            response = session.send(prepared, timeout=(5, 10), allow_redirects=False, **settings)
        except _RedirectResponseError as redirect:
            response = redirect.response
        if not isinstance(response, Response):
            raise NativeAccountsTransportError("prepared_futures_original_response_required")
        return response, proof, actual_send_at
    except NativeAccountsTransportError:
        raise
    except Exception:
        raise NativeAccountsTransportError("prepared_futures_transport_failed") from None


class GuardedFuturesAccountsWireReceipt:
    """Opaque actually issued final-key-guarded account entity, without signing metadata."""

    __slots__ = ("_receipt", "_key_proof", "__weakref__")
    _receipt: FuturesAccountsWireReceipt
    _key_proof: PreparedKeyProofReference

    def __init__(
        self, receipt: FuturesAccountsWireReceipt, key_proof: PreparedKeyProofReference
    ) -> None:
        """Construct only shape; actual closed transport must additionally register issuance."""
        object.__setattr__(self, "_receipt", receipt)
        object.__setattr__(self, "_key_proof", key_proof)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse replacement of the original actual entity or its exact actual-key proof."""
        raise AttributeError("Guarded native accounts receipt is immutable")

    def __repr__(self) -> str:
        """Expose neither financial entity bytes nor signing/session material."""
        return "<GuardedFuturesAccountsWireReceipt:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic persistence before the trusted raw/source producer verifies this receipt."""
        raise TypeError("Guarded native accounts receipt cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic copy traversal into a transient original entity."""
        raise TypeError("Guarded native accounts receipt cannot be copied")


_GUARDED_ACCOUNT_RECEIPTS: WeakKeyDictionary[
    GuardedFuturesAccountsWireReceipt,
    tuple[FuturesAccountsWireReceipt, PreparedKeyProofReference],
] = WeakKeyDictionary()


def require_guarded_accounts_wire(
    candidate: object,
) -> tuple[FuturesAccountsWireReceipt, PreparedKeyProofReference]:
    """Require actual closed final-request issuance before new native source interpretation.

    Args:
        candidate: Actual transport result or an unissued plain/shaped caller object.

    Returns:
        Exact original entity and actual committed key proof used at physical request dispatch.

    Raises:
        NativeAccountsTransportError: If the result was not actually issued or its material changed.
    """
    if not isinstance(candidate, GuardedFuturesAccountsWireReceipt):
        raise NativeAccountsTransportError("native_accounts_guarded_transport_required")
    actual = _GUARDED_ACCOUNT_RECEIPTS.get(candidate)
    if actual is None or actual != (candidate._receipt, candidate._key_proof):
        raise NativeAccountsTransportError("native_accounts_guarded_transport_required")
    return actual[0].model_copy(deep=True), actual[1].model_copy(deep=True)


def read_futures_prepared_accounts_wire(
    client: User, selection: ExecutionClientSelection, token: PreparedKeyToken
) -> GuardedFuturesAccountsWireReceipt:
    """Read the fixed accounts endpoint using the exact key proved by the same operation.

    Args:
        client: Actual same original SDK User under its existing egress/session lock.
        selection: Frozen actual public credential/environment selected by the committed attempt.
        token: Actual issued same-operation token bound after the safe key proof committed.

    Returns:
        Actually issued original entity with final Requests-prepared key proof; no auth data is retained.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    session = _routed_session(client, selected)
    expected_url = _selected_origin(selected) + _ACCOUNTS_PATH
    method, url, headers, payload, query = client._prepare_request(
        method="GET", uri=_ACCOUNTS_PATH, post_params={}, query_params={}, auth=True
    )
    if method != "GET" or url != expected_url or payload or query:
        raise NativeAccountsTransportError("native_accounts_prepared_request_mismatch")

    def guard(prepared: PreparedRequest, observed_at: datetime) -> PreparedKeyProofReference:
        """Compare only the actual final key after Requests authentication preparation."""
        if _routed_session(client, selected) is not session:
            raise NativeAccountsTransportError("native_accounts_transport_changed_before_send")
        return token.assert_prepared_key(
            client, selected, prepared.headers.get("APIKey"), observed_at
        )

    deadline = monotonic() + _READ_SECONDS
    response, proof, started_at = _request_guarded_original_response(
        session, expected_url, headers, guard
    )
    try:
        if response.url != expected_url or response.history:
            raise NativeAccountsTransportError("native_accounts_http_response_route_mismatch")
        if response.status_code != 200:
            raise NativeAccountsBodyPrivacyError("http_error")
        entity = _entity_bytes(response, deadline)
        known_auth = tuple(
            value
            for value in (
                headers.get("APIKey"),
                headers.get("Authent"),
                getattr(client, "_secret", None),
            )
            if isinstance(value, str) and value
        )
        if not native_accounts_body_is_classified(entity, known_auth):
            raise NativeAccountsBodyPrivacyError("source_invalid")
        observed_at = datetime.now(UTC)
        if _routed_session(client, selected) is not session:
            raise NativeAccountsTransportError("native_accounts_transport_changed_during_read")
        original = FuturesAccountsWireReceipt(
            client_selection=selected,
            request_started_at=started_at,
            observed_at=observed_at,
            status_code=response.status_code,
            entity_bytes=entity,
        )
        result = GuardedFuturesAccountsWireReceipt(original, proof)
        _GUARDED_ACCOUNT_RECEIPTS[result] = (
            original.model_copy(deep=True),
            proof.model_copy(deep=True),
        )
        return result
    finally:
        response.close()
