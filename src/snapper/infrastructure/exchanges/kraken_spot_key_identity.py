"""Read original Spot key identity using the actual SDK signer and its owned routed Session.

The ordinary SDK request path can renew its session and decode sensitive errors.
This fixed read uses the already owned Session, protects the exact final POST
body, and closes the original response before issuing its safe identity source.
No original key-info body, nonce, signature or body digest is persisted.
"""

import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from hmac import compare_digest
from time import monotonic
from typing import cast

from kraken.spot import User
from requests import PreparedRequest
from requests import Request
from requests import RequestException
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.spot_account_identity import SpotAccountPublicIdentity
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _entity_bytes
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _RedirectResponseError
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _stop_redirect
from snapper.infrastructure.exchanges.prepared_spot_account_identity import (
    PreparedSpotAccountIdentity,
)
from snapper.infrastructure.exchanges.prepared_spot_account_identity import (
    _mint_spot_account_identity,
)
from snapper.infrastructure.exchanges.spot_key_identity_projection import (
    SpotKeyIdentityProjectionError,
)
from snapper.infrastructure.exchanges.spot_key_identity_projection import project_spot_key_identity
from snapper.infrastructure.network.egress_context import current_egress_identity

_ORIGIN = "https://api.kraken.com"
_PATH = "/0/private/GetApiKeyInfo"
_URL = _ORIGIN + _PATH
_CONTENT_TYPE = "application/json; charset=utf-8"
_SIGNED_HEADER_NAMES = frozenset({"API-Key", "API-Sign", "Content-Type"})


class SpotKeyIdentityTransportError(ValueError):
    """The original selected Spot key-info source is unavailable without exposing private data."""


@dataclass(frozen=True, slots=True, repr=False)
class _OriginalSignedRequest:
    """Transient original signer output; generic formatting must never reveal its contents."""

    headers: dict[str, str]
    body: bytes
    expected_key: str


def _routed_session(client: User, selection: ExecutionClientSelection) -> Session:
    """Require the actual original Spot User inside the existing private egress ownership."""
    identity = current_egress_identity()
    if (
        selection.exchange != "kraken"
        or selection.mode != "live"
        or selection.environment != "production"
        or selection.public_api_origin != _ORIGIN
        or client.URL != _ORIGIN
    ):
        raise SpotKeyIdentityTransportError("spot_key_info_actual_route_unproven")
    if identity is None or identity.exchange != "kraken" or identity.traffic_class != "private":
        raise SpotKeyIdentityTransportError("spot_key_info_private_egress_required")
    session = getattr(client, "_SpotClient__session", None)
    if not isinstance(session, Session) or session.trust_env:
        raise SpotKeyIdentityTransportError("spot_key_info_actual_session_unproven")
    return session


def _original_signed_request(client: User) -> _OriginalSignedRequest:
    """Use only the pinned SDK's exact nonce-only JSON signer for the fixed key-info POST."""
    method, url, headers, parameters, query = client._prepare_request(
        method="POST", uri=_PATH, params={}, auth=True, do_json=True
    )
    if method != "POST" or url != _URL or query or set(parameters) != {"nonce"}:
        raise SpotKeyIdentityTransportError("spot_key_info_sdk_request_changed")
    nonce = parameters["nonce"]
    if (
        not isinstance(nonce, str)
        or not nonce.isascii()
        or not nonce.isdecimal()
        or len(nonce) > 32
    ):
        raise SpotKeyIdentityTransportError("spot_key_info_sdk_nonce_unproven")
    if (
        set(headers) != _SIGNED_HEADER_NAMES
        or any(not isinstance(value, str) or not value for value in headers.values())
        or headers["Content-Type"] != _CONTENT_TYPE
    ):
        raise SpotKeyIdentityTransportError("spot_key_info_sdk_auth_unproven")
    copied = dict(cast(dict[str, str], headers))
    return _OriginalSignedRequest(copied, json.dumps(parameters).encode("utf-8"), copied["API-Key"])


def _prepare_owned_request(session: Session, original: _OriginalSignedRequest) -> PreparedRequest:
    """Detach the final AuthBase request and own its exact response interception before dispatch."""
    request = Request(method="POST", url=_URL, headers=original.headers, data=original.body)
    prepared = session.prepare_request(request).copy()
    prepared.hooks = {"response": [_stop_redirect]}
    if (
        prepared.method != "POST"
        or prepared.url != _URL
        or prepared.body != original.body
        or "Authorization" in prepared.headers
        or "Transfer-Encoding" in prepared.headers
        or "Content-Encoding" in prepared.headers
        or prepared.headers.get("Content-Length") != str(len(original.body))
        or any(prepared.headers.get(name) != value for name, value in original.headers.items())
    ):
        raise SpotKeyIdentityTransportError("spot_key_info_final_request_changed")
    return prepared


def _require_actual_dispatch(
    client: User,
    selection: ExecutionClientSelection,
    session: Session,
    original: _OriginalSignedRequest,
    actual_send_at: datetime,
) -> None:
    """Verify actual owner/key and disabled adapter retries after all request/environment preparation."""
    actual_key = getattr(client, "_key", None)
    if (
        _routed_session(client, selection) is not session
        or not isinstance(actual_key, str)
        or not compare_digest(actual_key.encode("utf-8"), original.expected_key.encode("utf-8"))
    ):
        raise SpotKeyIdentityTransportError("spot_key_info_final_owner_or_key_changed")
    revision = selection.credential_timestamp
    if revision is None or revision > actual_send_at:
        raise SpotKeyIdentityTransportError("spot_key_info_credential_not_yet_known")
    adapter = session.get_adapter(_URL)
    if not isinstance(adapter, HTTPAdapter) or adapter.max_retries.total not in {0}:
        raise SpotKeyIdentityTransportError("spot_key_info_hidden_retry_unproven")


def _send_original_request(
    client: User,
    selection: ExecutionClientSelection,
    session: Session,
    original: _OriginalSignedRequest,
) -> tuple[Response, datetime]:
    """Perform one bounded actual send with a privately owned final request and redirect barrier."""
    prepared = _prepare_owned_request(session, original)
    settings = session.merge_environment_settings(_URL, {}, True, None, None)
    if settings.get("stream") is not True:
        raise SpotKeyIdentityTransportError("spot_key_info_streaming_boundary_changed")
    started_at = datetime.now(UTC)
    _require_actual_dispatch(client, selection, session, original, started_at)
    request_failed = False
    response: Response | None = None
    try:
        response = session.send(prepared, timeout=(5, 10), allow_redirects=False, **settings)
    except _RedirectResponseError as redirected:
        response = redirected.response
    except RequestException as error:
        if error.response is not None:
            error.response.close()
        request_failed = True
        response = None
    if request_failed:
        raise SpotKeyIdentityTransportError("spot_key_info_transport_failed") from None
    if not isinstance(response, Response):
        raise SpotKeyIdentityTransportError("spot_key_info_original_response_required")
    return response, started_at


def _safe_original_identity(
    response: Response,
    original: _OriginalSignedRequest,
    selection: ExecutionClientSelection,
    started_at: datetime,
    deadline: float,
) -> SpotAccountPublicIdentity:
    """Read and project only an original HTTP success, closing its sensitive source on every path."""
    try:
        if response.url != _URL or response.history:
            raise SpotKeyIdentityTransportError("spot_key_info_response_route_changed")
        if response.status_code != 200:
            raise SpotKeyIdentityTransportError("spot_key_info_http_refused")
        body = _entity_bytes(response, deadline)
        return project_spot_key_identity(
            body, original.expected_key, selection, started_at, datetime.now(UTC)
        )
    finally:
        response.close()


def read_spot_prepared_key_identity(
    client: User, selection: ExecutionClientSelection
) -> PreparedSpotAccountIdentity:
    """Issue one actual safe Spot identity source using the original routed signer/session.

    Args:
        client: Actual original SDK User whose private egress and worker lifecycle the caller owns.
        selection: Original instantiated credential revision and supported production endpoint.

    Returns:
        Opaque original account identity; actual persistence, Futures association and funds remain separate.
    """
    selected = ExecutionClientSelection.model_validate(selection).model_copy(deep=True)
    try:
        session = _routed_session(client, selected)
        original = _original_signed_request(client)
        deadline = monotonic() + 30
        response, started_at = _send_original_request(client, selected, session, original)
        observation = _safe_original_identity(response, original, selected, started_at, deadline)
        if _routed_session(client, selected) is not session:
            raise SpotKeyIdentityTransportError("spot_key_info_session_changed_during_read")
        return _mint_spot_account_identity(client, session, original.expected_key, observation)
    except (SpotKeyIdentityTransportError, SpotKeyIdentityProjectionError):
        raise
    except Exception:
        pass
    raise SpotKeyIdentityTransportError("spot_key_info_transport_failed") from None
