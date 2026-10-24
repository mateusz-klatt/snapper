"""Read selected-key public identity without retaining the sensitive original response.

The authenticated endpoint echoes the signing API key and may include other
private account metadata. Only an exact in-memory key match and whitelisted
account UID/permission values leave the existing closed transport operation.
The separate explicit account-link operation also projects the original supported
IIBAN, without changing any native v1 serialization. The body never enters the
ordinary execution/account raw sink or a retained body hash.
"""

from collections.abc import Callable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from hmac import compare_digest
from time import monotonic
from typing import Never
from typing import SupportsIndex

from kraken.futures import User
from pydantic import TypeAdapter
from pydantic import ValidationError
from requests import PreparedRequest
from requests import Response
from requests import Session

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.futures_account_link import KrakenInternalAccountId
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    NativeAccountsTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _entity_bytes
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    _request_guarded_original_response,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _routed_session
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _selected_origin
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    PreparedFuturesAccountLink,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    _mint_futures_account_link,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import _mint_prepared_private_key

_KEY_CHECK_PATH = "/api/auth/v1/api-keys/v3/check"
_INTERNAL_ACCOUNT: TypeAdapter[str] = TypeAdapter(KrakenInternalAccountId)


class NativeKeyIdentityTransportError(ValueError):
    """The selected-key response cannot establish safe public identity without exposing its body."""


def _public_identity(
    body: bytes,
    expected_key: str,
    selection: ExecutionClientSelection,
    started_at: datetime,
    observed_at: datetime,
) -> NativeAccountPublicIdentity:
    """Discard the echoed API key after comparing it with the actual signed request in memory."""
    try:
        document = decode_private_execution_receipt(body)
    except (ValueError, UnicodeError, RecursionError):
        raise NativeKeyIdentityTransportError("native_key_check_json_invalid") from None
    returned_key = document.get("apiKey")
    try:
        matched = isinstance(returned_key, str) and compare_digest(
            returned_key.encode(), expected_key.encode()
        )
    except UnicodeError:
        matched = False
    if not matched:
        raise NativeKeyIdentityTransportError("native_key_check_selected_key_mismatch")
    permissions = document.get("permissions")
    if not isinstance(permissions, dict):
        raise NativeKeyIdentityTransportError("native_key_check_permissions_missing")
    try:
        return NativeAccountPublicIdentity.model_validate(
            {
                "client_selection": selection,
                "account_uid": document.get("accountUid"),
                "general_access": permissions.get("general"),
                "transfer_access": permissions.get("transfer"),
                "request_started_at": started_at,
                "observed_at": observed_at,
            }
        )
    except ValidationError:
        raise NativeKeyIdentityTransportError("native_key_check_public_fields_invalid") from None


def _public_internal_account(body: bytes) -> str:
    """Extract only a supported original IIBAN for the explicit new account-link operation."""
    try:
        if "\x00" in body.decode("utf-8"):
            raise ValueError("Original key check is not a UTF-8 JSON document")
        document = decode_private_execution_receipt(body)
        return _INTERNAL_ACCOUNT.validate_python(document.get("iiban"), strict=True)
    except (ValueError, UnicodeError, RecursionError):
        raise NativeKeyIdentityTransportError(
            "native_key_check_internal_account_unavailable"
        ) from None


def _request_key_check_response(
    session: Session,
    url: str,
    headers: Mapping[str, str],
    guard: Callable[[PreparedRequest, datetime], str],
) -> tuple[Response, str, datetime]:
    """Normalize only final-request failures while retaining typed safe source-body limit errors."""
    try:
        return _request_guarded_original_response(session, url, headers, guard)
    except NativeAccountsTransportError:
        raise NativeKeyIdentityTransportError("native_key_check_transport_failed") from None


def _read_key_identity_operation[ResultT](
    client: User,
    selection: ExecutionClientSelection,
    project: Callable[[NativeAccountPublicIdentity, str, str], ResultT],
    *,
    include_internal_account: bool = False,
) -> ResultT:
    """Perform the fixed signed identity check inside the actual existing egress/session lock.

    Args:
        client: Selected authenticated SDK user whose actual session the dispatcher owns.
        selection: Public revision of that same instantiated credential and supported endpoint.
        project: Closed safe identity/token projection after response cleanup, never the source body.
        include_internal_account: Explicit new operation requiring the original supported IIBAN.

    Returns:
        Only the explicit projection; sensitive HTTP source and headers remain inside this operation.

    Raises:
        NativeKeyIdentityTransportError: If the response cannot prove the exact selected key's identity.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    session = _routed_session(client, selected)
    expected_url = _selected_origin(selected) + _KEY_CHECK_PATH
    method, url, headers, payload, query = client._prepare_request(
        method="GET", uri=_KEY_CHECK_PATH, post_params={}, query_params={}, auth=True
    )
    expected_key = headers.get("APIKey")
    if (
        method != "GET"
        or url != expected_url
        or payload
        or query
        or not isinstance(expected_key, str)
        or not expected_key
    ):
        raise NativeKeyIdentityTransportError("native_key_check_prepared_request_mismatch")
    started_at = datetime.now(UTC)
    revision = selected.credential_timestamp
    if revision is None or revision > started_at:
        raise NativeKeyIdentityTransportError("native_key_check_credential_not_yet_known")
    deadline = monotonic() + 30

    def guard(prepared: PreparedRequest, actual_send_at: datetime) -> str:
        """Freeze only the actual final Requests key after comparing it with this exact SDK/User."""
        final_key = prepared.headers.get("APIKey")
        actual_user_key = getattr(client, "_key", None)
        if (
            not isinstance(final_key, str)
            or not isinstance(actual_user_key, str)
            or not compare_digest(final_key.encode("utf-8"), expected_key.encode("utf-8"))
            or not compare_digest(final_key.encode("utf-8"), actual_user_key.encode("utf-8"))
            or _routed_session(client, selected) is not session
        ):
            raise NativeKeyIdentityTransportError("native_key_check_final_prepared_key_mismatch")
        return final_key

    try:
        response, final_key, started_at = _request_key_check_response(session, url, headers, guard)
        try:
            if response.url != expected_url or response.history:
                raise NativeKeyIdentityTransportError("native_key_check_response_route_mismatch")
            if response.status_code != 200:
                raise NativeKeyIdentityTransportError("native_key_check_http_refused")
            body = _entity_bytes(response, deadline)
            observed_at = datetime.now(UTC)
            if _routed_session(client, selected) is not session:
                raise NativeKeyIdentityTransportError("native_key_check_transport_changed")
            public = _public_identity(body, final_key, selected, started_at, observed_at)
            internal_account = _public_internal_account(body) if include_internal_account else ""
        finally:
            response.close()
    except (NativeKeyIdentityTransportError, NativeAccountsTransportError):
        raise
    except Exception:
        raise NativeKeyIdentityTransportError("native_key_check_transport_failed") from None
    return project(public, final_key, internal_account)


def _read_key_identity[ResultT](
    client: User,
    selection: ExecutionClientSelection,
    project: Callable[[NativeAccountPublicIdentity, str, str], ResultT],
    *,
    include_internal_account: bool = False,
) -> ResultT:
    """Keep original private decoder, request and cleanup exceptions inside the closed operation."""
    error: NativeKeyIdentityTransportError | NativeAccountsTransportError
    try:
        return _read_key_identity_operation(
            client, selection, project, include_internal_account=include_internal_account
        )
    except NativeKeyIdentityTransportError as failure:
        error = NativeKeyIdentityTransportError(str(failure))
    except NativeAccountsTransportError as failure:
        error = NativeAccountsTransportError(str(failure))
    except Exception:
        error = NativeKeyIdentityTransportError("native_key_check_transport_failed")
    raise error from None


class PreparedNativeKeyObservation:
    """Opaque safe public observation plus the uncommitted actual transport-issued key token."""

    __slots__ = ("observation", "token")
    observation: NativeAccountPublicIdentity
    token: PreparedKeyToken

    def __init__(self, observation: NativeAccountPublicIdentity, token: PreparedKeyToken) -> None:
        """Keep the exact matched transport result without serializing its opaque expected key."""
        object.__setattr__(self, "observation", observation)
        object.__setattr__(self, "token", token)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse relabeling the prepared token with another public response."""
        raise AttributeError("Prepared native key observation is immutable")

    def __repr__(self) -> str:
        """Reveal no expected key, original response or signing SDK object."""
        return "<PreparedNativeKeyObservation:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic persistence of the uncommitted runtime token."""
        raise TypeError("Prepared native key observation cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass conversion/copy traversal into the actual key token."""
        raise TypeError("Prepared native key observation cannot be copied")


def read_futures_key_identity(
    client: User, selection: ExecutionClientSelection
) -> NativeAccountPublicIdentity:
    """Preserve the existing public-only single-request identity transport contract.

    Args:
        client: Actual original authenticated SDK User inside its existing egress dispatch.
        selection: Exact original selected public credential/environment revision.

    Returns:
        Only the verified whitelisted public identity; no runtime token or original response.
    """
    return _read_key_identity(
        client, selection, lambda observation, prepared_key, internal_account: observation
    )


def read_futures_prepared_key_identity(
    client: User, selection: ExecutionClientSelection
) -> PreparedNativeKeyObservation:
    """Issue a private key token inside the same actual prepared-header/echo comparison operation.

    Args:
        client: Actual original SDK User whose signing session is owned by the dispatcher.
        selection: Exact same selected public route; no later mutable key supplies the expectation.

    Returns:
        The exact public response and opaque actual transport-issued token, not yet proof-bound.
    """

    def prepare(
        observation: NativeAccountPublicIdentity, prepared_key: str, internal_account: str
    ) -> PreparedNativeKeyObservation:
        """Capture the actual prepared key only after the original sensitive response was discarded."""
        return PreparedNativeKeyObservation(
            observation, _mint_prepared_private_key(client, prepared_key, observation)
        )

    return _read_key_identity(client, selection, prepare)


def read_futures_prepared_account_link(
    client: User, selection: ExecutionClientSelection
) -> PreparedFuturesAccountLink:
    """Capture UUID and IIBAN from one verified original response without changing native v1 bytes.

    Args:
        client: Actual original SDK User owned by the existing private egress dispatcher.
        selection: Exact original public credential and production/demo endpoint revision.

    Returns:
        Issued same-response source and original token; no Spot association or financial grant.
    """

    def prepare(
        observation: NativeAccountPublicIdentity, prepared_key: str, internal_account: str
    ) -> PreparedFuturesAccountLink:
        """Issue only the explicit safe projection after closing the sensitive original HTTP source."""
        return _mint_futures_account_link(client, prepared_key, observation, internal_account)

    return _read_key_identity(client, selection, prepare, include_internal_account=True)
