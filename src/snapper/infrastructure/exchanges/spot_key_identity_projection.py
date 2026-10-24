"""Discard sensitive original Spot key-info fields after exact in-memory selected-key comparison.

The official schema and its example disagree on the signing-key field name.
Exactly one documented spelling may supply the echo, and that spelling remains
explicit in the safe projection. Neither the raw body nor a digest of secrets
belongs in a retained observation, ordinary exception or accounting raw sink.
"""

from contextlib import suppress
from datetime import datetime
from hmac import compare_digest
from typing import Literal

from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.spot_account_identity import SpotAccountPublicIdentity
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)

_MAX_KEY_INFO_BYTES = 8 * 1024 * 1024


class SpotKeyIdentityProjectionError(ValueError):
    """A safe original identity projection is unavailable without exposing private response data."""


def _original_result(body: bytes) -> dict[str, object]:
    """Decode only bounded unique-key UTF-8 JSON and require actual successful Spot response shape."""
    document: dict[str, object] | None = None
    with suppress(ValueError, UnicodeError, RecursionError):
        if len(body) > _MAX_KEY_INFO_BYTES or "\x00" in body.decode("utf-8"):
            raise ValueError("Unsupported original key-info body")
        document = decode_private_execution_receipt(body)
    if document is None:
        raise SpotKeyIdentityProjectionError("spot_key_info_json_invalid") from None
    if document.get("error") != []:
        raise SpotKeyIdentityProjectionError("spot_key_info_provider_refused")
    result = document.get("result")
    if not isinstance(result, dict):
        raise SpotKeyIdentityProjectionError("spot_key_info_result_unavailable")
    return result


def _matched_echo(result: dict[str, object], expected_key: str) -> Literal["api_key", "apiKey"]:
    """Accept exactly one documented original spelling and compare its value without normalization."""
    fields: tuple[Literal["api_key", "apiKey"], ...] = ("api_key", "apiKey")
    present = [name for name in fields if name in result]
    if len(present) != 1:
        raise SpotKeyIdentityProjectionError("spot_key_info_key_echo_ambiguous")
    name = present[0]
    actual = result[name]
    try:
        matched = (
            bool(expected_key)
            and isinstance(actual, str)
            and compare_digest(actual.encode("utf-8"), expected_key.encode("utf-8"))
        )
    except UnicodeError:
        matched = False
    if not matched:
        raise SpotKeyIdentityProjectionError("spot_key_info_selected_key_mismatch")
    return name


def project_spot_key_identity(
    body: bytes,
    expected_key: str,
    selection: ExecutionClientSelection,
    request_started_at: datetime,
    observed_at: datetime,
) -> SpotAccountPublicIdentity:
    """Project only same-original identity after actual transport's final prepared-key comparison.

    Args:
        body: Transient actual successful HTTP response; never a retained raw accounting source.
        expected_key: Exact original SDK/final prepared key held only inside the closed operation.
        selection: Original selected public credential and independently verified Spot endpoint.
        request_started_at: Actual local request clock from the final owned dispatch boundary.
        observed_at: Actual local response-completion clock, not a claimed provider timestamp.

    Returns:
        Strict safe projection only; the closed transport must independently issue its source receipt.
    """
    result = _original_result(body)
    field = _matched_echo(result, expected_key)
    permissions = result.get("permissions")
    if not isinstance(permissions, list):
        raise SpotKeyIdentityProjectionError("spot_key_info_permissions_unavailable")
    observation: SpotAccountPublicIdentity | None = None
    with suppress(ValidationError):
        observation = SpotAccountPublicIdentity.model_validate(
            {
                "client_selection": selection,
                "internal_account_id": result.get("iban"),
                "echoed_key_field": field,
                "permissions": tuple(permissions),
                "request_started_at": request_started_at,
                "observed_at": observed_at,
            }
        )
    if observation is None:
        raise SpotKeyIdentityProjectionError("spot_key_info_public_fields_invalid") from None
    return observation
