"""Bounded privacy classification before native HTTP state persistence.

This is not financial interpretation. Only a documented successful account
object with no auth/control fields or known credential echoes may enter raw
state storage. Unclassified/malformed bodies have fixed failure metadata only.
"""

import json
from dataclasses import dataclass
from functools import partial
from typing import Literal
from typing import Never

from snapper.infrastructure.exchanges.execution_wire_receipts import _AUTHENTICATION_FIELDS
from snapper.infrastructure.exchanges.execution_wire_receipts import _CONTROL_EVENTS

_MAX_BODY_BYTES = 8 * 1024 * 1024
_MAX_FIELDS = 65536
_MAX_NODES = 65536
_MAX_DEPTH = 64
_AUTH_FIELDS = frozenset(value.casefold() for value in _AUTHENTICATION_FIELDS) | frozenset(
    {
        "auth",
        "authent",
        "authentication",
        "credentials",
        "password",
        "passphrase",
        "private_key",
        "access_token",
        "accesstoken",
    }
)


class NativeAccountsBodyPrivacyError(ValueError):
    """Refuse unclassified state bodies without retaining their content, digest or exception text."""

    def __init__(self, reason: Literal["http_error", "source_invalid"]) -> None:
        """Expose only an enumerated source disposition, never provider-controlled failure details."""
        super().__init__("native_accounts_body_not_classified")
        self.privacy_reason = reason


@dataclass(slots=True)
class _Inspection:
    """Observe every original pair before duplicate-key overwrite can hide sensitive source fields."""

    fields: int = 0
    refused: bool = False


class _ObjectPairs(list[tuple[str, object]]):
    """Keep all original pairs, including overwritten non-auth labels containing known secrets."""


def _pairs(values: list[tuple[str, object]], inspection: _Inspection) -> _ObjectPairs:
    """Check all-depth original field names/control discriminators without retaining their source."""
    inspection.fields += len(values)
    if inspection.fields > _MAX_FIELDS:
        raise ValueError("native_state_privacy_field_bound")
    for name, value in values:
        normalized = name.strip().casefold()
        inspection.refused |= normalized in _AUTH_FIELDS or normalized == "python-kraken-sdk"
        inspection.refused |= normalized == "method" and isinstance(value, str)
        inspection.refused |= (
            normalized == "event"
            and isinstance(value, str)
            and value.strip().casefold() in _CONTROL_EVENTS
        )
    return _ObjectPairs(values)


def _numeric_text(source: str) -> str:
    """Preserve a numeric lexeme only for privacy matching, without interpreting its amount."""
    return source


def _nonfinite_constant(source: str) -> Never:
    """Refuse non-JSON numeric constants before retaining any source body or content digest."""
    raise ValueError("native_state_privacy_non_json_number")


def _bounded_safe_tree(document: _ObjectPairs, secrets: tuple[str, ...]) -> bool:
    """Refuse excessive shape or known auth strings, including escaped JSON and overwritten values."""
    pending: list[tuple[object, int]] = [(document, 0)]
    seen = 0
    while pending:
        value, depth = pending.pop()
        seen += 1
        if seen > _MAX_NODES or depth > _MAX_DEPTH:
            return False
        if isinstance(value, _ObjectPairs):
            pending.extend((part, depth + 1) for pair in value for part in pair)
        elif isinstance(value, list):
            pending.extend((child, depth + 1) for child in value)
        elif isinstance(value, str) and any(secret and secret in value for secret in secrets):
            return False
    return True


def native_accounts_body_is_classified(
    entity: bytes, known_auth_values: tuple[str, ...] = ()
) -> bool:
    """Require a bounded known financial envelope before computing any retained body digest.

    Args:
        entity: Original bounded fixed-endpoint HTTP bytes, never a header or key-check response.
        known_auth_values: Closed actual request key/signature/secret values for in-memory echo refusal.

    Returns:
        Whether original bytes may be retained as financial state; no body/hash is emitted on refusal.
    """
    if len(entity) > _MAX_BODY_BYTES:
        return False
    inspection = _Inspection()
    try:
        document: object = json.loads(
            entity,
            object_pairs_hook=partial(_pairs, inspection=inspection),
            parse_int=_numeric_text,
            parse_float=_numeric_text,
            parse_constant=_nonfinite_constant,
        )
    except (ValueError, UnicodeError, RecursionError):
        return False
    if (
        inspection.refused
        or not isinstance(document, _ObjectPairs)
        or dict(document).get("result") != "success"
        or not isinstance(dict(document).get("accounts"), _ObjectPairs)
    ):
        return False
    return _bounded_safe_tree(document, known_auth_values)
