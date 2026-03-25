"""Payload redaction utility for sensitive keys.

Recursively masks sensitive values in dictionaries before persisting
control/telemetry payloads.  Accepts dict, JSON string, or None and
always returns a JSON string (or None for None input).
"""

import json
from typing import Any

from snapper.core.json_types import JsonObject

_SENSITIVE_KEYS = frozenset(
    {
        "password",
        "api_key",
        "secret",
        "token",
        "bearer",
        "ws_token",
        "refresh_token",
        "access_token",
    }
)

_REDACTED = "[REDACTED]"


def _redact_dict(data: JsonObject) -> JsonObject:
    """Return a shallow copy of *data* with sensitive values replaced.

    Recurses into nested dicts and lists of dicts.

    Args:
        data: Dictionary to redact.

    Returns:
        New dictionary with sensitive leaf values replaced by ``[REDACTED]``.
    """
    result: JsonObject = {}
    for key, value in data.items():
        if key.lower() in _SENSITIVE_KEYS:
            result[key] = _REDACTED
        elif isinstance(value, dict):
            result[key] = _redact_dict(value)
        elif isinstance(value, list):
            result[key] = [_redact_dict(item) if isinstance(item, dict) else item for item in value]
        else:
            result[key] = value
    return result


def redact(payload: JsonObject | str | None) -> str | None:
    """Redact sensitive keys from *payload* and return a JSON string.

    Args:
        payload: A dict, a JSON-encoded string, or None.

    Returns:
        JSON string with sensitive values replaced by ``[REDACTED]``,
        or None when *payload* is None.
    """
    if payload is None:
        return None
    if isinstance(payload, str):
        parsed: Any = json.loads(payload)
        if isinstance(parsed, dict):
            return json.dumps(_redact_dict(parsed))
        return payload
    return json.dumps(_redact_dict(payload))
