"""Privacy adapter for an actual owned private SDK receive context.

This adapter establishes no account ownership. It prevents malformed
private input from reaching the SDK branch that logs original source text.
Classifiable financial frames still use the existing genuine raw-wire decoder.
"""

import json
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import DecimalException
from functools import partial
from types import MappingProxyType
from typing import Never
from typing import SupportsIndex

from snapper.infrastructure.exchanges.execution_wire_receipts import _AUTHENTICATION_FIELDS
from snapper.infrastructure.exchanges.execution_wire_receipts import _PRIVATE_FUTURES_FEEDS
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message

_ACK_EVENTS = frozenset({"subscribed", "subscribed_failed", "unsubscribed", "unsubscribed_failed"})
_SIMPLE_EVENTS = frozenset(
    {"subscribe", "unsubscribe", "error", "alert", "info", "heartbeat", "ping", "pong"}
)
_AUTH_ECHOES = ("api_key", "original_challenge", "signed_challenge")
_AUTH_NAMES = frozenset(name.casefold() for name in _AUTHENTICATION_FIELDS) | frozenset(
    {
        "auth",
        "authentication",
        "credentials",
        "password",
        "passphrase",
        "private_key",
        "access_token",
        "accesstoken",
        "authent",
        "nonce",
        "privatekey",
        "apisecret",
    }
)


class TransientPrivateControl(Mapping[str, object]):
    """Read-only, repr-safe SDK control fields without raw bytes or default serialization."""

    __slots__ = ("_fields", "_failure_reason", "_receive_metadata")
    _fields: Mapping[str, object]
    _failure_reason: str | None
    _receive_metadata: object | None

    def __init__(
        self,
        fields: dict[str, object],
        failure_reason: str | None = None,
        receive_metadata: object | None = None,
    ) -> None:
        """Preserve only an explicit in-memory control projection and a fixed failure category.

        Args:
            fields: Narrow control fields needed by the pinned SDK or internal ACK verifier.
            failure_reason: Fixed local category for malformed or unclassified private input.
            receive_metadata: Immutable locally created physical context, never a provider field.
        """
        object.__setattr__(self, "_fields", MappingProxyType(dict(fields)))
        object.__setattr__(self, "_failure_reason", failure_reason)
        object.__setattr__(self, "_receive_metadata", receive_metadata)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse reassignment of private fields and public physical metadata properties."""
        raise AttributeError("Private authentication control is immutable")

    @property
    def failure_reason(self) -> str | None:
        """Expose only the fixed local failure classification.

        Returns:
            A fixed refusal reason, or absence for a successfully decoded transient control.
        """
        return self._failure_reason

    @property
    def receive_metadata(self) -> object | None:
        """Expose only the decoder-created immutable local physical context.

        Returns:
            Actual local metadata, absent when its physical receive ownership was unavailable.
        """
        return self._receive_metadata

    def __getitem__(self, key: str) -> object:
        """Supply the mapping operations used by actual pinned SDK control branches."""
        return self._fields[key]

    def __iter__(self) -> Iterator[str]:
        """Expose only the whitelisted transient field names."""
        return iter(self._fields)

    def __len__(self) -> int:
        """Expose only the number of whitelisted transient fields."""
        return len(self._fields)

    def __repr__(self) -> str:
        """Prevent logger formatting from exposing authentication echoes."""
        return "<TransientPrivateControl:redacted>"

    def __str__(self) -> str:
        """Prevent ordinary string formatting from exposing authentication echoes."""
        return repr(self)

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse ordinary pickle transport of sensitive transient control fields."""
        raise TypeError("Private authentication controls cannot be serialized")


@dataclass(slots=True)
class _Inspection:
    """Track every parsed pair, including authentication fields later hidden by duplicate keys."""

    authentication_pairs: int = 0
    duplicate_keys: bool = False
    nonfinite_constant: bool = False


def _pairs(pairs: list[tuple[str, object]], inspection: _Inspection) -> dict[str, object]:
    """Observe all object pairs before ordinary JSON last-key selection loses source information."""
    result: dict[str, object] = {}
    for key, value in pairs:
        inspection.authentication_pairs += int(key.casefold() in _AUTH_NAMES)
        inspection.duplicate_keys |= key in result
        result[key] = value
    return result


def _constant(value: str, inspection: _Inspection) -> None:
    """Mark non-JSON constants without converting them into a control-plane value."""
    inspection.nonfinite_constant = True


def _failure(reason: str) -> TransientPrivateControl:
    """Return a fixed safe event that bypasses the SDK raw-input warning branch."""
    return TransientPrivateControl(
        {"event": "error", "message": "private_wire_decode_refused"}, reason
    )


def _text(value: object) -> str | None:
    """Refuse malformed challenge/auth text before an SDK signature error could expose it."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        value.encode("utf-8")
    except UnicodeError:
        return None
    return value


def _control(document: dict[str, object], inspection: _Inspection) -> TransientPrivateControl:
    """Project control data for the actual SDK without storing its body or exposing its repr."""
    if inspection.duplicate_keys or inspection.nonfinite_constant:
        return _failure("control_json_ambiguous")
    event = document.get("event")
    if event == "challenge":
        challenge = _text(document.get("message"))
        return (
            TransientPrivateControl({"event": "challenge", "message": challenge})
            if challenge is not None
            else _failure("challenge_text_invalid")
        )
    if isinstance(event, str) and event in _ACK_EVENTS:
        feed = document.get("feed")
        echoes = {name: _text(document.get(name)) for name in _AUTH_ECHOES}
        if not isinstance(feed, str) or feed not in _PRIVATE_FUTURES_FEEDS:
            return _failure("control_feed_unclassified")
        if any(value is None for value in echoes.values()):
            return _failure("control_auth_echo_unavailable")
        return TransientPrivateControl({"event": event, "feed": feed, **echoes})
    if isinstance(event, str) and event in _SIMPLE_EVENTS:
        return TransientPrivateControl({"event": event})
    return _failure("control_unclassified")


def _number(value: str) -> None:
    """Avoid interpreting numeric operands during privacy-only envelope inspection."""


def _inspect_document(data: str | bytes | bytearray, inspection: _Inspection) -> object:
    """Inspect all original JSON pairs without retaining a second original byte string."""
    return json.loads(
        data,
        parse_float=_number,
        parse_int=_number,
        object_pairs_hook=partial(_pairs, inspection=inspection),
        parse_constant=partial(_constant, inspection=inspection),
    )


def _financial(
    data: str | bytes | bytearray, feed: str, delegate: PrivateExecutionJsonCodec
) -> object:
    """Keep safely classified financial bytes even when current numeric decoding is unavailable."""
    try:
        return delegate.loads(data)
    except (ValueError, UnicodeError, RecursionError, DecimalException):
        original = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        result = PrivateWireMessage({"feed": feed}, original)
        result.__dict__["_snapper_numeric_decode_unavailable"] = True
        return result


def decode_owned_private_message(
    data: str | bytes | bytearray, delegate: PrivateExecutionJsonCodec
) -> object:
    """Sanitize only an actual owned private receive loop before the SDK can log raw errors.

    Args:
        data: Actual private socket source already selected by the real receive-context owner.
        delegate: Existing exact financial/physical-epoch decoder, without standard-library mutation.

    Returns:
        Original financial boundary result or nonserializable sanitized transient control data.
    """
    inspection = _Inspection()
    try:
        document = _inspect_document(data, inspection)
        if not isinstance(document, dict):
            return _failure("private_object_required")
        root_authentication = sum(name.casefold() in _AUTH_NAMES for name in document)
        if inspection.authentication_pairs > root_authentication:
            return _failure("authentication_fields_nested_or_repeated")
        if inspection.authentication_pairs or is_execution_control_message(document):
            return _control(document, inspection)
        if document.get("feed") == "heartbeat":
            return TransientPrivateControl({"feed": "heartbeat"})
        feed = document.get("feed")
        if not isinstance(feed, str) or feed not in _PRIVATE_FUTURES_FEEDS:
            return _failure("private_feed_unclassified")
        return _financial(data, feed, delegate)
    except (ValueError, UnicodeError, RecursionError):
        return _failure("private_json_invalid")
