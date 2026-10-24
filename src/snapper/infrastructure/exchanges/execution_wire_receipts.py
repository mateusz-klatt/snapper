"""Retain original private SDK JSON payloads and their exact decimal values.

The pinned Kraken SDK decodes WebSocket payloads through a module-local JSON
object before calling application callbacks. A narrow codec preserves the
original UTF-8 payload and parses private financial decimal numbers as Decimal.
Public market messages and outbound JSON retain their ordinary SDK behavior.
The standard-library JSON module is never modified.
"""

import json
from collections.abc import Mapping
from decimal import Decimal
from types import ModuleType
from typing import Any
from typing import Never
from typing import cast

import kraken.futures.websocket as futures_websocket
import kraken.spot.websocket.connectors as spot_websocket

_PRIVATE_FUTURES_FEEDS = frozenset(
    {
        "fills",
        "fills_snapshot",
        "account_log",
        "account_log_snapshot",
        "open_orders",
        "open_orders_snapshot",
        "open_positions",
        "open_positions_snapshot",
        "balances",
        "balances_snapshot",
    }
)
_PRIVATE_SPOT_CHANNELS = frozenset({"executions", "balances"})
_CONTROL_EVENTS = frozenset(
    {
        "challenge",
        "subscribe",
        "subscribed",
        "subscribed_failed",
        "unsubscribe",
        "unsubscribed",
        "unsubscribed_failed",
        "error",
        "alert",
        "info",
        "heartbeat",
        "ping",
        "pong",
    }
)
_AUTHENTICATION_FIELDS = frozenset(
    {
        "api_key",
        "apiKey",
        "api_secret",
        "apiSecret",
        "secret",
        "token",
        "original_challenge",
        "signed_challenge",
        "signature",
        "authorization",
        "Authorization",
    }
)


class PrivateWireMessage(dict[str, object]):
    """A decoded private message retaining its original UTF-8 JSON payload.

    Receipt bytes are an attribute, never an injected provider JSON field. This
    preserves the SDK callback contract and distinguishes authentic boundary
    capture from a provider-controlled key claiming to contain raw evidence.
    """

    def __init__(self, decoded: dict[str, object], wire_bytes: bytes) -> None:
        """Keep one immutable payload byte string beside the parsed provider fields."""
        super().__init__(decoded)
        self._wire_bytes = wire_bytes

    @property
    def wire_bytes(self) -> bytes:
        """Return the exact JSON payload bytes observed at the SDK decoding boundary.

        Returns:
            Original payload bytes, independent of mutations to the decoded mapping.
        """
        return self._wire_bytes


def private_message_wire_bytes(message: Mapping[str, object]) -> bytes | None:
    """Read genuine boundary receipt metadata without trusting provider JSON keys.

    Args:
        message: The object passed by the SDK to its application callback.

    Returns:
        Original private payload bytes, or explicit absence for other mappings.
    """
    return message.wire_bytes if isinstance(message, PrivateWireMessage) else None


def _unique_receipt_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Refuse duplicate keys at every object depth before financial interpretation."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Private execution receipt contains duplicate JSON keys")
        result[key] = value
    return result


def _reject_receipt_constant(value: str) -> Never:
    """Retain malformed evidence in storage while refusing non-JSON financial numbers."""
    raise ValueError(f"Private execution receipt contains a non-JSON numeric constant: {value}")


def decode_private_execution_receipt(wire_bytes: bytes) -> dict[str, object]:
    """Strictly reparse stored original evidence before interpreting any financial value.

    Args:
        wire_bytes: Original UTF-8 JSON payload, already preserved before interpretation.

    Returns:
        A unique-key object with exact decimal operands and no non-finite JSON extensions.
    """
    decoded: object = json.loads(
        wire_bytes,
        parse_float=Decimal,
        parse_constant=_reject_receipt_constant,
        object_pairs_hook=_unique_receipt_object,
    )
    if not isinstance(decoded, dict):
        raise ValueError("Private execution receipt must contain a JSON object")
    return cast(dict[str, object], decoded)


def is_execution_control_message(message: Mapping[str, object]) -> bool:
    """Exclude authenticated control envelopes while allowing financial data with an event field.

    Args:
        message: Actual SDK envelope before financial interpretation or durable capture.

    Returns:
        Whether the envelope is an RPC/control response or contains authentication material.
    """
    event = message.get("event")
    return (
        (isinstance(event, str) and event.strip().casefold() in _CONTROL_EVENTS)
        or isinstance(message.get("method"), str)
        or any(field in message for field in _AUTHENTICATION_FIELDS)
        or "python-kraken-sdk" in message
    )


def _is_private_message(message: object) -> bool:
    """Recognize only SDK channels carrying authenticated financial observations."""
    if not isinstance(message, dict) or is_execution_control_message(message):
        return False
    feed = message.get("feed")
    channel = message.get("channel")
    return (isinstance(feed, str) and feed in _PRIVATE_FUTURES_FEEDS) or (
        isinstance(channel, str) and channel in _PRIVATE_SPOT_CHANNELS
    )


class PrivateExecutionJsonCodec:
    """Delegate SDK JSON while preserving private financial decimal operands.

    Arbitrary keyword types are restricted to this external SDK/JSON wrapper;
    they preserve the standard decoder/encoder call signatures at the boundary.
    """

    @staticmethod
    def loads(data: str | bytes | bytearray, **kwargs: Any) -> object:
        """Capture private payload bytes without changing explicit caller decode options.

        Args:
            data: JSON payload received by the SDK WebSocket connector.
            **kwargs: Explicit standard JSON decoder options, passed through unchanged.

        Returns:
            Ordinary decoded data, or a private mapping with exact Decimal operands.
        """
        decoded: object = json.loads(data, **kwargs)
        if kwargs or not _is_private_message(decoded):
            return decoded
        exact: object = json.loads(data, parse_float=Decimal)
        original = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        return PrivateWireMessage(cast(dict[str, object], exact), original)

    @staticmethod
    def dumps(value: object, **kwargs: Any) -> str:
        """Preserve ordinary outbound SDK encoding without decimal coercion.

        Args:
            value: SDK outbound JSON value.
            **kwargs: Standard JSON encoder options supplied by the SDK.

        Returns:
            The unchanged standard encoder result.
        """
        return json.dumps(value, **kwargs)


def _install_connector_codec(connector: ModuleType) -> None:
    """Replace only the pinned SDK connector's module-local JSON dependency."""
    current = getattr(connector, "json", None)
    if isinstance(current, PrivateExecutionJsonCodec):
        return
    if current is not json:
        raise RuntimeError("Kraken WebSocket JSON boundary differs from the supported SDK")
    connector.__dict__["json"] = PrivateExecutionJsonCodec()


def install_private_execution_wire_capture() -> None:
    """Install idempotent private receipt capture in both pinned Kraken connectors."""
    _install_connector_codec(futures_websocket)
    _install_connector_codec(spot_websocket)
