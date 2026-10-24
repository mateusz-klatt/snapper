"""Refuse authentication-bearing product responses before retaining original bytes or their digest.

Unknown safe product metadata remains in the exact original entity. This check
recognizes authentication field names at every JSON depth, without treating a
generic instrument name or string value as an authentication field. Invalid or
ambiguous JSON is never persisted as a product success source.
"""

from collections.abc import Mapping

from snapper.application.trade.futures_trading_instruments import FuturesProductSourceError
from snapper.application.trade.futures_trading_instruments import FuturesTradingInstrumentsSnapshot
from snapper.application.trade.futures_trading_instruments import parse_futures_trading_instruments
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message
from snapper.infrastructure.exchanges.private_control_decoder import _AUTH_NAMES

_PRODUCT_AUTH_NAMES = _AUTH_NAMES | {"authent", "nonce", "privatekey", "apisecret"}
_MAX_ENTITY_BYTES = 8 * 1024 * 1024


class FuturesProductUnsafeSourceError(FuturesProductSourceError):
    """A product response contains control or authentication fields and cannot be retained."""


def _require_safe_fields(document: Mapping[str, object]) -> None:
    """Inspect all strict decoded children before any success source is hashed or persisted."""
    if is_execution_control_message(document):
        raise FuturesProductUnsafeSourceError("futures_product_control_source_refused")
    pending: list[object] = [document]
    while pending:
        node = pending.pop()
        if isinstance(node, Mapping):
            if any(name.casefold() in _PRODUCT_AUTH_NAMES for name in node):
                raise FuturesProductUnsafeSourceError(
                    "futures_product_authentication_source_refused"
                )
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)


def parse_safe_futures_product_source(wire_bytes: bytes) -> FuturesTradingInstrumentsSnapshot:
    """Classify an entire successful product source before allowing original entity retention.

    Args:
        wire_bytes: Actual closed-transport entity, still transient and never included in errors.

    Returns:
        Complete strict parsed observations without independently asserting authenticated provenance.

    Raises:
        FuturesProductSourceError: If original syntax, completeness or product identity is invalid.
        FuturesProductUnsafeSourceError: If any authentication or control material is detected.
    """
    if not isinstance(wire_bytes, bytes) or len(wire_bytes) > _MAX_ENTITY_BYTES:
        raise FuturesProductSourceError("futures_product_original_entity_required")
    try:
        document = decode_private_execution_receipt(wire_bytes)
    except (ValueError, UnicodeError, RecursionError):
        raise FuturesProductSourceError("futures_product_original_json_invalid") from None
    _require_safe_fields(document)
    return parse_futures_trading_instruments(wire_bytes)
