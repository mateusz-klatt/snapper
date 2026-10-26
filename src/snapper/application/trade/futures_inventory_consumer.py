"""Project qualified inventory into read-only consumers without inventing command authority.

Original inventory remains independently retained with exact decimal operands.
The legacy account presentation can represent only positions whose additional
required mark and funding operands are actually present. Recovery observations
never authorize adoption, financial correction or a terminal order decision.
"""

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from typing import cast

from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPosition
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import OrderRow
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native


@dataclass(frozen=True)
class FuturesInventoryRecoveryObservation:
    """Keep original durable order identity and explicit inventory knowledge while parked.

    Neither an observed venue order nor its absence supplies the original
    selected submission context needed by financial recovery. These values are
    deliberately separate from ``PendingOrderState`` and never create a request.
    """

    order_public_id: str
    client_order_id: str | None
    venue_order_id: str | None
    presence: Literal["unavailable", "present", "absent"]
    reason: str


def observe_futures_recovery_order(
    order: OrderRow,
    inventory: FuturesOpenOrdersSnapshot | None,
    unavailable_reason: str,
) -> FuturesInventoryRecoveryObservation:
    """Park an actual durable order without interpreting unavailable inventory as absence.

    Args:
        order: Original row from the wallet-fenced active-order recovery query.
        inventory: Qualified complete observation, or no qualified observation.
        unavailable_reason: Fixed availability code from the actual current reader.

    Returns:
        Original identities and observed presence, with no fabricated admission or request.
    """
    venue_id = order.get("exchange_order_id")
    if inventory is None:
        presence: Literal["unavailable", "present", "absent"] = "unavailable"
        reason = unavailable_reason
    elif not venue_id:
        presence = "unavailable"
        reason = "venue_order_identity_unavailable"
    else:
        present = any(entry.venue_order_id == venue_id for entry in inventory.orders)
        presence = "present" if present else "absent"
        reason = "original_submission_context_unavailable"
    return FuturesInventoryRecoveryObservation(
        order_public_id=order["public_id"],
        client_order_id=order.get("client_order_id"),
        venue_order_id=venue_id,
        presence=presence,
        reason=reason,
    )


def _display_number(value: object) -> float:
    """Convert an original finite numeric operand only at the legacy display boundary."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise ValueError("futures_inventory_display_numeric_unavailable")
    exact = Decimal(value)
    displayed = float(exact)
    if not math.isfinite(displayed) or (exact and displayed == 0):
        raise ValueError("futures_inventory_display_numeric_unrepresentable")
    return displayed


def _position_presentation(
    position: FuturesOpenPosition, original: Mapping[str, object], observed_at: datetime
) -> JsonObject:
    """Retain genuine mark/funding operands without deriving a mark or defaulting funding."""
    if position.side not in ("long", "short"):
        raise ValueError("futures_inventory_display_side_unavailable")
    return {
        "symbol": kraken_futures_ws_to_native(position.wire_symbol.upper()),
        "side": "buy" if position.side == "long" else "sell",
        "size": _display_number(position.size),
        "entry_price": _display_number(position.average_entry_price),
        "mark_price": _display_number(original.get("markPrice")),
        "unrealized_pnl": _display_number(position.unrealized_pnl),
        "unrealized_funding": _display_number(position.unrealized_funding),
        "timestamp": observed_at.isoformat(),
    }


def serialize_futures_inventory_positions(
    inventory: FuturesOpenPositionsSnapshot, original_entity: bytes, observed_at: datetime
) -> str:
    """Project one qualified original position response into the narrower legacy display.

    Args:
        inventory: Actual current reader's fully parsed original inventory observation.
        original_entity: The same qualified retained entity, never a fresh unretained SDK response.
        observed_at: Original source observation instant, without renewing the source age.

    Returns:
        A complete legacy presentation, including an honestly observed empty array.

    Raises:
        ValueError: If original bytes differ or a required display operand cannot be represented.
    """
    if parse_futures_open_positions(original_entity) != inventory:
        raise ValueError("futures_inventory_display_source_mismatch")
    originals = cast(
        list[dict[str, object]], decode_private_execution_receipt(original_entity)["openPositions"]
    )
    return json.dumps(
        [
            _position_presentation(position, original, observed_at)
            for position, original in zip(inventory.positions, originals, strict=True)
        ],
        allow_nan=False,
    )
