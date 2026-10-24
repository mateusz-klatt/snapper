"""Interpret complete original Futures inventory arrays without granting financial authority.

Literal provider identifiers, signed exact numeric observations and missing/null
distinctions remain intact. These values establish neither authenticated account
ownership nor present exchange completeness. Separate endpoint observations are
not an atomic account snapshot, and unsupported values cannot authorize orders.
"""

from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from typing import Self
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.infrastructure.exchanges.execution_wire_receipts import _CONTROL_EVENTS
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.private_control_decoder import _AUTH_NAMES

_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_MAX_ENTRIES = 16384
_MAX_SHAPE_NODES = 65536
_MAX_SHAPE_DEPTH = 64
_INVENTORY_AUTH_NAMES = _AUTH_NAMES | frozenset({"authent", "nonce", "privatekey", "apisecret"})


class FuturesInventorySourceError(ValueError):
    """The supplied entity cannot establish the supported complete inventory interpretation."""


class _InventoryValue(BaseModel):
    """Keep strict immutable observations separate from any source or financial capability."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class FuturesInventoryTime(_InventoryValue):
    """Original provider text and its explicit UTC instant, without local observation knowledge."""

    original_text: EvidenceText
    instant: EvidenceInstant

    @model_validator(mode="after")
    def validate_original_time(self) -> Self:
        """Bind the interpreted clock back to its literal source value.

        Returns:
            The unchanged consistent provider-clock observation.
        """
        if _instant(self.original_text) != self.instant:
            raise FuturesInventorySourceError("futures_inventory_time_copy_mismatch")
        return self


class FuturesOptionalNumber(_InventoryValue):
    """Distinguish an absent field from an explicit null or an exact numeric observation."""

    present: bool
    value: EvidenceDecimal | None

    @model_validator(mode="after")
    def validate_presence(self) -> Self:
        """Refuse numeric content attached to an asserted absent source member.

        Returns:
            The unchanged source-presence observation.
        """
        if not self.present and self.value is not None:
            raise FuturesInventorySourceError("futures_inventory_absent_value_supplied")
        return self


class FuturesOptionalText(_InventoryValue):
    """Preserve optional nullable provider text without inferring a default currency or identifier."""

    present: bool
    value: EvidenceText | None

    @model_validator(mode="after")
    def validate_presence(self) -> Self:
        """Refuse text attached to an asserted absent source member.

        Returns:
            The unchanged source-presence observation.
        """
        if not self.present and self.value is not None:
            raise FuturesInventorySourceError("futures_inventory_absent_value_supplied")
        return self


class FuturesOpenOrder(_InventoryValue):
    """One literal open-order observation without inferred remaining size or supported execution type."""

    venue_order_id: EvidenceText
    client_order_id: FuturesOptionalText
    wire_symbol: EvidenceText
    side: EvidenceText
    order_type: EvidenceText
    status: EvidenceText
    filled_size: EvidenceDecimal
    unfilled_size: FuturesOptionalNumber
    limit_price: FuturesOptionalNumber
    stop_price: FuturesOptionalNumber
    trigger_signal: FuturesOptionalText
    reduce_only: bool
    received_time: FuturesInventoryTime
    last_update_time: FuturesInventoryTime


class FuturesOpenPosition(_InventoryValue):
    """One position including unsettled mature contracts; source numerics retain their original signs."""

    wire_symbol: EvidenceText
    side: EvidenceText
    size: EvidenceDecimal
    average_entry_price: EvidenceDecimal
    unrealized_pnl: EvidenceDecimal
    unrealized_funding: EvidenceDecimal | None
    pnl_currency: FuturesOptionalText
    max_fixed_leverage: FuturesOptionalNumber


class FuturesOpenOrdersSnapshot(_InventoryValue):
    """A completely interpreted response array, not a current inventory or available-funds proof."""

    parser_policy: Literal["kraken_futures_open_orders_v1"] = "kraken_futures_open_orders_v1"
    server_time: FuturesInventoryTime
    orders: tuple[FuturesOpenOrder, ...] = Field(max_length=_MAX_ENTRIES)

    @model_validator(mode="after")
    def validate_order_identities(self) -> Self:
        """Refuse multiple entries naming the same UUID without merging or overwriting them.

        Returns:
            The unchanged complete unambiguous order observation.
        """
        try:
            identities = [UUID(order.venue_order_id) for order in self.orders]
        except ValueError:
            raise FuturesInventorySourceError("futures_inventory_order_identity_invalid") from None
        if len(set(identities)) != len(identities):
            raise FuturesInventorySourceError("futures_inventory_duplicate_order_identity")
        return self


class FuturesOpenPositionsSnapshot(_InventoryValue):
    """A locally supported one-row-per-exact-symbol interpretation, independent of market expiry."""

    parser_policy: Literal["kraken_futures_open_positions_v1"] = "kraken_futures_open_positions_v1"
    server_time: FuturesInventoryTime
    positions: tuple[FuturesOpenPosition, ...] = Field(max_length=_MAX_ENTRIES)

    @model_validator(mode="after")
    def validate_position_identities(self) -> Self:
        """Refuse unsupported ambiguous position layouts rather than inventing netting semantics.

        Returns:
            The unchanged inventory supported by a one-position-per-symbol consumer.
        """
        symbols = [position.wire_symbol for position in self.positions]
        if len(set(symbols)) != len(symbols):
            raise FuturesInventorySourceError("futures_inventory_unsupported_duplicate_positions")
        return self


def _object(value: object) -> Mapping[str, object]:
    """Require an original JSON object without inventing an empty replacement."""
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise FuturesInventorySourceError("futures_inventory_object_required")
    return value


def _text(value: object) -> str:
    """Retain exact nonempty provider text without trimming or case folding."""
    if not isinstance(value, str) or not value.strip():
        raise FuturesInventorySourceError("futures_inventory_text_required")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise FuturesInventorySourceError("futures_inventory_text_not_unicode") from None
    return value


def _number(value: object) -> Decimal:
    """Accept only finite original JSON numbers, never Boolean, numeric text or binary float."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise FuturesInventorySourceError("futures_inventory_numeric_required")
    return Decimal(value)


def _instant(value: str) -> datetime:
    """Interpret an explicit UTC provider clock without treating it as local receipt time."""
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise FuturesInventorySourceError("futures_inventory_timestamp_invalid") from None
    if result.utcoffset() != timedelta(0):
        raise FuturesInventorySourceError("futures_inventory_timestamp_not_utc")
    return result.astimezone(UTC)


def _time(value: object) -> FuturesInventoryTime:
    """Retain both the exact time text and its validated explicit UTC interpretation."""
    original = _text(value)
    return FuturesInventoryTime(original_text=original, instant=_instant(original))


def _optional_number(
    source: Mapping[str, object], name: str, *, nullable: bool = False
) -> FuturesOptionalNumber:
    """Preserve absent and documented nullable states without manufacturing zero."""
    if name not in source:
        return FuturesOptionalNumber(present=False, value=None)
    value = source[name]
    if value is None and nullable:
        return FuturesOptionalNumber(present=True, value=None)
    return FuturesOptionalNumber(present=True, value=_number(value))


def _optional_text(
    source: Mapping[str, object], name: str, *, nullable: bool = False
) -> FuturesOptionalText:
    """Preserve actual optional identifiers and currencies without assuming missing defaults."""
    if name not in source:
        return FuturesOptionalText(present=False, value=None)
    value = source[name]
    if value is None and nullable:
        return FuturesOptionalText(present=True, value=None)
    return FuturesOptionalText(present=True, value=_text(value))


def _boolean(value: object) -> bool:
    """Require an actual JSON Boolean for the source reduction flag."""
    if not isinstance(value, bool):
        raise FuturesInventorySourceError("futures_inventory_boolean_required")
    return value


def _order(value: object) -> FuturesOpenOrder:
    """Project one complete row while leaving optional remaining size and literal enum support explicit."""
    source = _object(value)
    return FuturesOpenOrder(
        venue_order_id=_text(source.get("order_id")),
        client_order_id=_optional_text(source, "cliOrdId"),
        wire_symbol=_text(source.get("symbol")),
        side=_text(source.get("side")),
        order_type=_text(source.get("orderType")),
        status=_text(source.get("status")),
        filled_size=_number(source.get("filledSize")),
        unfilled_size=_optional_number(source, "unfilledSize"),
        limit_price=_optional_number(source, "limitPrice"),
        stop_price=_optional_number(source, "stopPrice"),
        trigger_signal=_optional_text(source, "triggerSignal"),
        reduce_only=_boolean(source.get("reduceOnly")),
        received_time=_time(source.get("receivedTime")),
        last_update_time=_time(source.get("lastUpdateTime")),
    )


def _position(value: object) -> FuturesOpenPosition:
    """Preserve all required position observations including explicit null funding and mature symbols."""
    source = _object(value)
    if "unrealizedFunding" not in source:
        raise FuturesInventorySourceError("futures_inventory_required_funding_missing")
    funding = source["unrealizedFunding"]
    return FuturesOpenPosition(
        wire_symbol=_text(source.get("symbol")),
        side=_text(source.get("side")),
        size=_number(source.get("size")),
        average_entry_price=_number(source.get("price")),
        unrealized_pnl=_number(source.get("unrealizedPnl")),
        unrealized_funding=None if funding is None else _number(funding),
        pnl_currency=_optional_text(source, "pnlCurrency", nullable=True),
        max_fixed_leverage=_optional_number(source, "maxFixedLeverage", nullable=True),
    )


def _private_object_values(source: Mapping[str, object]) -> list[object]:
    """Inspect field names as strip/casefold privacy labels, without rewriting source identifiers."""
    for name, value in source.items():
        label = name.strip().casefold()
        if label in _INVENTORY_AUTH_NAMES or label == "python-kraken-sdk":
            raise FuturesInventorySourceError("futures_inventory_control_message_refused")
        if label == "method" and isinstance(value, str):
            raise FuturesInventorySourceError("futures_inventory_control_message_refused")
        if (
            label == "event"
            and isinstance(value, str)
            and value.strip().casefold() in _CONTROL_EVENTS
        ):
            raise FuturesInventorySourceError("futures_inventory_control_message_refused")
    return list(source.values())


def _require_private_body(source: Mapping[str, object]) -> None:
    """Bound the complete decoded tree and refuse auth/control fields at every depth.

    These are explicit local parser-v1 resource/privacy limits, not provider
    schema claims. Literal financial text and values are never normalized.
    Trusted transport must still independently classify before retention/hash.
    """
    pending: list[tuple[object, int]] = [(source, 0)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > _MAX_SHAPE_NODES or depth > _MAX_SHAPE_DEPTH:
            raise FuturesInventorySourceError("futures_inventory_shape_bound")
        if isinstance(value, Mapping):
            pending.extend((child, depth + 1) for child in _private_object_values(value))
        elif isinstance(value, list):
            pending.extend((child, depth + 1) for child in value)


def _document(wire_bytes: bytes) -> Mapping[str, object]:
    """Decode bounded exact JSON before interpreting fields, without hashing or retaining an entity."""
    if not isinstance(wire_bytes, bytes) or len(wire_bytes) > _MAX_ENTITY_BYTES:
        raise FuturesInventorySourceError("futures_inventory_original_entity_required")
    try:
        decoded_text = wire_bytes.decode("utf-8")
        if "\x00" in decoded_text:
            raise ValueError("futures_inventory_literal_nul_refused")
        source = decode_private_execution_receipt(wire_bytes)
    except ValueError, UnicodeError, RecursionError, ArithmeticError:
        raise FuturesInventorySourceError("futures_inventory_original_json_invalid") from None
    _require_private_body(source)
    if source.get("result") != "success" or "error" in source or "errors" in source:
        raise FuturesInventorySourceError("futures_inventory_success_unproven")
    return source


def _entries(source: Mapping[str, object], name: str) -> list[object]:
    """Require the entire bounded array rather than silently truncating or skipping unknown rows."""
    result = source.get(name)
    if not isinstance(result, list) or len(result) > _MAX_ENTRIES:
        raise FuturesInventorySourceError("futures_inventory_complete_array_required")
    return result


def parse_futures_open_orders(wire_bytes: bytes) -> FuturesOpenOrdersSnapshot:
    """Interpret every row in one original open-orders response without inventing financial permission.

    Args:
        wire_bytes: Original decoded HTTP entity bytes, independently retained by the trusted producer.

    Returns:
        Complete literal observations requiring separate source, freshness and financial checks.

    Raises:
        ValueError: When syntax, required fields or locally supported inventory identity is invalid.
    """
    source = _document(wire_bytes)
    return FuturesOpenOrdersSnapshot(
        server_time=_time(source.get("serverTime")),
        orders=tuple(_order(value) for value in _entries(source, "openOrders")),
    )


def parse_futures_open_positions(wire_bytes: bytes) -> FuturesOpenPositionsSnapshot:
    """Interpret all positions including mature unsettled contracts without inferring current flatness.

    Args:
        wire_bytes: Original decoded HTTP entity bytes, independently retained by the trusted producer.

    Returns:
        Complete original position observations without netting, currency defaults or source authority.

    Raises:
        ValueError: When syntax, required fields or the supported one-row-per-symbol shape is invalid.
    """
    source = _document(wire_bytes)
    return FuturesOpenPositionsSnapshot(
        server_time=_time(source.get("serverTime")),
        positions=tuple(_position(value) for value in _entries(source, "openPositions")),
    )
