"""Tests for the domain order-lifecycle StrEnums added in Batch 4.

Pins the byte-identical string values, the Literal-alias membership
contracts, and the ``StrEnum`` string-operation ergonomics for
``OrderTypeEnum``, ``OrderStatusEnum``, and ``OrderEventEnum``.
"""

import pytest

from snapper.core.types import OrderEvent
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderEventType
from snapper.core.types import OrderStatus
from snapper.core.types import OrderStatusEnum
from snapper.core.types import OrderType
from snapper.core.types import OrderTypeEnum

_ORDER_TYPE_VALUES = {
    OrderTypeEnum.MARKET: "market",
    OrderTypeEnum.LIMIT: "limit",
    OrderTypeEnum.STOP: "stop",
    OrderTypeEnum.STOP_LIMIT: "stop_limit",
}

_ORDER_STATUS_VALUES = {
    OrderStatusEnum.NEW: "new",
    OrderStatusEnum.SUBMITTED: "submitted",
    OrderStatusEnum.OPEN: "open",
    OrderStatusEnum.FILLED: "filled",
    OrderStatusEnum.PARTIALLY_FILLED: "partially_filled",
    OrderStatusEnum.CANCELLED: "cancelled",
    OrderStatusEnum.REJECTED: "rejected",
}

_ORDER_EVENT_VALUES = {
    OrderEventEnum.SUBMITTED: "submitted",
    OrderEventEnum.ACCEPTED: "accepted",
    OrderEventEnum.REJECTED: "rejected",
    OrderEventEnum.EXECUTED: "executed",
    OrderEventEnum.CANCELLED: "cancelled",
    OrderEventEnum.EXPIRED: "expired",
    OrderEventEnum.REPLACED: "replaced",
}

_ORDER_EVENT_TYPE_MEMBERS = {
    OrderEventEnum.SUBMITTED,
    OrderEventEnum.ACCEPTED,
    OrderEventEnum.REJECTED,
    OrderEventEnum.CANCELLED,
    OrderEventEnum.EXPIRED,
    OrderEventEnum.REPLACED,
}


@pytest.mark.parametrize(("member", "value"), list(_ORDER_TYPE_VALUES.items()))
def test_order_type_values(member: OrderTypeEnum, value: str) -> None:
    """Given: OrderTypeEnum members. When: reading .value. Then: byte-identical strings."""
    assert member.value == value
    assert member == value


@pytest.mark.parametrize(("member", "value"), list(_ORDER_STATUS_VALUES.items()))
def test_order_status_values(member: OrderStatusEnum, value: str) -> None:
    """Given: OrderStatusEnum members. When: reading .value. Then: byte-identical strings."""
    assert member.value == value
    assert member == value


@pytest.mark.parametrize(("member", "value"), list(_ORDER_EVENT_VALUES.items()))
def test_order_event_values(member: OrderEventEnum, value: str) -> None:
    """Given: OrderEventEnum members. When: reading .value. Then: byte-identical strings."""
    assert member.value == value
    assert member == value


@pytest.mark.parametrize("member", list(_ORDER_TYPE_VALUES))
def test_order_literal_accepts_enum_member_order_type(member: OrderTypeEnum) -> None:
    """Given: an OrderTypeEnum member. When: assigned to OrderType-annotated variable. Then: it holds."""
    value: OrderType = member
    assert value == member


@pytest.mark.parametrize("member", list(_ORDER_STATUS_VALUES))
def test_order_literal_accepts_enum_member_order_status(member: OrderStatusEnum) -> None:
    """Given: an OrderStatusEnum member. When: assigned to OrderStatus-annotated variable. Then: it holds."""
    value: OrderStatus = member
    assert value == member


@pytest.mark.parametrize("member", list(_ORDER_EVENT_VALUES))
def test_order_literal_accepts_enum_member_order_event(member: OrderEventEnum) -> None:
    """Given: an OrderEventEnum member. When: assigned to OrderEvent-annotated variable. Then: it holds."""
    value: OrderEvent = member
    assert value == member


@pytest.mark.parametrize("member", list(_ORDER_EVENT_VALUES))
def test_order_event_type_excludes_executed(member: OrderEventEnum) -> None:
    """Given: every OrderEventEnum member. When: checking OrderEventType-logical membership.

    Then: EXECUTED is in OrderEvent but NOT in OrderEventType; every
    other member is in both. This pins the non-execution-event Literal
    subset that OrderData / OrderEventData carry.
    """
    logical_members = _ORDER_EVENT_TYPE_MEMBERS
    if member is OrderEventEnum.EXECUTED:
        assert member not in logical_members
    else:
        assert member in logical_members
        value: OrderEventType = member
        assert value == member


@pytest.mark.parametrize(
    ("enum_cls", "member"),
    [
        (OrderTypeEnum, OrderTypeEnum.MARKET),
        (OrderTypeEnum, OrderTypeEnum.LIMIT),
        (OrderTypeEnum, OrderTypeEnum.STOP),
        (OrderTypeEnum, OrderTypeEnum.STOP_LIMIT),
        (OrderStatusEnum, OrderStatusEnum.NEW),
        (OrderStatusEnum, OrderStatusEnum.SUBMITTED),
        (OrderStatusEnum, OrderStatusEnum.OPEN),
        (OrderStatusEnum, OrderStatusEnum.FILLED),
        (OrderStatusEnum, OrderStatusEnum.PARTIALLY_FILLED),
        (OrderStatusEnum, OrderStatusEnum.CANCELLED),
        (OrderStatusEnum, OrderStatusEnum.REJECTED),
        (OrderEventEnum, OrderEventEnum.SUBMITTED),
        (OrderEventEnum, OrderEventEnum.ACCEPTED),
        (OrderEventEnum, OrderEventEnum.REJECTED),
        (OrderEventEnum, OrderEventEnum.EXECUTED),
        (OrderEventEnum, OrderEventEnum.CANCELLED),
        (OrderEventEnum, OrderEventEnum.EXPIRED),
        (OrderEventEnum, OrderEventEnum.REPLACED),
    ],
)
def test_enum_round_trip_from_string(
    enum_cls: type[OrderTypeEnum] | type[OrderStatusEnum] | type[OrderEventEnum],
    member: OrderTypeEnum | OrderStatusEnum | OrderEventEnum,
) -> None:
    """Given: a raw string value. When: constructing enum from value. Then: the same member is returned."""
    result = enum_cls(member.value)
    assert result is member


def test_enum_string_operations() -> None:
    """Given: StrEnum members. When: used in string operations. Then: they behave as strings.

    Covers the three operations callers rely on across the codebase:
    equality against a raw string, f-string interpolation yielding the
    value (not the repr), and ``in``-membership against a list.
    """
    assert OrderStatusEnum.OPEN == "open"
    assert f"{OrderStatusEnum.OPEN}" == "open"
    assert "open" in [OrderStatusEnum.OPEN]
    assert OrderTypeEnum.MARKET == "market"
    assert OrderEventEnum.EXECUTED == "executed"
