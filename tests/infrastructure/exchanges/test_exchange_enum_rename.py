"""Tests pinning the exchange-wire-format enum rename + StrEnum conversion.

Earlier these enums were plain ``Enum`` classes named
``OrderTypeEnum`` / ``OrderStatusEnum`` inside
``snapper.infrastructure.exchanges.contracts``. They were renamed to
``ExchangeOrderTypeEnum`` / ``ExchangeOrderStatusEnum`` and switched to
``StrEnum`` in the same change, so these tests assert two things:

1. Every wire-format value is byte-identical to its pre-rename form
   (no silent value drift).
2. ``StrEnum`` subclasses ``str``, giving exchange-side code the same
   string-operation ergonomics the domain side enjoys.
"""

from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum

_EXPECTED_ORDER_TYPE_VALUES: dict[ExchangeOrderTypeEnum, str] = {
    ExchangeOrderTypeEnum.LIMIT: "limit",
    ExchangeOrderTypeEnum.MARKET: "market",
    ExchangeOrderTypeEnum.ICEBERG: "iceberg",
    ExchangeOrderTypeEnum.STOP_LOSS: "stop-loss",
    ExchangeOrderTypeEnum.STOP_LOSS_LIMIT: "stop-loss-limit",
    ExchangeOrderTypeEnum.TAKE_PROFIT: "take-profit",
    ExchangeOrderTypeEnum.TAKE_PROFIT_LIMIT: "take-profit-limit",
    ExchangeOrderTypeEnum.TRAILING_STOP: "trailing-stop",
    ExchangeOrderTypeEnum.TRAILING_STOP_LIMIT: "trailing-stop-limit",
    ExchangeOrderTypeEnum.SETTLE_POSITION: "settle-position",
}

_EXPECTED_ORDER_STATUS_VALUES: dict[ExchangeOrderStatusEnum, str] = {
    ExchangeOrderStatusEnum.PENDING: "pending",
    ExchangeOrderStatusEnum.OPEN: "open",
    ExchangeOrderStatusEnum.CLOSED: "closed",
    ExchangeOrderStatusEnum.PENDING_NEW: "pending_new",
    ExchangeOrderStatusEnum.NEW: "new",
    ExchangeOrderStatusEnum.PARTIALLY_FILLED: "partially_filled",
    ExchangeOrderStatusEnum.FILLED: "filled",
    ExchangeOrderStatusEnum.CANCELED: "canceled",
    ExchangeOrderStatusEnum.EXPIRED: "expired",
}


def test_exchange_order_type_enum_values_unchanged() -> None:
    """Every ExchangeOrderTypeEnum value survives the Enum -> StrEnum conversion.

    Given: the renamed wire-format ExchangeOrderTypeEnum,
    When: reading .value on every member,
    Then: all 10 values match the pre-rename strings byte-for-byte
        (limit/market/iceberg + 7 stop/take-profit/trailing variants
        + settle-position).
    """
    actual = {member: member.value for member in ExchangeOrderTypeEnum}
    assert actual == _EXPECTED_ORDER_TYPE_VALUES


def test_exchange_order_status_enum_values_unchanged() -> None:
    """Every ExchangeOrderStatusEnum value survives the Enum -> StrEnum conversion.

    Given: the renamed wire-format ExchangeOrderStatusEnum,
    When: reading .value on every member,
    Then: all 9 values match the pre-rename strings byte-for-byte,
        including the American-spelling ``canceled`` which the
        cross-mapping in implementations/kraken.py +
        adapters/kraken_futures.py relies on.
    """
    actual = {member: member.value for member in ExchangeOrderStatusEnum}
    assert actual == _EXPECTED_ORDER_STATUS_VALUES


def test_strenum_inheritance() -> None:
    """Given: an Exchange*Enum member. When: compared against str.

    Then: it is a str instance and behaves as one (equality with raw
    strings, f-string interpolation emits the value not the repr).
    """
    assert isinstance(ExchangeOrderStatusEnum.OPEN, str)
    assert isinstance(ExchangeOrderTypeEnum.MARKET, str)
    assert ExchangeOrderStatusEnum.OPEN == "open"
    assert ExchangeOrderTypeEnum.MARKET == "market"
    assert f"{ExchangeOrderStatusEnum.CANCELED}" == "canceled"
    assert f"{ExchangeOrderTypeEnum.TRAILING_STOP}" == "trailing-stop"
