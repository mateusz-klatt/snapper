"""Tests for the canonical paired-execution key helpers."""

from snapper.core.paired_execution import compute_paired_group_key
from snapper.core.paired_execution import paired_group_leg_token
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum


def test_leg_token_is_exchange_instrument_mode() -> None:
    """A leg token is the canonical ``{exchange}:{instrument}:{mode}`` string.

    Given: an exchange, instrument and mode,
    When: the leg token is built,
    Then: the three are joined with ``:`` in that order.
    """
    token = paired_group_leg_token(ExchangeEnum.KRAKEN, "BTC-USD", ExecutionModeEnum.LIVE)
    assert token == "kraken:BTC-USD:live"


def test_group_key_is_sorted_and_order_independent() -> None:
    """The group key sorts its leg tokens so leg order does not change it.

    Given: the same two legs supplied in opposite orders,
    When: the group key is computed for each ordering,
    Then: both produce the identical sorted, ``|``-joined key.
    """
    forward = compute_paired_group_key(
        [
            (ExchangeEnum.KRAKEN, "BTC-USD", ExecutionModeEnum.LIVE),
            (ExchangeEnum.KRAKEN, "ETH-USD", ExecutionModeEnum.LIVE),
        ]
    )
    reverse = compute_paired_group_key(
        [
            (ExchangeEnum.KRAKEN, "ETH-USD", ExecutionModeEnum.LIVE),
            (ExchangeEnum.KRAKEN, "BTC-USD", ExecutionModeEnum.LIVE),
        ]
    )
    assert forward == "kraken:BTC-USD:live|kraken:ETH-USD:live"
    assert forward == reverse
