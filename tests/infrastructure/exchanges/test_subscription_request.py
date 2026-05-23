"""Tests for replayable subscription request helpers."""

import json

from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges._subscription_request import canonicalise_parameters


def test_key_is_order_insensitive_for_symbols() -> None:
    """Subscription keys collapse equivalent symbol sets.

    Given: Two subscription requests with the same symbols in different order,
    When: Their keys are computed,
    Then: The keys are equal.
    """
    left = SubscriptionRequest(
        channel="ticker", symbols=("BTC/USD", "ETH/USD"), parameters_json="{}"
    )
    right = SubscriptionRequest(
        channel="ticker",
        symbols=("ETH/USD", "BTC/USD"),
        parameters_json="{}",
    )
    assert left.key() == right.key()


def test_key_distinguishes_ohlc_interval() -> None:
    """Subscription keys keep material parameters distinct.

    Given: Two OHLC subscription requests for the same symbol set and different intervals,
    When: Their keys are computed,
    Then: The keys are different.
    """
    one_minute = SubscriptionRequest(
        channel="ohlc",
        symbols=("BTC/USD",),
        parameters_json='{"interval": 1}',
    )
    five_minute = SubscriptionRequest(
        channel="ohlc",
        symbols=("BTC/USD",),
        parameters_json='{"interval": 5}',
    )
    assert one_minute.key() != five_minute.key()


def test_canonicalise_parameters_strips_symbol_keys() -> None:
    """Symbol-bearing fields are removed from canonical parameters.

    Given: A subscribe parameter payload with all known symbol aliases,
    When: The payload is canonicalised,
    Then: Only non-symbol parameters remain.
    """
    encoded = canonicalise_parameters(
        {
            "channel": "ticker",
            "symbol": ["BTC/USD"],
            "symbols": ["ETH/USD"],
            "product": "PF_XBTUSD",
            "products": ["PF_ETHUSD"],
            "snapshot": True,
        }
    )
    assert json.loads(encoded) == {"channel": "ticker", "snapshot": True}


def test_canonicalise_parameters_sorts_keys() -> None:
    """Parameter canonicalisation is stable across insertion order.

    Given: Two equivalent payloads with different insertion order,
    When: The payloads are canonicalised,
    Then: The encoded JSON strings are identical.
    """
    first = canonicalise_parameters({"snapshot": True, "channel": "ticker"})
    second = canonicalise_parameters({"channel": "ticker", "snapshot": True})
    assert first == second
