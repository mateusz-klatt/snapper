"""Unit tests for Kraken exchange OHLC schemas."""

import pytest

from snapper.infrastructure.exchanges.schemas.kraken import KrakenCandleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscribeParamsSchema


def test_ohlc_subscribe_params_as_params() -> None:
    """Verify OHLC subscription params serialize correctly.

    Given KrakenOhlcSubscribeParamsSchema with symbol, interval, snapshot,
    When as_params() is called,
    Then returns dict with channel='ohlc' and all fields.
    """
    params = KrakenOhlcSubscribeParamsSchema(symbol=["BTC-USD"], interval=5, snapshot=False)
    rendered = params.as_params()
    assert rendered == {
        "channel": "ohlc",
        "symbol": ["BTC-USD"],
        "interval": 5,
        "snapshot": False,
    }


def test_ohlc_candle_as_dict_returns_floats() -> None:
    """Verify candle schema exports all fields as proper types.

    Given a KrakenCandleSchema with OHLCV data,
    When as_dict() is called,
    Then returns dict with float values for numeric fields.
    """
    candle = KrakenCandleSchema(
        symbol="BTC-USD",
        open=50000.0,
        high=50100.0,
        low=49900.0,
        close=50050.0,
        volume=12.5,
        trades=42,
        interval=1,
        timestamp="1700000000",
    )
    payload = candle.as_dict()
    assert payload["open"] == pytest.approx(50000.0)
    assert payload["volume"] == pytest.approx(12.5)
    assert payload["trades"] == 42
    assert payload["symbol"] == "BTC-USD"


def test_ohlc_message_helpers_return_primary_symbol_and_dicts() -> None:
    """Verify envelope helpers extract symbol and normalize candles.

    Given a KrakenOhlcEventEnvelope with two candles for ETH-USD,
    When primary_symbol() and as_dicts() are called,
    Then primary_symbol returns 'ETH-USD' and as_dicts returns 2 dicts.
    """
    candle_1 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3000.0,
        high=3050.0,
        low=2950.0,
        close=3025.0,
    )
    candle_2 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3025.0,
        high=3060.0,
        low=3010.0,
        close=3055.0,
    )
    message = KrakenOhlcEventEnvelope(channel="ohlc", type="snapshot", data=[candle_1, candle_2])
    normalized = message.as_dicts()
    assert message.primary_symbol() == "ETH-USD"
    assert len(normalized) == 2
    assert normalized[0]["close"] == pytest.approx(3025.0)
