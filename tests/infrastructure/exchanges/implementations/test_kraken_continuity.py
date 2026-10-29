"""Exercise the native history transport and actual live-observation trust boundaries."""

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import kraken as kr
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_rest_egress import KrakenRestOperationKind
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget


def _trade(at: datetime, symbol: str = "BTC-USD") -> TradeUpdate:
    """Create an independently timestamped live trade for a trust-boundary regression."""
    return TradeUpdate(symbol, "buy", 1.0, 100.0, "market", at)


def _client() -> KrakenExchangeClient:
    """Prepare only the transport and candle state used by these isolated unit paths."""
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    client.api_key = ""
    client.api_secret = ""
    client._subscription_cache = {}
    client._ws_client = None
    client._ws_connected = False
    client._trade_built_candle_builder = TradeCandleBuilder()
    client._trade_built_candle_queue = asyncio.Queue[CandleUpdate]()
    return client


def _native_payload(start: datetime, end: datetime) -> dict[str, object]:
    """Retain distinct native VWAP and volume fields, count and unfinished edge."""
    return {
        "error": [],
        "result": {
            "XXBTZUSD": [
                [int(start.timestamp()), "100.0", "103.0", "99.0", "102.0", "101.25", "1.75", 7],
                [int(end.timestamp()), "100.0", "103.0", "99.0", "102.0", "101.25", "1.75", 7],
            ],
            "last": int(start.timestamp()),
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("tokenized", [False, True])
async def test_closed_history_uses_one_routed_native_full_field_attempt(
    monkeypatch: pytest.MonkeyPatch, tokenized: bool
) -> None:
    """Acquire native eight-field evidence through one routed REST attempt.

    Given: A canonical product and fixed closed acquisition window,
    When: Native one-minute history is fetched,
    Then: Routed native transport preserves VWAP/count without generic OHLCV or retries.
    """
    client = _client()
    start = datetime(2026, 10, 28, 21, 18, tzinfo=UTC)
    end = datetime(2026, 10, 28, 21, 30, tzinfo=UTC)
    observed_at = end + timedelta(seconds=42)
    native_get = MagicMock(return_value=_native_payload(start, end))
    market = MagicMock(return_value={"symbol": "BTC/USD", "id": "XXBTZUSD"})
    client._ccxt_client = SimpleNamespace(market=market, publicGetOHLC=native_get)
    client.get_ohlcv = AsyncMock(side_effect=AssertionError("Generic OHLCV loses native fields"))
    calls: list[tuple[str, KrakenRestOperationKind, RestProxyTarget]] = []

    async def dispatch(
        *,
        operation: str,
        kind: KrakenRestOperationKind,
        target: RestProxyTarget,
        sync_call: Callable[[], object],
    ) -> object:
        calls.append((operation, kind, target))
        return sync_call()

    client._dispatch_routed_rest = AsyncMock(side_effect=dispatch)
    monkeypatch.setattr(kr, "native_to_ccxt", lambda symbol: "BTC/USD")
    monkeypatch.setattr(
        kr,
        "native_to_kraken_asset_class",
        lambda symbol: "tokenized_asset" if tokenized else "currency",
    )
    with patch.object(kr, "datetime", wraps=datetime) as clock:
        clock.now.return_value = observed_at
        candles = await client.get_closed_1m_candles("BTC-USD", start, end)

    parameters: dict[str, str | int] = {
        "pair": "XXBTZUSD",
        "interval": 1,
        "since": int(start.timestamp()) - 60,
    }
    if tokenized:
        parameters["asset_class"] = "tokenized_asset"
    market.assert_called_once_with("BTC/USD")
    native_get.assert_called_once_with(parameters)
    client._dispatch_routed_rest.assert_awaited_once()
    assert len(calls) == 1
    assert calls[0][:2] == ("closed_ohlc_catchup", "public_read")
    assert calls[0][2].client is client._ccxt_client
    client.get_ohlcv.assert_not_awaited()
    assert len(candles) == 1
    assert candles[0].interval_begin == start
    assert (candles[0].vwap, candles[0].volume, candles[0].trades) == (101.25, 1.75, 7)
    assert candles[0].complete is True
    clock.now.assert_called_once_with(UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "market",
    [
        {"symbol": "ETH/USD", "id": "XXBTZUSD"},
        {"symbol": "BTC/USD", "id": ""},
        {"symbol": "BTC/USD", "id": 1},
        {"symbol": "BTC/USD"},
    ],
)
async def test_closed_history_rejects_unbound_catalog_identity_before_dispatch(
    monkeypatch: pytest.MonkeyPatch, market: dict[str, object]
) -> None:
    """Bind the native response key to the requested canonical catalog product.

    Given: The catalog lookup has a mismatched symbol or unusable native identity,
    When: Native closed history is requested,
    Then: The request fails before any routed network dispatch.
    """
    client = _client()
    client._ccxt_client = SimpleNamespace(market=MagicMock(return_value=market))
    client._dispatch_routed_rest = AsyncMock()
    monkeypatch.setattr(kr, "native_to_ccxt", lambda symbol: "BTC/USD")
    start = datetime(2026, 10, 28, 21, 18, tzinfo=UTC)
    with pytest.raises(ValueError, match="canonical"):
        await client.get_closed_1m_candles("BTC-USD", start, start + timedelta(minutes=1))
    client._dispatch_routed_rest.assert_not_awaited()


@pytest.mark.asyncio
async def test_closed_history_propagates_one_acquisition_failure_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed native request remains unavailable evidence.

    Given: The one routed native acquisition fails,
    When: Closed history is requested,
    Then: The original failure propagates without a retry or generic fallback.
    """
    client = _client()
    client._ccxt_client = SimpleNamespace(
        market=MagicMock(return_value={"symbol": "BTC/USD", "id": "XXBTZUSD"})
    )
    client._dispatch_routed_rest = AsyncMock(side_effect=TimeoutError("native acquisition failed"))
    client.get_ohlcv = AsyncMock()
    monkeypatch.setattr(kr, "native_to_ccxt", lambda symbol: "BTC/USD")
    monkeypatch.setattr(kr, "native_to_kraken_asset_class", lambda symbol: "currency")
    start = datetime(2026, 10, 28, 21, 18, tzinfo=UTC)

    with pytest.raises(TimeoutError, match="native acquisition failed"):
        await client.get_closed_1m_candles("BTC-USD", start, start + timedelta(minutes=1))

    client._dispatch_routed_rest.assert_awaited_once()
    client.get_ohlcv.assert_not_awaited()


def test_live_client_uses_receipt_time_to_refuse_outage_fragments() -> None:
    """A delayed first event cannot certify the current partial minute after reconnect.

    Given: Receipt resumes after a minute boundary with an event from the preceding minute,
    When: Current and following-minute trades arrive,
    Then: Only the following fully observed minute is published.
    """
    client = _client()
    minute = datetime(2026, 10, 28, 21, 31, tzinfo=UTC)
    with patch.object(kr, "datetime", wraps=datetime) as clock:
        clock.now.return_value = minute + timedelta(seconds=10)
        client._fold_observed_trade_candle(_trade(minute - timedelta(seconds=1)))
        clock.now.return_value = minute + timedelta(seconds=21)
        client._fold_observed_trade_candle(_trade(minute + timedelta(seconds=20)))
        clock.now.return_value = minute + timedelta(minutes=1, seconds=2)
        client._fold_observed_trade_candle(_trade(minute + timedelta(minutes=1, seconds=1)))

    candles = client._trade_built_candle_builder.pop_completed(minute + timedelta(minutes=2))
    assert [c.interval_begin for c in candles] == [minute + timedelta(minutes=1)]
    assert candles[0].trades == 1
    assert client._trade_built_candle_builder.refused_trades == 2


def _seed_old_generation(client: KrakenExchangeClient) -> None:
    """Retain a previously trusted bucket and queued candle before a real feed break."""
    minute = datetime(2026, 10, 28, 21, 20, tzinfo=UTC)
    client._trade_built_candle_builder = TradeCandleBuilder(history_start=minute)
    client._trade_built_candle_builder.update(_trade(minute))
    candle = client._trade_built_candle_builder.pop_all()[0]
    client._trade_built_candle_queue.put_nowait(candle)


def test_explicit_feed_break_discards_retained_and_queued_old_generation() -> None:
    """A connection break removes correction trust and queued pre-break output.

    Given: The builder retains a trusted bucket and its queue contains old output,
    When: An explicit feed break resets the client generation,
    Then: Both retained state and queued candles are discarded.
    """
    client = _client()
    _seed_old_generation(client)

    client.reset_trade_candle_epoch()

    assert client._trade_built_candle_queue.empty()
    assert client._trade_built_candle_builder.retained_buckets == 0
    assert client._trade_built_candle_builder.update_count == 0


@pytest.mark.asyncio
async def test_new_sdk_connection_resets_candle_trust_before_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new SDK client starts without prior-generation candle state.

    Given: The previous generation retains trusted and queued candles,
    When: A new actual SDK client starts,
    Then: Its start callback sees neither retained nor queued old-generation data.
    """
    client = _client()
    _seed_old_generation(client)

    async def start() -> None:
        assert client._trade_built_candle_queue.empty()
        assert client._trade_built_candle_builder.retained_buckets == 0

    sdk_client = SimpleNamespace(start=AsyncMock(side_effect=start))
    monkeypatch.setattr(kr, "SpotWSClient", MagicMock(return_value=sdk_client))

    await client._start_new_ws_client()

    sdk_client.start.assert_awaited_once()
    assert client._ws_client is sdk_client


@pytest.mark.asyncio
async def test_actual_socket_close_discards_trust_before_await_and_after_late_callback() -> None:
    """Actual close fences both aggregator ticks and final teardown callbacks.

    Given: Trusted old state exists and another callback can arrive during close,
    When: The actual SDK socket is closed,
    Then: State is discarded before awaiting teardown and after the final callback.
    """
    client = _client()
    _seed_old_generation(client)

    async def close() -> None:
        assert client._trade_built_candle_queue.empty()
        assert client._trade_built_candle_builder.retained_buckets == 0
        _seed_old_generation(client)

    client._ws_client = SimpleNamespace(close=AsyncMock(side_effect=close))
    client._ws_connected = True

    await client._close_ws_client()

    assert client._trade_built_candle_queue.empty()
    assert client._trade_built_candle_builder.retained_buckets == 0
    assert client._ws_client is None
    assert client._ws_connected is False
