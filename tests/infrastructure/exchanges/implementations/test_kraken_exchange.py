"""Tests for Kraken exchange client implementation."""

import asyncio
import contextlib
import threading
import time
from collections.abc import Coroutine
from dataclasses import asdict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import ccxt
import pytest
import requests
from ccxt.base.errors import NetworkError
from loguru import logger
from pydantic import ValidationError
from pytest import MonkeyPatch

from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.errors import RestPoolDispatchError
from snapper.infrastructure.exchanges.implementations import kraken as kr
from snapper.infrastructure.exchanges.implementations.kraken import (
    _OHLC_DEPRECATED_TIMESTAMP_NOTICE,
)
from snapper.infrastructure.exchanges.implementations.kraken import _OHLC_NOTICES_LOGGED
from snapper.infrastructure.exchanges.implementations.kraken import _RESUBSCRIBE_CHUNK_DELAY_S
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken import _enqueue_or_drop_oldest


class _DummyWs:
    """Test dummy for Kraken WebSocket connection."""

    def __init__(self, queue: asyncio.Queue[Any]):
        self.queue = queue
        self.subscriptions: list[tuple[dict[str, Any], int | None]] = []
        self.exception_occur = False
        self.closed = False

    async def __aenter__(self) -> _DummyWs:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    async def subscribe(self, *, params: dict[str, Any], req_id: int | None = None) -> None:
        self.subscriptions.append((params, req_id))

    async def close(self) -> None:
        self.closed = True


def _client() -> KrakenExchangeClient:
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    client.api_key = "key"
    client.api_secret = "secret"
    client.exchange_name = "kraken"
    client._ws_connected = True
    client._tick_queue = asyncio.Queue[TickerUpdate]()
    client._candle_queues = {}
    client._trade_queue = asyncio.Queue[TradeUpdate]()
    client._trade_built_candle_builder = TradeCandleBuilder(interval_seconds=60)
    client._trade_built_candle_queue = asyncio.Queue[CandleUpdate]()
    client._trade_built_candle_aggregator_task = None
    client._trade_built_candles_enabled = False
    client._ccxt_client = SimpleNamespace()
    client._subscription_cache = {}
    client._health_tracker = SubscriptionHealthTracker()
    client.settings = SimpleNamespace(trade_built_finalize_grace_seconds=12)
    client._rest_pool = None
    client._rest_pool_closed = False

    async def _with_retry(fn: Any, *args: Any, **kwargs: Any) -> Any:
        kwargs.pop("retry_network_errors", None)
        kwargs.pop("egress_kind", None)
        kwargs.pop("egress_operation", None)
        kwargs.pop("egress_target", None)
        return await fn(*args, **kwargs)

    client._with_retry = _with_retry
    return client


@pytest.mark.asyncio
async def test_handle_channel_data_drops_ticker_snapshot_envelope() -> None:
    """Ticker snapshot envelope is dropped.

    Given: A Kraken ticker frame marked as a snapshot,
    When: _handle_channel_data dispatches the frame,
    Then: The ticker handler is not called.
    """
    client = _client()
    client._handle_ticker_data = MagicMock()

    await client._handle_channel_data(
        {"channel": "ticker", "type": "snapshot", "data": [{"symbol": "BTC/USD"}]}
    )

    client._handle_ticker_data.assert_not_called()


def test_spot_health_tracker_dark_recovers_trades_too() -> None:
    """Spot dark recovery covers the trade channel.

    Given: A freshly constructed Spot client,
    When: Its health tracker is inspected,
    Then: dark_recovery_channels includes trade so trade-built candles have a
        slow dark-channel backstop.

    Returns:
        None.
    """
    client = KrakenExchangeClient()
    assert client._health_tracker.dark_recovery_channels == frozenset({"ticker", "trade"})


@pytest.mark.asyncio
async def test_spot_trade_dark_recovery_resubscribes_trade(
    monkeypatch: MonkeyPatch,
) -> None:
    """A genuinely dark Spot trade subscription is re-subscribed.

    Given: A Kraken Spot client whose confirmed trade subscription has produced
        no recent data past the dark-recovery threshold,
    When: The inherited dark-subscription recovery pass runs,
    Then: The client re-subscribes the trade channel for that symbol.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        None.
    """
    client = _client()
    ws = _DummyWs(asyncio.Queue())
    client._ws_client = ws
    tracker = SubscriptionHealthTracker(
        data_stale_threshold_s=100.0,
        dark_recovery_threshold_multiplier=3.0,
        retry_subscribe_spacing_s=0.0,
        dark_recovery_channels=frozenset({"trade"}),
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges._subscription_health.time.monotonic", lambda: 0.0
    )
    tracker.mark_confirmed("trade", "XBT/USD")
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges._subscription_health.time.monotonic", lambda: 400.0
    )
    await client._recover_dark_subscriptions(tracker)
    assert ws.subscriptions == [
        ({"channel": "trade", "symbol": ["XBT/USD"], "snapshot": False}, None),
    ]
    entry = tracker.snapshot()[("trade", "XBT/USD")]
    assert (entry.status, entry.dark_recovery_count) == ("pending", 1)


@pytest.mark.asyncio
async def test_handle_channel_data_drops_trade_snapshot_envelope() -> None:
    """Trade snapshot envelope is dropped.

    Given: A Kraken trade frame marked as a snapshot,
    When: _handle_channel_data dispatches the frame,
    Then: The trade handler is not called.
    """
    client = _client()
    client._handle_trade_data = MagicMock()

    await client._handle_channel_data(
        {"channel": "trade", "type": "snapshot", "data": [{"symbol": "BTC/USD"}]}
    )

    client._handle_trade_data.assert_not_called()


@pytest.mark.asyncio
async def test_handle_channel_data_passes_ticker_update_envelope() -> None:
    """Ticker update envelope reaches the ticker handler.

    Given: A Kraken ticker frame marked as an update,
    When: _handle_channel_data dispatches the frame,
    Then: The ticker handler receives the frame data.
    """
    client = _client()
    client._handle_ticker_data = MagicMock()
    data = [{"symbol": "BTC/USD"}]

    await client._handle_channel_data({"channel": "ticker", "type": "update", "data": data})

    client._handle_ticker_data.assert_called_once_with(data)


@pytest.mark.asyncio
async def test_handle_channel_data_passes_ohlc_snapshot_envelope() -> None:
    """OHLC snapshot envelope reaches the OHLC handler.

    Given: A Kraken OHLC frame marked as a snapshot,
    When: _handle_channel_data dispatches the frame,
    Then: The OHLC handler receives the frame data.
    """
    client = _client()
    client._handle_ohlc_data = MagicMock()
    data = [{"symbol": "BTC/USD", "interval": 1}]

    await client._handle_channel_data({"channel": "ohlc", "type": "snapshot", "data": data})

    client._handle_ohlc_data.assert_called_once_with(data)


def test_get_ccxt_client_returns_ccxt_instance() -> None:
    """CCXT client accessor.

    Given a KrakenExchangeClient instance,
    When get_ccxt_client is called,
    Then it returns the internal CCXT client instance.
    """
    client = _client()
    ccxt_client = client.get_ccxt_client()
    assert ccxt_client is client._ccxt_client


@pytest.mark.asyncio
async def test_close_ws_client_shuts_down_session() -> None:
    """WebSocket client shutdown.

    Given a KrakenExchangeClient with an active WebSocket connection,
    When _close_ws_client is called,
    Then the WebSocket client and session are closed and references cleared.
    """
    client = _client()

    class _DummySession:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class _DummyWsClient:
        def __init__(self) -> None:
            self.closed = False
            self._SpotAsyncClient__session = _DummySession()

        async def close(self) -> None:
            self.closed = True

    client._ws_client = _DummyWsClient()
    client._ws_connected = True
    await client._close_ws_client()
    assert client._ws_client is None
    assert client._ws_connected is False


@pytest.mark.asyncio
async def test_get_balance_filters_and_returns_currency() -> None:
    """Balance retrieval with currency filtering.

    Given a KrakenExchangeClient with a mocked balance response,
    When get_balance is called with or without a currency filter,
    Then it returns AccountBalance objects filtered appropriately.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {
            "free": 0,
            "info": {},
            "USD": {"free": 1, "used": 2, "total": 3},
            "EUR": {"free": 0, "used": 0, "total": 0},
        }

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    balances = await client.get_balance()
    assert balances["USD"] == AccountBalance("USD", 1.0, 2.0, 3.0)
    assert "info" not in balances
    eur_only = await client.get_balance("EUR")
    assert set(eur_only.keys()) == {"EUR"}
    assert eur_only["EUR"].total == pytest.approx(0.0)


def test_kraken_declares_native_account_capabilities() -> None:
    """Kraken spot advertises balance-supported, positions-not-applicable.

    Given the KrakenExchangeClient class,
    When its account-reading capability attributes are inspected,
    Then balance is SUPPORTED and positions are NOT_APPLICABLE (spot has no
        derivatives positions).
    """
    assert KrakenExchangeClient.balance_capability is CapabilityStatus.SUPPORTED
    assert KrakenExchangeClient.position_capability is CapabilityStatus.NOT_APPLICABLE


@pytest.mark.asyncio
async def test_read_native_balances_faithful_multi_currency() -> None:
    """Native balance read returns one faithful entry per currency.

    Given a balance envelope carrying every aggregate metadata key plus two real
        currencies each with finite total/free/used,
    When read_native_balances is called,
    Then only the two currencies yield NativeBalanceEntry values with their
        venue-reported total/free/used, and the aggregate metadata keys are
        skipped.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {
            "free": {"BTC": 1.5, "USD": 10000.0},
            "used": {"BTC": 0.5, "USD": 2000.0},
            "total": {"BTC": 2.0, "USD": 12000.0},
            "info": {},
            "timestamp": 1,
            "datetime": "2022-01-01T00:00:00.000Z",
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
        }

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    entries = await client.read_native_balances()
    assert entries == [
        NativeBalanceEntry(currency="BTC", total=2.0, free=1.5, used=0.5),
        NativeBalanceEntry(currency="USD", total=12000.0, free=10000.0, used=2000.0),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_row", [None, 0.0, 5, "not-a-dict", ["total"]])
async def test_read_native_balances_rejects_non_dict_currency_row(bad_row: object) -> None:
    """A non-aggregate currency row that is not a mapping raises ValueError.

    Given a balance envelope with a valid currency plus a non-aggregate currency
        key whose value is null or a scalar rather than a mapping,
    When read_native_balances is called,
    Then it raises ValueError instead of silently dropping the malformed row into
        an authoritative observed-empty reading.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": bad_row,
        }

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    with pytest.raises(ValueError, match="not a mapping"):
        await client.read_native_balances()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_key", [None, ""])
async def test_read_native_balances_rejects_invalid_currency_key(bad_key: object) -> None:
    """A non-aggregate key that is not a non-empty string raises ValueError.

    Given a balance envelope with a valid currency plus a non-aggregate key that
        is either None (not a string) or an empty string (falsy),
    When read_native_balances is called,
    Then it raises ValueError rather than emitting an entry labelled with a
        garbage currency.
    """
    client = _client()

    async def _fake_balance() -> dict[object, Any]:
        return {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            bad_key: {"free": 1.0, "used": 0.0, "total": 1.0},
        }

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    with pytest.raises(ValueError, match="not a mapping"):
        await client.read_native_balances()


@pytest.mark.asyncio
async def test_read_native_balances_empty_returns_empty_list() -> None:
    """An observed-empty balance yields an empty list, never a zero entry.

    Given a balance envelope with only aggregate keys and no currencies,
    When read_native_balances is called,
    Then it returns an empty list rather than a fabricated zero-balance entry.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {"free": {}, "used": {}, "total": {}, "info": {}}

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    assert await client.read_native_balances() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
async def test_read_native_balances_rejects_non_finite(bad_value: float) -> None:
    """A non-finite sub-field is a corrupt envelope and raises ValueError.

    Given a currency whose total is NaN or infinite,
    When read_native_balances is called,
    Then it raises ValueError instead of emitting a poisoned entry.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {"BTC": {"free": 1.0, "used": 0.0, "total": bad_value}}

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    with pytest.raises(ValueError, match="non-finite"):
        await client.read_native_balances()


@pytest.mark.asyncio
async def test_read_native_balances_rejects_missing_subfield() -> None:
    """A missing total/free/used sub-field raises instead of coercing to zero.

    Given a currency whose used sub-field is absent,
    When read_native_balances is called,
    Then it raises ValueError rather than defaulting the missing field to zero.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {"BTC": {"free": 1.0, "total": 1.0}}

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    with pytest.raises(ValueError, match="missing 'used'"):
        await client.read_native_balances()


@pytest.mark.asyncio
async def test_read_native_balances_requires_credentials() -> None:
    """Missing API credentials fail closed before any venue call.

    Given a client with no API key or secret,
    When read_native_balances is called,
    Then it raises RuntimeError without reaching the ccxt fetch path.
    """
    client = _client()
    client.api_key = None
    client.api_secret = None
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client.read_native_balances()


@pytest.mark.asyncio
async def test_read_native_positions_default_raises() -> None:
    """The Spot client inherits the fail-closed native-position default.

    Given a Spot KrakenExchangeClient that declares no position capability
        and does not override ``read_native_positions``,
    When read_native_positions is called,
    Then the base-class default raises NotImplementedError rather than
        returning a fabricated empty position set.
    """
    client = _client()
    with pytest.raises(NotImplementedError):
        await client.read_native_positions()


@pytest.mark.asyncio
async def test_subscribe_ticks_yields_and_respects_exception_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tick subscription yields updates until exception flag.

    Given a KrakenExchangeClient with a queued ticker update,
    When subscribe_ticks is iterated and exception_occur is set,
    Then it yields the update and stops iteration.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    ws = _DummyWs(client._tick_queue)
    client._ws_client = ws
    await client._tick_queue.put(
        TickerUpdate(
            symbol="BTC/USD",
            bid=1.0,
            bid_qty=0.1,
            ask=2.0,
            ask_qty=0.2,
            last=1.5,
            volume=1.0,
            vwap=1.5,
            low=1.0,
            high=2.0,
            change=0.5,
            change_pct=1.0,
        )
    )
    gen = client.subscribe_ticks(["BTC-USD"], req_id=1)
    first = await anext(gen)
    ws.exception_occur = True
    assert isinstance(first, TickerUpdate)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_subscribe_candles_yields_per_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Candle subscription yields updates per interval.

    Given a KrakenExchangeClient with a candle queue for a specific interval,
    When subscribe_candles is iterated,
    Then it yields candle updates from the interval-specific queue.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    interval = 1
    client._candle_queues[interval] = asyncio.Queue[CandleUpdate]()
    ws = _DummyWs(client._candle_queues[interval])
    client._ws_client = ws
    await client._candle_queues[interval].put(
        CandleUpdate(
            symbol="BTC/USD",
            open=1,
            high=2,
            low=0.5,
            close=1.5,
            vwap=1.5,
            trades=1,
            volume=1.0,
            interval_begin=datetime.fromtimestamp(0),
            interval=1,
        )
    )
    gen = client.subscribe_candles(["BTC-USD"], timeframe="1m", req_id=2)
    first = await anext(gen)
    ws.exception_occur = True
    assert isinstance(first, CandleUpdate)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_subscribe_candles_appends_one_entry_per_interval() -> None:
    """Candle subscription cache keeps intervals distinct.

    Given: A Kraken client subscribing to the same symbol with two candle intervals,
    When: Both subscriptions are started,
    Then: The cache contains separate OHLC entries for each interval.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    ws = _DummyWs(asyncio.Queue[object]())
    client._ws_client = ws
    for timeframe, interval in (("1m", 1), ("5m", 5)):
        gen = client.subscribe_candles(["BTC-USD"], timeframe=timeframe)
        await client._candle_queues.setdefault(interval, asyncio.Queue[CandleUpdate]()).put(
            CandleUpdate(
                symbol="BTC/USD",
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                vwap=1.5,
                trades=1,
                volume=1.0,
                interval_begin=datetime.fromtimestamp(0, UTC),
                interval=interval,
            )
        )
        await anext(gen)
        await gen.aclose()
    encoded = {req.parameters_json for req in client._subscription_cache.values()}
    assert any('"interval": 1' in value for value in encoded)
    assert any('"interval": 5' in value for value in encoded)


@pytest.mark.asyncio
async def test_subscribe_dedup_replaces_entry_with_same_key() -> None:
    """Repeated subscriptions replace the same cache key.

    Given: A Kraken client subscribing twice to the same tick channel and symbol,
    When: Both subscriptions run,
    Then: The cache has one ticker entry for that logical subscription.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    ws = _DummyWs(client._tick_queue)
    client._ws_client = ws
    for _ in range(2):
        await client._tick_queue.put(
            TickerUpdate(
                symbol="BTC/USD",
                bid=1.0,
                bid_qty=0.1,
                ask=2.0,
                ask_qty=0.2,
                last=1.5,
                volume=1.0,
                vwap=1.5,
                low=1.0,
                high=2.0,
                change=0.5,
                change_pct=1.0,
            )
        )
        gen = client.subscribe_ticks(["BTC-USD"])
        await anext(gen)
        await gen.aclose()
    ticker_entries = [req for req in client._subscription_cache.values() if req.channel == "ticker"]
    assert len(ticker_entries) == 1


@pytest.mark.asyncio
async def test_replay_iterates_cache_in_insertion_order_with_delay() -> None:
    """Replay resubscribes cached requests in insertion order with delay.

    Given: A Kraken client with two cached subscriptions,
    When: Subscriptions are replayed,
    Then: The websocket receives subscribe calls in insertion order with one
        paced ``_RESUBSCRIBE_CHUNK_DELAY_S`` sleep between them.
    """
    client = _client()
    ws = AsyncMock()
    client._ws_client = ws
    first = SubscriptionRequest(channel="ticker", symbols=("BTC/USD",), parameters_json="{}")
    second = SubscriptionRequest(channel="trade", symbols=("ETH/USD",), parameters_json="{}")
    client._subscription_cache[first.key()] = first
    client._subscription_cache[second.key()] = second
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await client._replay_subscriptions()
    assert ws.subscribe.await_args_list[0].kwargs["params"] == {
        "channel": "ticker",
        "symbol": ["BTC/USD"],
    }
    assert ws.subscribe.await_args_list[1].kwargs["params"] == {
        "channel": "trade",
        "symbol": ["ETH/USD"],
    }
    sleep_mock.assert_awaited_once_with(_RESUBSCRIBE_CHUNK_DELAY_S)


@pytest.mark.asyncio
async def test_replay_requires_connected_ws_client() -> None:
    """Replay fails when no websocket client exists.

    Given: A Kraken client with no websocket client,
    When: Subscriptions are replayed,
    Then: RuntimeError is raised.
    """
    client = _client()
    client._ws_client = None
    with pytest.raises(RuntimeError):
        await client._replay_subscriptions()


def test_connected_ws_client_requires_connected_client() -> None:
    """Connected websocket accessor fails before connection.

    Given: A Kraken client with no websocket client,
    When: _connected_ws_client is called,
    Then: RuntimeError is raised.
    """
    client = _client()
    client._ws_client = None
    with pytest.raises(RuntimeError):
        client._connected_ws_client()


@pytest.mark.asyncio
async def test_ensure_ws_connected_auto_replays_after_reconnect() -> None:
    """Websocket reconnect automatically replays cached subscriptions.

    Given: A Kraken client with a cached subscription and no active websocket,
    When: _ensure_ws_connected creates a new client,
    Then: The replay hook is awaited.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    req = SubscriptionRequest(channel="ticker", symbols=("BTC/USD",), parameters_json="{}")
    client._subscription_cache[req.key()] = req
    with (
        patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient") as ws_cls,
        patch.object(client, "_replay_subscriptions", new_callable=AsyncMock) as replay_mock,
    ):
        ws_cls.return_value.start = AsyncMock()
        await client._ensure_ws_connected()
    replay_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_ws_connected_closes_partial_client_on_failure() -> None:
    """A failed build/replay closes the partial client and re-raises.

    Given: A Kraken client whose subscription replay raises,
    When: _ensure_ws_connected builds a new client,
    Then: The partial client is closed and the error propagates so the
        caller's recovery loop can retry without leaking the client.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    req = SubscriptionRequest(channel="ticker", symbols=("BTC/USD",), parameters_json="{}")
    client._subscription_cache[req.key()] = req
    with (
        patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient") as ws_cls,
        patch.object(client, "_replay_subscriptions", new_callable=AsyncMock) as replay_mock,
        patch.object(client, "_close_ws_client", new_callable=AsyncMock) as close_mock,
    ):
        ws_cls.return_value.start = AsyncMock()
        replay_mock.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            await client._ensure_ws_connected()
    close_mock.assert_awaited_once()
    assert client._ws_connected is False


@pytest.mark.asyncio
async def test_ensure_ws_connected_times_out_on_hung_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connect that never completes is bounded so recovery can retry.

    Given: A SpotWSClient whose start() never returns (the SDK's connect timeout
        never fires — its walrus-reset loop polls the socket forever).
    When: _ensure_ws_connected builds a new client,
    Then: It times out within _WS_CONNECT_TIMEOUT_S, tears the partial client
        down, and raises — so the recovery loop retries with a fresh client
        instead of wedging until a process restart.
    """
    monkeypatch.setattr(kr, "_WS_CONNECT_TIMEOUT_S", 0.05)
    client = KrakenExchangeClient(api_key="key", api_secret="secret")

    async def _hang() -> None:
        await asyncio.Event().wait()

    with (
        patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient") as ws_cls,
        patch.object(client, "_close_ws_client", new_callable=AsyncMock) as close_mock,
    ):
        ws_cls.return_value.start = _hang
        with pytest.raises(TimeoutError):
            await client._ensure_ws_connected()
    close_mock.assert_awaited_once()
    assert client._ws_connected is False


class TestKrakenExchangeClient:
    """Tests for krakenExchangeClient."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def test_init(self) -> None:
        """Verify init."""
        client = KrakenExchangeClient()
        assert client.api_key is None
        assert client.api_secret is None
        assert not client.sandbox

    def test_init_with_credentials(self) -> None:
        """Verify init with credentials."""
        client = KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )
        assert client.api_key == "test_key"
        assert client.api_secret == "test_secret"
        assert not client.sandbox

    async def test_connect(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify connect."""
        with patch.object(kraken_client, "_ccxt_client") as mock_client:
            mock_client.load_markets = AsyncMock(return_value={})
            await kraken_client.connect()
            mock_client.load_markets.assert_called_once()

    async def test_disconnect(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect."""
        await kraken_client.disconnect()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_ticker(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get ticker."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_ticker.return_value = {
                "symbol": "BTC/USD",
                "last": 50000.0,
                "bid": 49999.0,
                "ask": 50001.0,
                "high": 51000.0,
                "low": 49000.0,
                "open": 49500.0,
                "close": 50000.0,
                "baseVolume": 100.0,
                "quoteVolume": 5000000.0,
                "timestamp": 1640995200000,
                "datetime": "2022-01-01T00:00:00.000Z",
            }
            ticker = await kraken_client.get_ticker("BTC-USD")
            assert ticker.symbol == "BTC-USD"
            assert ticker.last == float("50000.0")
            assert ticker.bid == float("49999.0")
            assert ticker.ask == float("50001.0")
            mock_client.fetch_ticker.assert_called_once_with("BTC/USD")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_ohlcv(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get ohlcv."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_ohlcv.return_value = [
                [1640995200000, 49500.0, 51000.0, 49000.0, 50000.0, 100.0],
                [1640998800000, 50000.0, 50500.0, 49800.0, 50200.0, 80.0],
            ]
            ohlcv_list = await kraken_client.get_ohlcv("BTC-USD", "1h", limit=2)
            assert len(ohlcv_list) == 2
            candle1 = ohlcv_list[0]
            assert candle1.open == float("49500.0")
            assert candle1.high == float("51000.0")
            assert candle1.low == float("49000.0")
            assert candle1.close == float("50000.0")
            assert candle1.volume == float("100.0")
            assert candle1.timestamp == pytest.approx(1640995200.0)
            mock_client.fetch_ohlcv.assert_called_once_with("BTC/USD", "1h", None, 2)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_create_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order returns PENDING snapshot built from request data.

        Given: A valid order request and CCXT returning an exchange id,
        When: create_order is called,
        Then: The snapshot has PENDING status, filled=0, remaining=amount, no fee,
              and client_order_id from the request.
        """
        mock_client = AsyncMock()
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=(42, "pub-42"),
            ),
        ):
            mock_client.create_order.return_value = {"id": "test_order_123"}
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == "test_order_123"
            assert order.symbol == "BTC-USD"
            assert order.side == OrderSideEnum.BUY
            assert order.type == ExchangeOrderTypeEnum.MARKET
            assert order.amount == float("0.1")
            assert order.status == ExchangeOrderStatusEnum.PENDING
            assert order.filled == pytest.approx(0.0)
            assert order.remaining == pytest.approx(0.1)
            assert order.fee is None
            assert order.db_order_id == 42
            assert order.db_order_public_id == "pub-42"
            mock_client.create_order.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_cancel_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order calls cancel then fetch_order for full data."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.cancel_order.return_value = {
                "info": {"result": {"count": 1}},
            }
            mock_client.fetch_order.return_value = {
                "id": "test_order_123",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "canceled",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
                "clientOrderId": "cl-123",
            }
            result = await kraken_client.cancel_order("test_order_123", "BTC-USD")
            assert result.id == "test_order_123"
            assert result.status == ExchangeOrderStatusEnum.CANCELED
            assert result.client_order_id == "cl-123"
            mock_client.cancel_order.assert_called_once_with("test_order_123", "BTC/USD")
            mock_client.fetch_order.assert_called_once_with("test_order_123", "BTC/USD")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_balance.return_value = {
                "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
                "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
                "free": {"BTC": 1.5, "USD": 10000.0},
                "used": {"BTC": 0.5, "USD": 2000.0},
                "total": {"BTC": 2.0, "USD": 12000.0},
                "info": {},
            }
            balances = await kraken_client.get_balance()
            assert "BTC" in balances
            assert "USD" in balances
            btc_balance = balances["BTC"]
            assert btc_balance.currency == "BTC"
            assert btc_balance.free == float("1.5")
            assert btc_balance.used == float("0.5")
            assert btc_balance.total == float("2.0")
            usd_balance = balances["USD"]
            assert usd_balance.currency == "USD"
            assert usd_balance.free == float("10000.0")
            mock_client.fetch_balance.assert_called_once()

    async def test_get_balance_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_balance()

    async def test_circuit_breaker_open(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify circuit breaker open."""
        with (
            patch.object(kraken_client, "_circuit_open_until", time.time() + 3600),
            pytest.raises(RuntimeError, match="Circuit breaker open"),
        ):
            await kraken_client.get_ticker("BTC-USD")

    async def test_connect_failure(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify connect failure."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Connection failed")
            with pytest.raises(Exception, match="Connection failed"):
                await kraken_client.connect()

    async def test_disconnect_with_websocket(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect with websocket."""
        mock_ws_client = AsyncMock()
        with (
            patch.object(kraken_client, "_ws_client", mock_ws_client),
            patch.object(kraken_client, "_ws_connected", True),
        ):
            mock_session = MagicMock()
            kraken_client._ccxt_client.session = mock_session
            await kraken_client.disconnect()
            mock_ws_client.close.assert_called_once()
            mock_session.close.assert_called_once()

    async def test_disconnect_exception(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect exception."""
        mock_ws_client = AsyncMock()
        mock_ws_client.close.side_effect = Exception("Close failed")
        with (
            patch.object(kraken_client, "_ws_client", mock_ws_client),
            patch.object(kraken_client, "_ws_connected", True),
        ):
            await kraken_client.disconnect()

    async def test_context_manager(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify context manager."""
        with (
            patch.object(kraken_client, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(kraken_client, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            async with kraken_client as client:
                assert client is kraken_client
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_get_ticker_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ticker error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("TickerSnapshot fetch failed")
            ),
            pytest.raises(Exception, match="TickerSnapshot fetch failed"),
        ):
            await kraken_client.get_ticker("BTC-USD")

    async def test_get_ohlcv_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("OhlcvSnapshot fetch failed")
            ),
            pytest.raises(Exception, match="OhlcvSnapshot fetch failed"),
        ):
            await kraken_client.get_ohlcv("BTC-USD")

    async def test_create_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_create_order_with_price_and_client_id(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order with price and client id.

        Given: A limit order request with price and client_order_id,
        When: create_order is called,
        Then: The snapshot has PENDING status, price and client_order_id from request.
        """
        mock_client = AsyncMock()
        mock_client.create_order.return_value = {"id": "test_order_123"}
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=float("0.1"),
                price=float("50000.0"),
                client_order_id="client_123",
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == "test_order_123"
            assert order.status == ExchangeOrderStatusEnum.PENDING
            assert order.client_order_id == "client_123"
            assert order.price == float("50000.0")
            mock_client.create_order.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_create_order_empty_id_skips_db_logging(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order with empty id skips db logging.

        Given: CCXT returns a response with an empty id string,
        When: create_order is called,
        Then: The snapshot has an empty id and db fields are not set.
        """
        mock_client = AsyncMock()
        mock_log = AsyncMock(return_value=None)
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_log_order_to_db", mock_log),
        ):
            mock_client.create_order.return_value = {"id": ""}
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == ""
            assert order.status == ExchangeOrderStatusEnum.PENDING
            assert order.db_order_id is None
            mock_log.assert_not_called()

    async def test_create_order_none_id_treated_as_empty(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order with None id is treated as empty string.

        Given: CCXT returns a response with id=None,
        When: create_order is called,
        Then: The snapshot has empty id and db logging is skipped.
        """
        mock_client = AsyncMock()
        mock_log = AsyncMock(return_value=None)
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_log_order_to_db", mock_log),
        ):
            mock_client.create_order.return_value = {"id": None}
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == ""
            assert order.status == ExchangeOrderStatusEnum.PENDING
            mock_log.assert_not_called()

    async def test_create_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order error."""
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=Exception("ExchangeOrderSnapshot creation failed"),
            ),
            pytest.raises(Exception, match="ExchangeOrderSnapshot creation failed"),
        ):
            await kraken_client.create_order(order_request)

    async def test_cancel_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.cancel_order("test_order_123")

    async def test_cancel_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order error."""
        with (
            patch.object(kraken_client, "_with_retry", side_effect=Exception("Cancel failed")),
            pytest.raises(Exception, match="Cancel failed"),
        ):
            await kraken_client.cancel_order("test_order_123")

    async def test_get_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_order("test_order_123")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get order."""
        mock_client = AsyncMock()
        mock_client.fetch_order.return_value = {
            "id": "test_order_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "closed",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.1,
            "remaining": 0.0,
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            order = await kraken_client.get_order("test_order_123", "BTC-USD")
            assert order.id == "test_order_123"
            assert order.status == ExchangeOrderStatusEnum.CLOSED
            assert order.filled == float("0.1")
            mock_client.fetch_order.assert_called_once_with("test_order_123", "BTC/USD")

    async def test_get_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order error."""
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=Exception("ExchangeOrderSnapshot fetch failed"),
            ),
            pytest.raises(Exception, match="ExchangeOrderSnapshot fetch failed"),
        ):
            await kraken_client.get_order("test_order_123")

    async def test_get_orders_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_orders()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_orders_open(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders open."""
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            orders = await kraken_client.get_orders(status=ExchangeOrderStatusEnum.OPEN, limit=10)
            assert len(orders) == 1
            assert orders[0].id == "order_1"
            assert orders[0].status == ExchangeOrderStatusEnum.OPEN
            mock_client.fetch_open_orders.assert_called_once_with(None, None, 10)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_orders_all_with_filter(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders all with filter."""
        mock_client = AsyncMock()
        mock_client.fetch_orders.return_value = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "closed",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.1,
                "remaining": 0.0,
            },
            {
                "id": "order_2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "amount": 0.1,
                "price": 51000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            orders = await kraken_client.get_orders(status=ExchangeOrderStatusEnum.CLOSED)
            assert len(orders) == 1
            assert orders[0].id == "order_1"
            assert orders[0].status == ExchangeOrderStatusEnum.CLOSED
            mock_client.fetch_orders.assert_called_once_with(None, None, None)

    async def test_get_orders_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("Orders fetch failed")
            ),
            pytest.raises(Exception, match="Orders fetch failed"),
        ):
            await kraken_client.get_orders()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance_single_currency(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance single currency."""
        mock_client = AsyncMock()
        mock_client.fetch_balance.return_value = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
            "free": {"BTC": 1.5, "USD": 10000.0},
            "used": {"BTC": 0.5, "USD": 2000.0},
            "total": {"BTC": 2.0, "USD": 12000.0},
            "info": {},
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            balances = await kraken_client.get_balance("BTC")
            assert "BTC" in balances
            assert len(balances) == 1
            assert balances["BTC"].currency == "BTC"
            assert balances["BTC"].free == float("1.5")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance_nonexistent_currency(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance nonexistent currency."""
        mock_client = AsyncMock()
        mock_client.fetch_balance.return_value = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "free": {"BTC": 1.5},
            "used": {"BTC": 0.5},
            "total": {"BTC": 2.0},
            "info": {},
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            balances = await kraken_client.get_balance("ETH")
            assert "ETH" in balances
            assert balances["ETH"].currency == "ETH"
            assert balances["ETH"].free == float("0")
            assert balances["ETH"].used == float("0")
            assert balances["ETH"].total == float("0")

    async def test_get_balance_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("AccountBalance fetch failed")
            ),
            pytest.raises(Exception, match="AccountBalance fetch failed"),
        ):
            await kraken_client.get_balance()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = TickerUpdate(
                symbol="BTC-USD",
                bid=50000.0,
                bid_qty=1.0,
                ask=50001.0,
                ask_qty=1.0,
                last=50000.5,
                volume=100.0,
                vwap=50000.0,
                low=49900.0,
                high=50100.0,
                change=100.0,
                change_pct=0.2,
            )

            async def mock_message_generator() -> None:
                await kraken_client._tick_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[TickerUpdate] = []
            async for message in kraken_client.subscribe_ticks(["BTC-USD"]):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].bid == pytest.approx(50000.0)
            assert messages[0].ask == pytest.approx(50001.0)
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_wildcard_seeds_confirmed_universe(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Wildcard ticks seed confirmed health for the resolved universe.

        Given: ``symbols=["*"]`` and a three-symbol market-data universe,
        When: ``subscribe_ticks`` runs the wildcard path,
        Then: the literal ``"*"`` is sent on the wire but a confirmed ticker
            entry is seeded for every wire symbol in the resolved universe
            (the single ``["*"]`` subscribe has no per-symbol ACK, so dark
            wildcard symbols become stale-trackable without false retries),
            and an INFO summary reports the tracked count.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        catalog = ["BTC-USD", "ETH-USD", "XRP-USD"]

        async def _spy_sleep(delay: float) -> None:
            mock_ws_client.exception_occur = True

        sink_id = logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            with (
                caplog.at_level("INFO"),
                patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken."
                    "get_available_kraken_symbols",
                    return_value=catalog,
                ),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken."
                    "native_to_kraken_websocket",
                    side_effect=lambda s: s.replace("-", "/"),
                ),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                    new=_spy_sleep,
                ),
            ):
                kraken_client._ws_client = mock_ws_client
                async for _ in kraken_client.subscribe_ticks(["*"]):
                    break
        finally:
            logger.remove(sink_id)

        snapshot = kraken_client._health_tracker.snapshot()
        seeded_symbols = {sym for (channel, sym) in snapshot if channel == "ticker"}
        assert seeded_symbols == {"BTC/USD", "ETH/USD", "XRP/USD"}
        assert all(
            entry.status == "confirmed"
            for (channel, _sym), entry in snapshot.items()
            if channel == "ticker"
        )
        assert ("ticker", "*") not in snapshot
        params = mock_ws_client.subscribe.await_args_list[0].kwargs["params"]
        assert params["symbol"] == ["*"]
        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any(
            "Subscribed to wildcard ticks on kraken/ticker: "
            "tracking 3 symbol(s) (confirmed; data pending)" in m
            for m in info_messages
        )

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_explicit_seeds_pending_and_logs_summary(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Explicit ticks seed exactly the requested symbols and log coverage.

        Given: an explicit two-symbol tick subscription,
        When: ``subscribe_ticks`` runs the non-wildcard path,
        Then: a pending ticker entry exists for each requested wire symbol
            and an INFO summary reports the explicit tracked count.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False

        async def _spy_sleep(delay: float) -> None:
            mock_ws_client.exception_occur = True

        sink_id = logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            with (
                caplog.at_level("INFO"),
                patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken."
                    "native_to_kraken_websocket",
                    side_effect=lambda s: s.replace("-", "/"),
                ),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                    new=_spy_sleep,
                ),
            ):
                kraken_client._ws_client = mock_ws_client
                async for _ in kraken_client.subscribe_ticks(["BTC-USD", "ETH-USD"]):
                    break
        finally:
            logger.remove(sink_id)

        snapshot = kraken_client._health_tracker.snapshot()
        seeded_symbols = {sym for (channel, sym) in snapshot if channel == "ticker"}
        assert seeded_symbols == {"BTC/USD", "ETH/USD"}
        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any(
            "Subscribed to explicit ticks on kraken/ticker: "
            "tracking 2 symbol(s) (data/ACK pending)" in m
            for m in info_messages
        )

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_trades_logs_coverage_summary(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Trade subscribe emits an N/M coverage summary after subscribing.

        Given: an explicit two-symbol trade subscription,
        When: ``subscribe_trades`` runs,
        Then: an INFO summary reports the tracked symbol count on
            ``kraken/trade``.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False

        async def _spy_sleep(delay: float) -> None:
            mock_ws_client.exception_occur = True

        sink_id = logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            with (
                caplog.at_level("INFO"),
                patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken."
                    "native_to_kraken_websocket",
                    side_effect=lambda s: s.replace("-", "/"),
                ),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                    new=_spy_sleep,
                ),
            ):
                kraken_client._ws_client = mock_ws_client
                async for _ in kraken_client.subscribe_trades(["BTC-USD", "ETH-USD"]):
                    break
        finally:
            logger.remove(sink_id)

        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any(
            "Subscribed to trades on kraken/trade: tracking 2 symbol(s) (data/ACK pending)" in m
            for m in info_messages
        )

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_logs_coverage_summary(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Candle subscribe emits an N/M coverage summary after subscribing.

        Given: an explicit single-symbol 5m candle subscription,
        When: ``subscribe_candles`` runs,
        Then: an INFO summary reports the tracked symbol count on the
            interval channel ``kraken/ohlc:5m``.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False

        async def _spy_sleep(delay: float) -> None:
            mock_ws_client.exception_occur = True

        sink_id = logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            with (
                caplog.at_level("INFO"),
                patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken."
                    "native_to_kraken_websocket",
                    side_effect=lambda s: s.replace("-", "/"),
                ),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                    new=_spy_sleep,
                ),
            ):
                kraken_client._ws_client = mock_ws_client
                async for _ in kraken_client.subscribe_candles(["BTC-USD"], "5m"):
                    break
        finally:
            logger.remove(sink_id)

        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any(
            "Subscribed to 5m candles on kraken/ohlc:5m: "
            "tracking 1 symbol(s) (data/ACK pending)" in m
            for m in info_messages
        )

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = CandleUpdate(
                symbol="BTC-USD",
                open=50000.0,
                high=50100.0,
                low=49900.0,
                close=50050.0,
                vwap=50000.0,
                trades=100,
                volume=10.5,
                interval_begin=datetime.now(UTC),
                interval=5,
            )

            async def mock_message_generator() -> None:
                if 5 not in kraken_client._candle_queues:
                    kraken_client._candle_queues[5] = asyncio.Queue()
                await kraken_client._candle_queues[5].put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[CandleUpdate] = []
            async for message in kraken_client.subscribe_candles(["BTC-USD"], "5m"):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].open == pytest.approx(50000.0)
            assert messages[0].close == pytest.approx(50050.0)
            assert messages[0].interval == 5
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_wildcard_expands_and_chunks(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Wildcard ``["*"]`` on the ohlc channel expands client-side and chunks.

        Given: ``symbols=["*"]`` and a catalog of 250 native symbols,
        When: ``subscribe_candles`` runs,
        Then: ``get_available_kraken_symbols`` is consulted and three
            subscribe calls are issued (100 + 100 + 50 ws symbols each)
            with an inter-chunk sleep between them — Kraken Spot WS v2's
            ohlc channel rejects the literal ``"*"`` exactly like the
            trade channel does (live-confirmed 2026-05-12:
            ``"Currency pair not in ISO 4217-A3 format *"``).
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        catalog = [f"SYM{i:03d}-USD" for i in range(250)]
        sleep_calls: list[float] = []

        async def _spy_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            mock_ws_client.exception_occur = True

        with (
            patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.get_available_kraken_symbols",
                return_value=catalog,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_websocket",
                side_effect=lambda s: s.replace("-", "/"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new=_spy_sleep,
            ),
        ):
            kraken_client._ws_client = mock_ws_client
            async for _ in kraken_client.subscribe_candles(["*"], "1m", req_id=7777):
                break

        assert mock_ws_client.subscribe.await_count == 3
        chunk_sizes = [
            len(call.kwargs["params"]["symbol"])
            for call in mock_ws_client.subscribe.await_args_list
        ]
        assert chunk_sizes == [100, 100, 50]
        assert sleep_calls == [0.1, 0.1]
        req_ids = [call.kwargs.get("req_id") for call in mock_ws_client.subscribe.await_args_list]
        assert req_ids == [7777, None, None]

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_trades(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe trades."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = TradeUpdate(
                symbol="BTC-USD",
                side="buy",
                quantity=0.1,
                price=50000.0,
                ord_type="limit",
                trade_id="12345",
                timestamp=datetime.now(UTC),
            )

            async def mock_message_generator() -> None:
                await kraken_client._trade_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[TradeUpdate] = []
            async for message in kraken_client.subscribe_trades(["BTC-USD"]):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].side == "buy"
            assert messages[0].price == pytest.approx(50000.0)
            assert messages[0].quantity == pytest.approx(0.1)
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_trades_wildcard_expands_and_chunks(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Wildcard ``["*"]`` on the trade channel expands client-side and chunks.

        Given: ``symbols=["*"]`` and a catalog of 250 native symbols,
        When: ``subscribe_trades`` runs,
        Then: ``get_available_kraken_symbols`` is consulted and three
            subscribe calls are issued (100 + 100 + 50 ws symbols each)
            with an inter-chunk sleep between them — Kraken Spot WS v2's
            trade channel rejects the literal ``"*"`` (live-confirmed
            2026-05-12: ``"Currency pair not in ISO 4217-A3 format *"``).
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        catalog = [f"SYM{i:03d}-USD" for i in range(250)]
        sleep_calls: list[float] = []

        async def _spy_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            mock_ws_client.exception_occur = True

        with (
            patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.get_available_kraken_symbols",
                return_value=catalog,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_websocket",
                side_effect=lambda s: s.replace("-", "/"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new=_spy_sleep,
            ),
        ):
            kraken_client._ws_client = mock_ws_client
            async for _ in kraken_client.subscribe_trades(["*"], req_id=4242):
                break

        assert mock_ws_client.subscribe.await_count == 3
        chunk_sizes = [
            len(call.kwargs["params"]["symbol"])
            for call in mock_ws_client.subscribe.await_args_list
        ]
        assert chunk_sizes == [100, 100, 50]
        assert sleep_calls == [0.1, 0.1]
        req_ids = [call.kwargs.get("req_id") for call in mock_ws_client.subscribe.await_args_list]
        assert req_ids == [4242, None, None]

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe executions."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = ExecutionUpdate(
                order_id="ABC",
                exec_type="new",
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                order_type=ExchangeOrderTypeEnum.LIMIT,
                order_status=ExchangeOrderStatusEnum.NEW,
                timestamp=datetime.now(UTC),
                cum_qty=0.0,
                cum_cost=0.0,
            )

            async def mock_message_generator() -> None:
                await kraken_client._execution_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[ExecutionUpdate] = []
            async for message in kraken_client.subscribe_executions(snap_orders=True):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].order_id == "ABC"
            assert messages[0].exec_type == "new"
            assert messages[0].symbol == "BTC-USD"
            mock_ws_client.subscribe.assert_called_once()
            subscribe_kwargs = mock_ws_client.subscribe.call_args.kwargs
            params = subscribe_kwargs["params"]
            assert params["channel"] == "executions"
            assert params["snap_orders"] is True
            assert params["snap_trades"] is False
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_snapshot_defaults_off(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Argument-free subscribe sends explicit false snapshot flags.

        Given: The executor call site invokes subscribe_executions() with no
            arguments,
        When: The subscription params are built,
        Then: snap_orders and snap_trades are sent as explicit False (the
            schema excludes only None) so the venue never replays
            already-accounted executions into the delta-based fill pipeline
            on subscribe or on SDK in-budget reconnects.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.exception_occur = True
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = mock_ws_client
            with (
                patch.object(kraken_client, "_close_ws_client", new_callable=AsyncMock),
                pytest.raises(ConnectionError),
            ):
                async for _ in kraken_client.subscribe_executions():
                    pytest.fail("dead client must not yield")
        params = mock_ws_client.subscribe.call_args.kwargs["params"]
        assert params["snap_orders"] is False
        assert params["snap_trades"] is False

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_raises_and_closes_on_connection_lost(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """A terminal exception_occur raises ConnectionError and resets the slot.

        Given: The SDK's in-client reconnect budget is exhausted
            (exception_occur terminal) while the executions generator runs,
        When: The consume loop observes the flag,
        Then: The generator closes the poisoned client (slot cleared via
            compare-and-clear) and raises ConnectionError so a supervising
            caller can detect death and rebuild — instead of exiting cleanly
            and leaving the dead client poisoning _ensure_ws_connected.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.exception_occur = False
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = mock_ws_client

            async def flip_after_subscribe() -> None:
                mock_ws_client.exception_occur = True

            asyncio.create_task(flip_after_subscribe())
            with pytest.raises(ConnectionError, match="private WS connection lost"):
                async for _ in kraken_client.subscribe_executions():
                    pytest.fail("no message was enqueued")
        mock_ws_client.close.assert_awaited()
        assert kraken_client._ws_client is None

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_hung_subscribe_times_out_and_rebuilds(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """A hung subscribe send is bounded and tears the client down.

        Given: A SpotWSClient whose subscribe() never returns (dead socket
            the SDK has not flagged yet),
        When: The executions generator issues the subscribe,
        Then: The send times out within _SDK_SEND_TIMEOUT_S, the subscribed
            client is closed (slot cleared) so the next attempt rebuilds
            fresh, and TimeoutError propagates so a supervising caller
            observes the death instead of wedging forever.
        """
        mock_ws_client = AsyncMock()
        mock_ws_client.exception_occur = False

        async def hung_subscribe(**_: object) -> None:
            await asyncio.Event().wait()

        mock_ws_client.subscribe = AsyncMock(side_effect=hung_subscribe)
        mock_ws_class.return_value = mock_ws_client
        with (
            patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch.object(kr, "_SDK_SEND_TIMEOUT_S", 0.05),
        ):
            kraken_client._ws_client = mock_ws_client
            with pytest.raises(TimeoutError):
                async for _ in kraken_client.subscribe_executions():
                    pytest.fail("hung subscribe must not yield")
        mock_ws_client.close.assert_awaited_once()
        assert kraken_client._ws_client is None

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_hung_subscribe_skips_close_when_replaced(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """A subscribe timeout never closes a slot another path replaced.

        Given: A hung subscribe during which a concurrent path installs a
            different client into the slot,
        When: The send times out,
        Then: TimeoutError still propagates but the replacement client is
            left untouched (compare-and-clear discipline).
        """
        subscribed = AsyncMock()
        subscribed.exception_occur = False
        replacement = AsyncMock()
        replacement.exception_occur = False

        async def hung_subscribe(**_: object) -> None:
            kraken_client._ws_client = replacement
            await asyncio.Event().wait()

        subscribed.subscribe = AsyncMock(side_effect=hung_subscribe)
        mock_ws_class.return_value = subscribed
        with (
            patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch.object(kr, "_SDK_SEND_TIMEOUT_S", 0.05),
        ):
            kraken_client._ws_client = subscribed
            with pytest.raises(TimeoutError):
                async for _ in kraken_client.subscribe_executions():
                    pytest.fail("hung subscribe must not yield")
        subscribed.close.assert_not_awaited()
        replacement.close.assert_not_awaited()
        assert kraken_client._ws_client is replacement

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_death_skips_close_when_slot_replaced(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Death cleanup never touches a slot another path already replaced.

        Given: The subscribed client dies (exception_occur terminal) while a
            concurrent path has already installed a different client in the
            slot,
        When: The generator's death cleanup runs,
        Then: ConnectionError still propagates but the replacement client is
            left untouched (no close, slot preserved).
        """
        subscribed = AsyncMock()
        subscribed.exception_occur = False
        replacement = AsyncMock()
        replacement.exception_occur = False
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = subscribed

            async def swap_slot_then_kill() -> None:
                kraken_client._ws_client = replacement
                subscribed.exception_occur = True

            asyncio.create_task(swap_slot_then_kill())
            with pytest.raises(ConnectionError, match="private WS connection lost"):
                async for _ in kraken_client.subscribe_executions():
                    pytest.fail("no message was enqueued")
        subscribed.close.assert_not_awaited()
        replacement.close.assert_not_awaited()
        assert kraken_client._ws_client is replacement

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions_inner_break_exits_cleanly(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """A receive-error break ends the generator without ConnectionError.

        Given: The execution queue raises a non-timeout error while the
            client is still healthy (exception_occur False),
        When: The consume loop breaks out,
        Then: The generator finishes cleanly (supervision treats clean
            return as death too) and the healthy client stays in the slot.
        """
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.exception_occur = False
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = mock_ws_client
            with patch.object(
                kraken_client._execution_queue,
                "get",
                side_effect=RuntimeError("queue torn down"),
            ):
                collected = [m async for m in kraken_client.subscribe_executions()]
        assert collected == []
        mock_ws_client.close.assert_not_awaited()
        assert kraken_client._ws_client is mock_ws_client

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_ensure_ws_connected_rebuilds_poisoned_slot(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """A dead client in the slot is closed and replaced under the lock.

        Given: _ws_client holds a client whose exception_occur flag is
            terminal and no generator finalizer ran for it,
        When: _ensure_ws_connected is called,
        Then: The dead client is closed and a fresh client is constructed
            and started, instead of the existence guard treating the dead
            client as connected.
        """
        poisoned = AsyncMock()
        poisoned.exception_occur = True
        kraken_client._ws_client = poisoned
        fresh = AsyncMock()
        fresh.exception_occur = False
        mock_ws_class.return_value = fresh
        await kraken_client._ensure_ws_connected()
        poisoned.close.assert_awaited_once()
        fresh.start.assert_awaited_once()
        assert kraken_client._ws_client is fresh

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket connection failed"),
            ),
            pytest.raises(Exception, match="WebSocket connection failed"),
        ):
            async for _ in kraken_client.subscribe_ticks(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_ensure_ws_connected(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        with patch.object(kraken_client, "_ensure_ws_connected") as mock_ensure:
            mock_ensure.return_value = None
            await mock_ensure()
            mock_ensure.assert_called_once()


class TestKrakenCoverageImprovement:
    """Tests for krakenCoverageImprovement."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def test_init_with_all_parameters(self) -> None:
        """Verify init with all parameters."""
        client = KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
            enable_rate_limit=False,
        )
        assert client.api_key == "test_key"
        assert client.api_secret == "test_secret"
        assert not client.sandbox

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_connect_success_resets_circuit_breaker(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify connect success resets circuit breaker."""
        mock_client = AsyncMock()
        mock_client.load_markets.return_value = {}
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            await kraken_client.connect()
            mock_client.load_markets.assert_called_once()

    async def test_connect_failure_raises_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify connect failure raises exception.

        The mock session is synchronous (MagicMock): the connect-failure
        cleanup calls ``session.close()`` without awaiting, and an
        AsyncMock attribute there would leave an un-awaited coroutine.
        """
        mock_client = AsyncMock(session=MagicMock())
        mock_client.load_markets.side_effect = Exception("Connection failed")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_with_retry") as mock_retry,
        ):
            mock_retry.side_effect = Exception("Connection failed")
            with pytest.raises(Exception, match="Connection failed"):
                await kraken_client.connect()
        mock_client.session.close.assert_called_once_with()

    async def test_disconnect_no_websocket(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect no websocket."""
        await kraken_client.disconnect()

    async def test_disconnect_no_session(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect no session."""
        mock_client = MagicMock()
        if hasattr(mock_client, "session"):
            delattr(mock_client, "session")
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            await kraken_client.disconnect()

    async def test_disconnect_closes_rest_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect closes rest session."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.session = mock_session
        with (
            patch.object(
                kraken_client, "_close_ws_client", new_callable=AsyncMock
            ) as mock_close_ws_client,
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.logger.info"
            ) as mock_info,
        ):
            await kraken_client.disconnect()
        mock_close_ws_client.assert_awaited_once()
        mock_session.close.assert_called_once()
        mock_info.assert_called_with("Kraken REST client session closed")

    async def test_disconnect_logs_warning_on_error(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect logs warning on error."""
        mock_client = MagicMock()
        mock_client.session = MagicMock()
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(
                kraken_client, "_close_ws_client", new_callable=AsyncMock
            ) as mock_close_ws_client,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.logger.warning"
            ) as mock_warning,
        ):
            mock_close_ws_client.side_effect = RuntimeError("boom")
            await kraken_client.disconnect()
        mock_warning.assert_called_once()
        warning_args = mock_warning.call_args[0]
        assert warning_args
        assert "boom" in warning_args[0]
        mock_client.session.close.assert_not_called()

    async def test_context_manager_entry_exit(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify context manager entry exit."""
        with (
            patch.object(kraken_client, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(kraken_client, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            async with kraken_client as client:
                assert client is kraken_client
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_get_ticker_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ticker exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("TickerSnapshot error")
            with pytest.raises(Exception, match="TickerSnapshot error"):
                await kraken_client.get_ticker("BTC-USD")

    async def test_get_ohlcv_with_all_parameters(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv with all parameters."""
        mock_return_value = [
            [1640995200000, 49500.0, 51000.0, 49000.0, 50000.0, 100.0],
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_return_value
            result = await kraken_client.get_ohlcv("BTC-USD", "1h", since=1640995200, limit=1)
            assert len(result) == 1
            mock_retry.assert_called_once()

    async def test_get_ohlcv_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("OhlcvSnapshot error")
            with pytest.raises(Exception, match="OhlcvSnapshot error"):
                await kraken_client.get_ohlcv("BTC-USD")

    async def test_create_order_no_api_key(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no api key."""
        kraken_client.api_key = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    async def test_create_order_no_api_secret(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no api secret."""
        kraken_client.api_secret = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    async def test_create_order_with_all_fields(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order with all fields returns PENDING snapshot from request.

        Given: A limit order request with all fields including client_order_id,
        When: create_order is called,
        Then: The snapshot has PENDING status and fields from the request.
        """
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = {"id": "order_123"}
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=float("0.1"),
                price=float("50000.0"),
                client_order_id="client_order_123",
            )
            result = await kraken_client.create_order(order_request)
            assert result.status == ExchangeOrderStatusEnum.PENDING
            assert result.client_order_id == "client_order_123"
            assert result.price == float("50000.0")

    async def test_create_order_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("ExchangeOrderSnapshot creation failed")
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            with pytest.raises(Exception, match="ExchangeOrderSnapshot creation failed"):
                await kraken_client.create_order(order_request)

    async def test_cancel_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.cancel_order("order_123")

    async def test_cancel_order_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Cancel failed")
            with pytest.raises(Exception, match="Cancel failed"):
                await kraken_client.cancel_order("order_123", "BTC-USD")

    async def test_get_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_order("order_123")

    async def test_get_order_success(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order success."""
        mock_order_data = {
            "id": "order_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "closed",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.1,
            "remaining": 0.0,
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_order_data
            result = await kraken_client.get_order("order_123", "BTC-USD")
            assert result.id == "order_123"
            assert result.status == ExchangeOrderStatusEnum.CLOSED

    async def test_get_order_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("ExchangeOrderSnapshot fetch failed")
            with pytest.raises(Exception, match="ExchangeOrderSnapshot fetch failed"):
                await kraken_client.get_order("order_123")

    async def test_get_orders_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_orders()

    async def test_get_orders_open_status(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders open status."""
        mock_orders_data = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            }
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_orders_data
            result = await kraken_client.get_orders(
                symbol="BTC-USD", status=ExchangeOrderStatusEnum.OPEN, limit=10
            )
            assert len(result) == 1
            assert result[0].status == ExchangeOrderStatusEnum.OPEN

    async def test_get_orders_all_with_status_filter(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders all with status filter."""
        mock_orders_data = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "closed",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.1,
                "remaining": 0.0,
            },
            {
                "id": "order_2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "amount": 0.1,
                "price": 51000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_orders_data
            result = await kraken_client.get_orders(status=ExchangeOrderStatusEnum.CLOSED)
            assert len(result) == 1
            assert result[0].status == ExchangeOrderStatusEnum.CLOSED

    async def test_get_orders_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Orders fetch failed")
            with pytest.raises(Exception, match="Orders fetch failed"):
                await kraken_client.get_orders()

    async def test_get_balance_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_balance()

    async def test_get_balance_single_currency(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance single currency."""
        mock_balance_data = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
            "free": {"BTC": 1.5, "USD": 10000.0},
            "used": {"BTC": 0.5, "USD": 2000.0},
            "total": {"BTC": 2.0, "USD": 12000.0},
            "info": {},
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_balance_data
            result = await kraken_client.get_balance("BTC")
            assert "BTC" in result
            assert len(result) == 1
            assert result["BTC"].currency == "BTC"
            assert result["BTC"].free == float("1.5")

    async def test_get_balance_nonexistent_currency(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance nonexistent currency."""
        mock_balance_data = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "free": {"BTC": 1.5},
            "used": {"BTC": 0.5},
            "total": {"BTC": 2.0},
            "info": {},
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_balance_data
            result = await kraken_client.get_balance("ETH")
            assert "ETH" in result
            assert result["ETH"].currency == "ETH"
            assert result["ETH"].free == float("0")
            assert result["ETH"].used == float("0")
            assert result["ETH"].total == float("0")

    async def test_get_balance_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("AccountBalance fetch failed")
            with pytest.raises(Exception, match="AccountBalance fetch failed"):
                await kraken_client.get_balance()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket error"),
            ),
            pytest.raises(Exception, match="WebSocket error"),
        ):
            async for _ in kraken_client.subscribe_ticks(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket error"),
            ),
            pytest.raises(Exception, match="WebSocket error"),
        ):
            async for _ in kraken_client.subscribe_candles(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_different_timeframes(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles different timeframes."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = True
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = mock_ws_client
            timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
            expected_intervals = [1, 5, 15, 30, 60, 240, 1440]
            for timeframe, expected_interval in zip(timeframes, expected_intervals, strict=False):
                try:
                    async for _ in kraken_client.subscribe_candles(["BTC-USD"], timeframe):
                        break
                except StopAsyncIteration:
                    pass
                mock_ws_client.subscribe.assert_called_with(
                    params={
                        "channel": "ohlc",
                        "symbol": ["BTC/USD"],
                        "interval": expected_interval,
                        "snapshot": True,
                    },
                    req_id=None,
                )
            mock_ws_client.reset_mock()

    async def test_close_ws_client_closes_resources(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify a successful close defers session ownership to the SDK.

        Given: A WebSocket client whose ``close()`` completes in bound (the
            real SDK closes its own aiohttp session inside ``close()``),
        When: ``_close_ws_client`` runs,
        Then: the venue layer does NOT double-close the session and the
            client slot is cleared.
        """
        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()
        mock_session = AsyncMock()
        mock_session.closed = False
        cast(Any, mock_ws)._SpotAsyncClient__session = mock_session
        kraken_client._ws_client = mock_ws
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_ws.close.assert_awaited_once()
        mock_session.close.assert_not_awaited()
        assert kraken_client._ws_client is None

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_ensure_ws_connected_initializes_client(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify ensure ws connected initializes client."""
        mock_ws_instance = AsyncMock()
        mock_ws_instance.start = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance
        await kraken_client._ensure_ws_connected()
        mock_ws_class.assert_called_once()
        mock_ws_instance.start.assert_awaited_once()
        assert kraken_client._ws_client is mock_ws_instance
        assert kraken_client._ws_connected is True

    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt")
    async def test_create_order_fallback_to_native_api(
        self,
        mock_native_to_ccxt: MagicMock,
        mock_native_to_rest: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify create order fallback to native api."""
        mock_native_to_ccxt.return_value = "AAPL/USD"
        mock_native_to_rest.return_value = "AAPLx/USD"
        order_request = ExchangeOrderRequest(
            symbol="AAPL-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=float("1"),
            price=float("150"),
            client_order_id="client-1",
        )
        trade_client = MagicMock()
        trade_client.create_order.return_value = {"txid": ["ORDER123"]}
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=ValueError("Unknown native symbol"),
            ),
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=(42, "pub-42"),
            ),
        ):
            result = await kraken_client.create_order(order_request)
        trade_client.create_order.assert_called_once()
        kwargs = trade_client.create_order.call_args.kwargs
        assert kwargs["pair"] == "AAPLx/USD"
        assert kwargs["extra_params"] == {
            "cl_ord_id": "client-1",
            "asset_class": "tokenized_asset",
        }
        assert result.id == "ORDER123"
        assert result.symbol == "AAPL-USD"
        assert result.db_order_id == 42
        assert result.db_order_public_id == "pub-42"

    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt")
    async def test_cancel_order_fallback_to_native_api(
        self,
        mock_native_to_ccxt: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify cancel order fallback to native api."""
        mock_native_to_ccxt.return_value = "AAPL/USD"
        trade_client = MagicMock()
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=ValueError("Unknown native symbol"),
            ),
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
        ):
            result = await kraken_client.cancel_order("ABC123", "AAPL-USD")
        trade_client.cancel_order.assert_called_once_with(txid="ABC123")
        assert result.id == "ABC123"
        assert result.status == ExchangeOrderStatusEnum.CANCELED


class TestCloseWsClientBranches:
    """Tests for closeWsClientBranches."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_close_ws_client_when_connected(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client when connected."""
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_ws_client.close.assert_awaited_once()
        assert kraken_client._ws_connected is False
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_timeout_force_closes_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A bounded-close timeout force-closes the leaked session (#143).

        Given: A WebSocket client whose ``close()`` hangs past the bound
            (the connector stuck in a reconnect backoff) and an open
            aiohttp session,
        When: ``_close_ws_client`` runs with a tiny close bound,
        Then: ``force_close_ws_client`` closes the session directly — the
            pre-fix behaviour abandoned the client and leaked one session
            per rebuild cycle.
        """

        async def _hang() -> None:
            await asyncio.sleep(999)

        class _StubSession:
            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        stub_session = _StubSession()
        stub_ws = SimpleNamespace(close=_hang, _SpotAsyncClient__session=stub_session)
        kraken_client._ws_client = cast(Any, stub_ws)
        kraken_client._ws_connected = True
        kraken_client._WS_CLOSE_TIMEOUT_SECONDS = 0.05
        await kraken_client._close_ws_client()
        assert stub_session.closed is True
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_session_already_closed(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client session already closed."""
        mock_session = MagicMock()
        mock_session.closed = True
        mock_session.close = AsyncMock()
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        mock_ws_client._SpotAsyncClient__session = mock_session
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_session.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_ws_client_closes_when_flag_false(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Close tears down an orphaned client even when the flag is False.

        Given: A client object present but ``_ws_connected`` left False
            (as happens when a recovery attempt was cancelled mid-replay),
        When: ``_close_ws_client`` runs,
        Then: ``close()`` is still awaited so the SDK client and its
            background task/session are torn down instead of leaked.
        """
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._close_ws_client()
        mock_ws_client.close.assert_awaited_once()
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_no_session_attribute(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client no session attribute."""
        mock_ws_client = MagicMock(spec=[])
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._close_ws_client()
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_timeout(self, kraken_client: KrakenExchangeClient) -> None:
        """WebSocket close falls back to forced cleanup on timeout.

        Given a WebSocket client whose close method hangs,
        When _close_ws_client is called,
        Then it times out, logs a warning, and sets client to None.
        """

        async def _hang() -> None:
            await asyncio.sleep(999)

        mock_ws_client = MagicMock()
        mock_ws_client.close = _hang
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        kraken_client._WS_CLOSE_TIMEOUT_SECONDS = 0.05
        await kraken_client._close_ws_client()
        assert kraken_client._ws_client is None
        assert kraken_client._ws_connected is False

    @pytest.mark.asyncio
    async def test_close_ws_client_swallows_close_error(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A non-timeout close error is swallowed, not propagated.

        Given: A WebSocket client whose close raises a non-timeout error,
        When: _close_ws_client runs (e.g. from the failure-cleanup path),
        Then: The error is swallowed and the client is torn down so it
            cannot mask the original connection failure being re-raised.
        """
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock(side_effect=RuntimeError("boom"))
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        assert kraken_client._ws_client is None
        assert kraken_client._ws_connected is False


class TestExecutionsSubscriptionCredentials:
    """Tests for executionsSubscriptionCredentials."""

    @pytest.fixture
    def kraken_client_no_creds(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key=None,
            api_secret=None,
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_subscribe_executions_requires_credentials(
        self, kraken_client_no_creds: KrakenExchangeClient
    ) -> None:
        """Verify subscribe executions requires credentials."""
        with pytest.raises(RuntimeError, match="API credentials required"):
            async for _ in kraken_client_no_creds.subscribe_executions():
                """Consumed by iteration to trigger exception."""
                pass


class TestWebsocketTimeoutPaths:
    """Tests for websocketTimeoutPaths."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_subscribe_ticks_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_ticks(["BTC-USD"])
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_candles_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_candles(["BTC-USD"], timeframe="1m")
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_trades_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe trades timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_trades(["BTC-USD"])
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_executions_timeout_then_dead_client_raises(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A queue timeout followed by a dead client raises ConnectionError.

        Given: The drain wait times out while the SDK flips exception_occur
            to terminal,
        When: The consume loop re-checks the flag,
        Then: The generator closes the poisoned client (slot cleared) and
            raises ConnectionError instead of ending cleanly — death must be
            observable to a supervising caller.
        """
        ws = MagicMock()
        ws.subscribe = AsyncMock()
        ws.close = AsyncMock()
        ws.exception_occur = False
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_executions()
            with pytest.raises(ConnectionError, match="private WS connection lost"):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()
        ws.close.assert_awaited_once()
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_instruments_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe instruments timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client._subscribe_instruments_impl(req_id=1, raw=False)
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()


class TestTickerSubscriptionAck:
    """Tests for tickerSubscriptionAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_ticker_ack_status_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Do not exercise non-OK ticker ack handling in this no-op body."""
        pass

    @pytest.mark.asyncio
    async def test_process_ticker_data_with_dict(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify process ticker data with dict."""
        message = {
            "channel": "ticker",
            "data": {
                "symbol": "XBT/USD",
                "bid": 50000.0,
                "ask": 50001.0,
                "last": 50000.5,
                "volume": 100.0,
            },
        }
        await kraken_client._on_message(message)


class TestInstrumentSubscriptionAck:
    """Tests for instrumentSubscriptionAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_instrument_data_with_pairs(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify process instrument data with pairs."""
        message = {
            "channel": "instrument",
            "data": {
                "pairs": [
                    {
                        "symbol": "XBT/USD",
                        "base": "XBT",
                        "quote": "USD",
                        "status": "online",
                        "qty_precision": 8,
                        "qty_increment": 0.00000001,
                        "price_precision": 1,
                        "price_increment": 0.1,
                        "cost_precision": 5,
                        "cost_min": 0.5,
                        "margin_initial": 0.2,
                        "position_limit_long": 100,
                        "position_limit_short": 100,
                    }
                ]
            },
        }
        await kraken_client._on_message(message)


class TestInstrumentDataProcessingError:
    """Tests for instrumentDataProcessingError."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_instrument_data_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify process instrument data exception."""
        message = {
            "channel": "instrument",
            "data": {"pairs": [{"symbol": "XBT/USD", "status": "online"}]},
        }
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.KrakenInstrumentPairSchema.model_validate",
                side_effect=Exception("Unexpected processing error"),
            ),
            patch("snapper.infrastructure.exchanges.implementations.kraken.logger") as mock_logger,
        ):
            await kraken_client._on_message(message)
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args
            assert "Failed to process instrument data" in str(call_args)


class TestRetryMechanismSyncFunction:
    """Tests for retryMechanismSyncFunction."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_sync_function(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify with retry sync function."""

        def sync_function(x: int, y: int) -> int:
            return x + y

        result = await kraken_client._with_retry(sync_function, 2, 3)
        assert result == 5

    @pytest.mark.asyncio
    async def test_with_retry_async_function(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify with retry async function."""

        async def async_function(x: int) -> int:
            return x * 2

        result = await kraken_client._with_retry(async_function, 5)
        assert result == 10


class TestOhlcSubscribeAck:
    """Tests for ohlcSubscribeAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)

    @pytest.mark.asyncio
    async def test_ohlc_ack_with_warning(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify ohlc ack with warning."""
        message = {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "XBT/USD", "interval": 60},
            "success": False,
            "error": "subscription failed",
        }
        ack = MagicMock()
        ack.success = False
        ack.result = MagicMock(symbol="XBT/USD", interval=60, warnings=["w1"])
        ack.error = "subscription failed"
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.KrakenOhlcSubscriptionAckSchema.model_validate",
                return_value=ack,
            ),
            patch("snapper.infrastructure.exchanges.implementations.kraken.logger") as mock_logger,
        ):
            await kraken_client._on_message(message)
            assert mock_logger.warning.called


class TestEnsureWsConnectedInitialization:
    """Tests for ensureWsConnectedInitialization."""

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_creates_client(self) -> None:
        """Verify ensure ws connected creates client."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as mock_client_class:
            mock_ws_client = AsyncMock()
            mock_client_class.return_value = mock_ws_client
            kraken_client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
            kraken_client._ws_client = None
            await kraken_client._ensure_ws_connected()
            mock_client_class.assert_called_once()
            mock_ws_client.start.assert_awaited_once()
            assert kraken_client._ws_connected is True


class TestOnMessageDataRouting:
    """Tests for onMessageDataRouting."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance with real bounded queues."""
        return KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)

    @pytest.mark.asyncio
    async def test_on_message_routes_ticker_list(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message routes ticker list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "ticker", "data": [{}]})
        assert kraken_client._tick_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_on_message_routes_trade_list(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message routes trade list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "trade", "data": [{}]})
        assert kraken_client._trade_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_on_message_routes_execution_list(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message routes execution list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_execution_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "executions", "data": [{}]})
        assert kraken_client._execution_queue.qsize() == 1


class TestRetryMaxRetriesExceeded:
    """Tests for retryMaxRetriesExceeded."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_max_retries_exceeded(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry max retries exceeded."""
        call_count = 0

        async def failing_function() -> None:
            nonlocal call_count
            call_count += 1
            raise ccxt.RateLimitExceeded("Rate limit exceeded")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(ccxt.RateLimitExceeded),
        ):
            await kraken_client._with_retry(failing_function)
        assert call_count == 3


class TestCreateOrderNetworkRetryExclusion:
    """Tests for the create_order double-place guard.

    Order creation is the one non-idempotent venue mutation on the spot
    client: a network-class failure after send is ambiguous (the order
    may have executed), Kraken dedupes cl_ord_id only among OPEN orders,
    and all strategy orders are MARKET. These tests pin the contract
    that create_order is never blindly re-sent on network errors while
    rate-limit retries (definitive venue-side 429 rejection) and the
    default retry behavior of idempotent calls remain intact.
    """

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_create_order_request_timeout_is_not_retried(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order is sent exactly once on request timeout.

        Given: CCXT create_order raises RequestTimeout (ambiguous — the
            order may have reached the venue and executed),
        When: create_order is called,
        Then: The venue call is made exactly once, the control flag is
            consumed by _with_retry rather than forwarded to the venue
            call, and the ambiguous failure surfaces as
            AmbiguousOrderSubmitError carrying the original timeout as
            __cause__ plus the submit identity.
        """
        mock_client = AsyncMock()
        mock_client.create_order.side_effect = ccxt.RequestTimeout("request timed out")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(AmbiguousOrderSubmitError) as exc_info,
        ):
            await kraken_client.create_order(
                ExchangeOrderRequest(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.MARKET,
                    amount=float("0.1"),
                    client_order_id="client_p02",
                )
            )
        mock_client.create_order.assert_called_once()
        assert "retry_network_errors" not in mock_client.create_order.call_args.kwargs
        assert isinstance(exc_info.value.__cause__, ccxt.RequestTimeout)
        assert exc_info.value.client_order_id == "client_p02"
        assert exc_info.value.instrument == "BTC-USD"

    @pytest.mark.asyncio
    async def test_unretried_network_failure_feeds_circuit_breaker(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify unretried network failures still trip the breaker.

        Given: The breaker is one failure away from its threshold,
        When: create_order fails with a network error that is not
            retried,
        Then: The breaker opens so subsequent calls fail fast instead of
            hammering the venue once per command during an outage.
        """
        mock_client = AsyncMock()
        mock_client.create_order.side_effect = ccxt.NetworkError("connection reset")
        kraken_client._circuit_failures = kraken_client._max_failures - 1
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            pytest.raises(AmbiguousOrderSubmitError),
        ):
            await kraken_client.create_order(
                ExchangeOrderRequest(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.MARKET,
                    amount=float("0.1"),
                )
            )
        assert kraken_client._circuit_failures == kraken_client._max_failures
        assert kraken_client._circuit_open_until > time.time()
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await kraken_client._with_retry(mock_client.create_order)

    @pytest.mark.asyncio
    async def test_create_order_still_retries_rate_limit(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify rate-limit retries survive the network-retry exclusion.

        Given: CCXT create_order raises RateLimitExceeded once (a 429 is
            a definitive venue-side rejection — re-sending cannot
            duplicate) and then succeeds,
        When: create_order is called,
        Then: The call is retried and the order snapshot is returned.
        """
        mock_client = AsyncMock()
        mock_client.create_order.side_effect = [
            ccxt.RateLimitExceeded("rate limited"),
            {"id": "order_after_429"},
        ]
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_log_order_to_db", AsyncMock(return_value=None)),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
        ):
            order = await kraken_client.create_order(
                ExchangeOrderRequest(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.MARKET,
                    amount=float("0.1"),
                )
            )
        assert order.id == "order_after_429"
        assert mock_client.create_order.call_count == 2

    @pytest.mark.asyncio
    async def test_with_retry_default_still_retries_network_errors(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify idempotent calls keep the default network retry.

        Given: A function raising NetworkError on every attempt,
        When: _with_retry runs without the exclusion flag,
        Then: All three attempts are made before the error propagates.
        """
        call_count = 0

        async def failing_function() -> None:
            nonlocal call_count
            call_count += 1
            raise ccxt.NetworkError("transient blip")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(ccxt.NetworkError),
        ):
            await kraken_client._with_retry(failing_function)
        assert call_count == 3


class TestAmbiguousSubmitClassification:
    """Exception taxonomy pins for spot create_order.

    Only genuinely ambiguous failures (request possibly executed) may
    wrap into AmbiguousOrderSubmitError; definitive venue answers and
    provably-not-sent failures must keep their native types so the
    executor's reject path still handles them.
    """

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def _request(self) -> ExchangeOrderRequest:
        """Build a market order request."""
        return ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=float("0.1"),
            client_order_id="client_tax",
        )

    @pytest.mark.asyncio
    async def test_ccxt_pool_dispatch_failure_is_wrapped(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A REST-pool dispatch failure on the ccxt path wraps as ambiguous.

        Given: The bounded-pool dispatch raising RestPoolDispatchError
            beneath _with_retry/_invoke_func (the submit work item may
            already sit in the executor queue and could still run on a
            freed worker),
        When: create_order is called,
        Then: AmbiguousOrderSubmitError surfaces with the dispatch error
            chained and the submit identity attached — never a definitive
            reject for a possibly-live order.
        """
        mock_client = MagicMock()

        async def failing_dispatch(func: Any, /, *args: Any, **kwargs: Any) -> Any:
            raise RestPoolDispatchError("can't start new thread")

        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_dispatch_blocking", failing_dispatch),
            pytest.raises(AmbiguousOrderSubmitError) as exc_info,
        ):
            await kraken_client.create_order(self._request())
        assert isinstance(exc_info.value.__cause__, RestPoolDispatchError)
        assert exc_info.value.client_order_id == "client_tax"

    @pytest.mark.asyncio
    async def test_native_pool_dispatch_failure_is_wrapped(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A REST-pool dispatch failure on the native path wraps as ambiguous.

        Given: The bounded-pool dispatch raising RestPoolDispatchError
            beneath the native Trade API send,
        When: _create_order_via_native is called,
        Then: AmbiguousOrderSubmitError surfaces with the dispatch error
            chained and the original venue call never re-executed.
        """
        trade_client = MagicMock()

        async def failing_dispatch(func: Any, /, *args: Any, **kwargs: Any) -> Any:
            raise RestPoolDispatchError("can't start new thread")

        with (
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
            patch.object(kraken_client, "_dispatch_blocking", failing_dispatch),
            pytest.raises(AmbiguousOrderSubmitError) as exc_info,
        ):
            await kraken_client._create_order_via_native(self._request())
        assert isinstance(exc_info.value.__cause__, RestPoolDispatchError)
        trade_client.create_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_exhausted_rate_limit_is_not_wrapped(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """An exhausted 429 keeps its native type despite subclassing NetworkError.

        Given: CCXT create_order raising RateLimitExceeded on every
            attempt (a definitive venue-side rejection — the order was
            never placed),
        When: create_order exhausts the rate-limit retries,
        Then: RateLimitExceeded propagates plain, never as ambiguous.
        """
        mock_client = AsyncMock()
        mock_client.create_order.side_effect = ccxt.RateLimitExceeded("rate limited")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(ccxt.RateLimitExceeded),
        ):
            await kraken_client.create_order(self._request())

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_is_not_wrapped(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A pre-send breaker rejection keeps its native RuntimeError type.

        Given: The REST circuit breaker open (raised BEFORE any send —
            the order provably never left the process),
        When: create_order is called,
        Then: The plain RuntimeError propagates (safe to reject).
        """
        kraken_client._circuit_open_until = time.time() + 3600
        mock_client = AsyncMock()
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            pytest.raises(RuntimeError, match="Circuit breaker open"),
        ):
            await kraken_client.create_order(self._request())
        mock_client.create_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_exchange_error_is_not_wrapped(self, kraken_client: KrakenExchangeClient) -> None:
        """A definitive venue rejection keeps its native ExchangeError type.

        Given: CCXT create_order raising InsufficientFunds (the venue
            answered authoritatively — no order was placed),
        When: create_order is called,
        Then: The ExchangeError propagates plain.
        """
        mock_client = AsyncMock()
        mock_client.create_order.side_effect = ccxt.InsufficientFunds("no funds")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            pytest.raises(ccxt.InsufficientFunds),
        ):
            await kraken_client.create_order(self._request())

    @pytest.mark.asyncio
    async def test_native_fallback_transport_failure_is_wrapped(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """The native Trade API path wraps transport failures as ambiguous.

        Given: A native-only symbol whose Trade API send raises a
            requests transport error (the order may have reached
            Kraken),
        When: _create_order_via_native is called,
        Then: AmbiguousOrderSubmitError surfaces with the original
            error chained.
        """
        trade_client = MagicMock()
        trade_client.create_order.side_effect = requests.exceptions.ConnectionError(
            "reset after send"
        )
        with (
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
            pytest.raises(AmbiguousOrderSubmitError) as exc_info,
        ):
            await kraken_client._create_order_via_native(self._request())
        assert isinstance(exc_info.value.__cause__, requests.exceptions.ConnectionError)


class TestStatusBranchInSubscriptions:
    """Tests for statusBranchInSubscriptions."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_with_subscribe_method_and_result(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message with subscribe method and result."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "trade",
                "symbol": "XBT/USD",
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_list_type_skips_req_id_branch(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message list type skips req id branch."""
        message: list[dict[str, str]] = [{"channel": "test", "data": "value"}]
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_dict_without_req_id(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message dict without req id."""
        message = {"channel": "heartbeat", "data": {}}
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_subscribe_ohlc_channel(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message subscribe ohlc channel."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "XBT/USD",
                "interval": 60,
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_subscribe_ohlc_failed(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message subscribe ohlc failed."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "XBT/USD",
                "interval": 60,
            },
            "success": False,
            "error": "subscription failed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ticker_channel_ack(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ticker channel ack."""
        message = {
            "channel": "ticker",
            "event": "subscribe",
            "status": "ok",
            "message": "subscribed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ticker_channel_ack_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ticker channel ack not ok."""
        message = {
            "channel": "ticker",
            "event": "subscribe",
            "status": "error",
            "message": "failed to subscribe",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_instrument_channel_ack(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument channel ack."""
        message = {
            "channel": "instrument",
            "event": "subscribe",
            "status": "ok",
            "message": "subscribed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_instrument_channel_ack_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument channel ack not ok."""
        message = {
            "channel": "instrument",
            "event": "subscribe",
            "status": "error",
            "message": "failed to subscribe",
        }
        await kraken_client._on_message(message)


class TestGetOrdersWithoutStatus:
    """Tests for getOrdersWithoutStatus."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_get_orders_no_status_filter(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no status filter."""
        mock_orders = [
            {
                "id": "order1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "price": 50000.0,
                "amount": 0.1,
                "filled": 0.0,
                "status": "open",
                "timestamp": 1704326400000,
            },
            {
                "id": "order2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "price": 51000.0,
                "amount": 0.05,
                "filled": 0.05,
                "status": "closed",
                "timestamp": 1704326500000,
            },
        ]
        with (
            patch.object(
                kraken_client,
                "_ccxt_client",
                create=True,
            ) as mock_ccxt,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                return_value="BTC/USD",
            ),
        ):
            mock_ccxt.fetch_open_orders = AsyncMock(return_value=mock_orders)
            mock_ccxt.fetch_orders = AsyncMock(return_value=mock_orders)
            result = await kraken_client.get_orders(symbol="BTC/USD", status=None)
            assert len(result) == 2


class TestOnMessageOhlcAck:
    """Tests for onMessageOhlcAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_ack_success(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription ack success."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": [],
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_ack_failure(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription ack failure."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": [],
            },
            "success": False,
            "error": "Subscription failed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_with_warnings(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription with warnings."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": ["Rate limit approaching"],
            },
            "success": True,
        }
        await kraken_client._on_message(message)


class TestOnMessageReqIdNotFound:
    """Tests for onMessageReqIdNotFound."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_req_id_not_found(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message req id not found."""
        message = {
            "req_id": 999,
            "success": True,
            "result": {"order_id": "ABC123"},
        }
        await kraken_client._on_message(message)


class TestWithRetryMaxRetries:
    """Tests for withRetryMaxRetries."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_network_error_opens_circuit_breaker(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry network error opens circuit breaker."""
        mock_func = AsyncMock()
        mock_func.side_effect = NetworkError("Network timeout")
        kraken_client._circuit_failures = kraken_client._max_failures - 1
        kraken_client._circuit_open_until = 0
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(NetworkError),
        ):
            await kraken_client._with_retry(mock_func)
        assert kraken_client._circuit_open_until > 0


class TestEnsureWsConnectedAlreadyConnected:
    """Tests for ensureWsConnectedAlreadyConnected."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_already_has_client(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected already has healthy client."""
        mock_ws_client = MagicMock()
        mock_ws_client.start = AsyncMock()
        mock_ws_client.exception_occur = False
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._ensure_ws_connected()
        assert kraken_client._ws_connected is True


@pytest.mark.asyncio
async def test_close_ws_client_closes_session() -> None:
    """WebSocket session closure on the close-error fallback path (#143).

    Given a KrakenExchangeClient whose WebSocket ``close()`` raises mid-way
        (the session it owns is left open),
    When _close_ws_client is called,
    Then ``force_close_ws_client`` closes the session directly and the
        client reference is cleared.
    """
    client = KrakenExchangeClient("key", "secret")
    session_closed = {"called": False}

    class DummySession:
        closed = False

        async def close(self) -> None:
            session_closed["called"] = True
            self.closed = True

    client._ws_connected = True
    client._ws_client = SimpleNamespace(
        close=AsyncMock(side_effect=RuntimeError("close boom")),
        _SpotAsyncClient__session=DummySession(),
    )
    await client._close_ws_client()
    assert client._ws_client is None
    assert session_closed["called"]


@pytest.mark.asyncio
async def test_with_retry_circuit_open_raises() -> None:
    """Circuit breaker prevents retries when open.

    Given a KrakenExchangeClient with an open circuit breaker,
    When _with_retry is called,
    Then it raises RuntimeError without executing the function.
    """
    client = KrakenExchangeClient("key", "secret")
    client._circuit_open_until = time.time() + 10
    with pytest.raises(RuntimeError, match="Circuit breaker open"):
        await client._with_retry(lambda: None)


@pytest.mark.asyncio
async def test_ensure_ws_connected_initializes_client() -> None:
    """WebSocket connection initialization.

    Given a KrakenExchangeClient without an active WebSocket,
    When _ensure_ws_connected is called,
    Then it creates and starts a new WebSocket client.
    """
    client = KrakenExchangeClient("key", "secret")

    class DummyWS:
        called: bool = False

        def __init__(self, key: str, secret: str, callback: Any) -> None:
            self.key = key
            self.secret = secret
            self.callback = callback
            self.exception_occur = False
            self.start = AsyncMock()
            DummyWS.called = True

    with patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient", DummyWS):
        await client._ensure_ws_connected()
    assert isinstance(client._ws_client, DummyWS)
    client._ws_client.start.assert_awaited_once()
    assert client._ws_connected
    assert DummyWS.called


@pytest.mark.asyncio
async def test_get_or_create_ws_client_requires_credentials() -> None:
    """WebSocket client creation requires API credentials.

    Given a KrakenExchangeClient without API credentials,
    When _get_or_create_ws_client is called,
    Then it raises RuntimeError indicating credentials are required.
    """
    client = KrakenExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client._get_or_create_ws_client()


class _StubWsClient:
    """Test stub for WebSocket client."""

    def __init__(self) -> None:
        self.subscribe = AsyncMock()
        self.close = AsyncMock()
        self.exception_occur = False

    async def __aenter__(self) -> _StubWsClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False


class _OneShotQueue(asyncio.Queue[Any]):
    """Test queue that delivers one payload then sets exception flag."""

    def __init__(self, payload: Any, ws: _StubWsClient) -> None:
        super().__init__()
        self._payload = payload
        self._ws = ws
        self._delivered = False

    async def get(self) -> Any:
        if self._delivered:
            await asyncio.sleep(0)
            raise AssertionError("queue consumed twice")
        self._delivered = True
        self._ws.exception_occur = True
        return self._payload


@pytest.mark.asyncio
async def test_subscribe_ticks_with_wildcard() -> None:
    """Tick subscription with wildcard symbol.

    Given a KrakenExchangeClient with a WebSocket client,
    When subscribe_ticks is called with wildcard symbol,
    Then it subscribes and yields tick updates from the queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._tick_queue = _OneShotQueue({"tick": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_ticks(["*"], req_id=7):
            updates.append(item)
    assert updates == [{"tick": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_candles_uses_interval_queue() -> None:
    """Candle subscription uses interval-specific queue.

    Given a KrakenExchangeClient with interval-specific candle queues,
    When subscribe_candles is called with a timeframe,
    Then it yields updates from the corresponding interval queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._candle_queues = {1: _OneShotQueue({"candle": 1}, ws)}
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_candles(["*"], timeframe="1m", req_id=1):
            updates.append(item)
    assert updates == [{"candle": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_trades_consumes_queue() -> None:
    """Trade subscription consumes from trade queue.

    Given a KrakenExchangeClient with a trade queue,
    When subscribe_trades is called,
    Then it yields trade updates from the queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._trade_queue = _OneShotQueue({"trade": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_trades(["*"], req_id=3):
            updates.append(item)
    assert updates == [{"trade": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_executions_consumes_queue_then_raises_on_death() -> None:
    """Execution subscription yields queued updates, then raises on death.

    Given a KrakenExchangeClient whose execution queue delivers one update
        and then flips the client's exception_occur flag to terminal,
    When subscribe_executions is consumed,
    Then the queued update is yielded first and the generator then raises
        ConnectionError (closing the poisoned client) so a supervising
        caller observes the death instead of a clean end.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._execution_queue = _OneShotQueue({"execution": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        with pytest.raises(ConnectionError, match="private WS connection lost"):
            async for item in client.subscribe_executions(req_id=5):
                updates.append(item)
    assert updates == [{"execution": 1}]
    ws.subscribe.assert_called_once()
    ws.close.assert_awaited_once()
    assert client._ws_client is None


@pytest.mark.asyncio
async def test_subscribe_instruments_raw_mode() -> None:
    """Instrument subscription in raw mode.

    Given a KrakenExchangeClient with raw instrument queue,
    When subscribe_instruments is called with raw=True,
    Then it yields raw instrument data from the raw queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._raw_instrument_queue = _OneShotQueue({"symbol": "RAW"}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates = [item async for item in client.subscribe_instruments(raw=True, req_id=11)]
    assert updates == [{"symbol": "RAW"}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_on_message_handles_subscription_acks() -> None:
    """Message handler processes subscription acknowledgments.

    Given a KrakenExchangeClient receiving subscription responses,
    When _on_message is called with various subscription acks,
    Then it handles success, failure, and warning cases appropriately.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": ["BTC/USD"]},
            "success": False,
            "error": "boom",
        }
    )
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions", "snap_orders": True, "snap_trades": False},
            "success": True,
            "warnings": ["slow"],
        }
    )
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1, "warnings": ["lag"]},
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_on_message_instrument_raw_queue_when_parse_fails() -> None:
    """Instrument message falls back to raw queue on parse failure.

    Given a KrakenExchangeClient with instrument parsing that fails,
    When _on_message receives an instrument message,
    Then raw data is queued and parsed queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    payload = {"symbol": "BTC/USD", "status": "online"}
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument",
        side_effect=ValueError("bad mapping"),
    ):
        await client._on_message({"channel": "instrument", "data": {"pairs": [payload]}})
    raw = await client._raw_instrument_queue.get()
    assert raw["symbol"] == "BTC/USD"
    assert client._instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_unexpected_format() -> None:
    """Instrument message with unexpected format is handled gracefully.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument message with non-dict data,
    Then it handles the message without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message({"channel": "instrument", "data": "not-a-dict"})


@pytest.mark.asyncio
async def test_with_retry_rate_limit_backoff() -> None:
    """Retry mechanism applies backoff on rate limit errors.

    Given a KrakenExchangeClient with a function that always rate limits,
    When _with_retry is called,
    Then it retries with backoff before raising the final error.
    """
    client = KrakenExchangeClient("key", "secret")
    calls = {"attempts": 0}

    async def always_rate_limited() -> None:
        calls["attempts"] += 1
        raise ccxt.RateLimitExceeded("rate")

    with (
        patch("snapper.infrastructure.exchanges.implementations.kraken._retry_sleep") as sleeper,
        pytest.raises(ccxt.RateLimitExceeded),
    ):
        await client._with_retry(always_rate_limited)
    assert calls["attempts"] == 3
    assert sleeper.await_count == 2


class _ErrorOnGetQueue(asyncio.Queue[Any]):
    """Test queue that raises error on first get, then sets exception flag."""

    def __init__(self, ws: _StubWsClient) -> None:
        super().__init__()
        self._ws = ws
        self._called = False

    async def get(self) -> Any:
        if not self._called:
            self._called = True
            raise RuntimeError("simulated error")
        self._ws.exception_occur = True
        await asyncio.sleep(0)
        raise AssertionError("should not reach here")


@pytest.mark.asyncio
async def test_subscribe_ticks_handles_receive_error() -> None:
    """Tick subscription handles queue receive errors.

    Given a KrakenExchangeClient with a queue that raises errors,
    When subscribe_ticks is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._tick_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_ticks(["BTC-USD"]):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_candles_handles_receive_error() -> None:
    """Candle subscription handles queue receive errors.

    Given a KrakenExchangeClient with a candle queue that raises errors,
    When subscribe_candles is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._candle_queues = {1: _ErrorOnGetQueue(ws)}
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_candles(["BTC-USD"], timeframe="1m"):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_trades_handles_receive_error() -> None:
    """Trade subscription handles queue receive errors.

    Given a KrakenExchangeClient with a trade queue that raises errors,
    When subscribe_trades is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._trade_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_trades(["BTC-USD"]):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_executions_handles_receive_error() -> None:
    """Execution subscription handles queue receive errors.

    Given a KrakenExchangeClient with an execution queue that raises errors,
    When subscribe_executions is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._execution_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_executions():
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_instruments_handles_receive_error() -> None:
    """Instrument subscription handles queue receive errors.

    Given a KrakenExchangeClient with an instrument queue that raises errors,
    When subscribe_instruments is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._instrument_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates = [item async for item in client.subscribe_instruments()]
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_ticks_raises_on_outer_error() -> None:
    """Tick subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_ticks is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_ticks(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_candles_raises_on_outer_error() -> None:
    """Candle subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_candles is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_candles(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_raises_on_outer_error() -> None:
    """Trade subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_trades is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_trades(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_executions_raises_on_outer_error() -> None:
    """Execution subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_executions is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_executions():
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_instruments_raises_on_outer_error() -> None:
    """Instrument subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_instruments is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_instruments():
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_on_message_with_ohlc_failed_subscription() -> None:
    """Message handler logs OHLC subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed OHLC subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1},
            "success": False,
            "error": "invalid interval",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ticker_status_warning() -> None:
    """Message handler logs ticker status warnings.

    Given a KrakenExchangeClient,
    When _on_message receives a ticker status error event,
    Then it handles the warning without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {"channel": "ticker", "event": "status", "status": "error", "message": "failed"}
    )


@pytest.mark.asyncio
async def test_on_message_instrument_status_warning() -> None:
    """Message handler logs instrument status warnings.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument status error event,
    Then it handles the warning without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {"channel": "instrument", "event": "status", "status": "error", "message": "failed"}
    )


@pytest.mark.asyncio
async def test_on_message_ticker_parse_error() -> None:
    """Message handler skips ticker on parse error.

    Given a KrakenExchangeClient with ticker parsing that fails,
    When _on_message receives a ticker message,
    Then the tick queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._tick_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
        side_effect=ValueError("bad ticker"),
    ):
        await client._on_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]})
    assert client._tick_queue.empty()


@pytest.mark.asyncio
async def test_handle_ticker_data_skips_dict() -> None:
    """Ticker handler ignores non-list data without error.

    Given ticker data as a dict instead of a list,
    When _handle_ticker_data is called,
    Then no items are enqueued and no exception is raised.
    """
    client = KrakenExchangeClient("key", "secret")
    client._tick_queue = asyncio.Queue()
    client._handle_ticker_data({"symbol": "BTC/USD", "bid": 50000.0})
    assert client._tick_queue.empty()


@pytest.mark.asyncio
async def test_on_message_trade_parse_error() -> None:
    """Message handler skips trade on parse error.

    Given a KrakenExchangeClient with trade parsing that fails,
    When _on_message receives a trade message,
    Then the trade queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._trade_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
        side_effect=ValueError("bad trade"),
    ):
        await client._on_message({"channel": "trade", "data": [{"symbol": "BTC/USD"}]})
    assert client._trade_queue.empty()


@pytest.mark.asyncio
async def test_on_message_execution_parse_error() -> None:
    """Message handler skips execution on parse error.

    Given a KrakenExchangeClient with execution parsing that fails,
    When _on_message receives an execution message,
    Then the execution queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._execution_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_execution_list",
        side_effect=ValueError("bad execution"),
    ):
        await client._on_message({"channel": "executions", "data": [{"order_id": "123"}]})
    assert client._execution_queue.empty()


@pytest.mark.asyncio
async def test_on_message_candle_parse_error() -> None:
    """Message handler skips candle on parse error.

    Given a KrakenExchangeClient with candle parsing that fails,
    When _on_message receives a candle message,
    Then the candle queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {1: asyncio.Queue()}
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
        side_effect=ValueError("bad candle"),
    ):
        await client._on_message({"channel": "ohlc", "data": [{"symbol": "BTC/USD"}]})
    assert client._candle_queues[1].empty()


@pytest.mark.asyncio
async def test_on_message_ohlc_broadcasts_to_all_queues_when_interval_not_matched() -> None:
    """OHLC message broadcasts to all queues when interval not matched.

    Given a KrakenExchangeClient with multiple candle queues,
    When _on_message receives an OHLC with unmatched interval,
    Then the candle is broadcast to all interval queues.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {5: asyncio.Queue(), 15: asyncio.Queue()}
    mock_candle = CandleUpdate(
        symbol="BTC-USD",
        interval=1,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        volume=100.0,
        vwap=50250.0,
        trades=500,
        interval_begin=datetime.now(UTC),
    )
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
        return_value=[mock_candle],
    ):
        await client._on_message({"channel": "ohlc", "data": [{"symbol": "BTC/USD"}]})
    assert not client._candle_queues[5].empty()
    assert not client._candle_queues[15].empty()


@pytest.mark.asyncio
async def test_on_message_ohlc_empty_list_when_invalid_data_type() -> None:
    """OHLC message with invalid data type leaves queue empty.

    Given a KrakenExchangeClient with a candle queue,
    When _on_message receives an OHLC with non-list data,
    Then the candle queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {1: asyncio.Queue()}
    await client._on_message({"channel": "ohlc", "data": "invalid"})
    assert client._candle_queues[1].empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_dict_having_pairs() -> None:
    """Instrument message with pairs dict queues raw data.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with pairs,
    Then raw instrument data is queued.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument",
        side_effect=ValueError("mocked"),
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [{"symbol": "BTC/USD", "status": "online"}]},
            }
        )
    assert not client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_empty_pairs() -> None:
    """Instrument message with empty pairs leaves queue empty.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with empty pairs,
    Then the raw instrument queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument",
        side_effect=ValueError("mocked"),
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [], "assets": []},
            }
        )
    assert client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_null_data() -> None:
    """Instrument message with null data leaves queues empty.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with null data,
    Then both instrument queues remain empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    await client._on_message({"channel": "instrument", "data": None})
    assert client._raw_instrument_queue.empty()
    assert client._instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_validation_error_skips_pair() -> None:
    """Instrument message skips pair on validation error.

    Given a KrakenExchangeClient with schema validation that fails,
    When _on_message receives an instrument message,
    Then the invalid pair is skipped and queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.KrakenInstrumentPairSchema.model_validate",
        side_effect=ValidationError.from_exception_data("test", []),
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [{"symbol": "BAD/PAIR"}]},
            }
        )
    assert client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_trade_subscription_failed_ack() -> None:
    """Message handler logs trade subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed trade subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": ["BTC/USD"]},
            "success": False,
            "error": "subscription failed",
        }
    )


@pytest.mark.asyncio
async def test_on_message_executions_subscription_failed_ack() -> None:
    """Message handler logs executions subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed executions subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions", "snap_orders": True, "snap_trades": False},
            "success": False,
            "error": "auth failed",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_failed_ack() -> None:
    """Message handler logs OHLC subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed OHLC subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1},
            "success": False,
            "error": "invalid interval",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_with_warnings() -> None:
    """Message handler logs OHLC subscription warnings.

    Given a KrakenExchangeClient,
    When _on_message receives an OHLC subscription with warnings,
    Then it handles the warnings without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": ["rate limit approaching", "deprecated interval"],
            },
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_ohlc_known_deprecation_notice_logged_once_then_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Known Kraken OHLC deprecation notice is logged INFO once, then DEBUG.

    Given a KrakenExchangeClient and the fixed Kraken v2 informational
    notice ``timestamp is deprecated, use interval_begin`` which Kraken
    emits in ``result.warnings`` on every OHLC subscribe ack,
    When the same notice arrives twice through ``_on_message``,
    Then the first occurrence is logged once at INFO with an explanation
    and subsequent occurrences are downgraded to DEBUG so the warning
    stream stays meaningful per the clean-signal-log rule.
    """
    _OHLC_NOTICES_LOGGED.discard(_OHLC_DEPRECATED_TIMESTAMP_NOTICE)
    client = KrakenExchangeClient("key", "secret")
    ack_message = {
        "method": "subscribe",
        "result": {
            "channel": "ohlc",
            "symbol": "BTC/USD",
            "interval": 1,
            "warnings": [_OHLC_DEPRECATED_TIMESTAMP_NOTICE],
        },
        "success": True,
    }
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            await client._on_message(ack_message)
            await client._on_message(ack_message)
    finally:
        logger.remove(sink_id)
    info_records = [r for r in caplog.records if r.levelname == "INFO"]
    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    warning_or_error = [r for r in caplog.records if r.levelname in {"WARNING", "ERROR"}]
    assert any(
        "timestamp is deprecated" in r.message for r in info_records
    ), f"expected exactly one INFO-level notice, got {[r.message for r in info_records]}"
    assert any(
        "OHLC subscription notice" in r.message for r in debug_records
    ), f"expected DEBUG suppression on repeat, got {[r.message for r in debug_records]}"
    assert (
        not warning_or_error
    ), f"known notice must not trigger WARNING/ERROR, got {[r.message for r in warning_or_error]}"


@pytest.mark.asyncio
async def test_on_message_subscribe_ack_unknown_channel() -> None:
    """Message handler handles unknown channel subscription.

    Given a KrakenExchangeClient,
    When _on_message receives a subscription ack for unknown channel,
    Then it handles the message without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {
                "channel": "unknown_channel",
                "symbol": "BTC/USD",
            },
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_on_message_general_exception_handling() -> None:
    """Message handler catches general exceptions.

    Given a KrakenExchangeClient with parsing that raises unexpected error,
    When _on_message receives a message,
    Then it catches the exception without propagating it.
    """
    client = KrakenExchangeClient("key", "secret")
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
        side_effect=Exception("unexpected error"),
    ):
        await client._on_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]})


@pytest.mark.asyncio
async def test_on_message_trade_subscription_validation_error() -> None:
    """Message handler handles trade subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives a trade subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade"},
            "success": "invalid",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_validation_error() -> None:
    """Message handler handles OHLC subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives an OHLC subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc"},
            "success": "not-bool",
        }
    )


@pytest.mark.asyncio
async def test_on_message_executions_subscription_validation_error() -> None:
    """Message handler handles executions subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives an executions subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions"},
            "success": "not-bool",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ticker_validation_error() -> None:
    """Message handler handles ticker with invalid status field.

    Given a KrakenExchangeClient,
    When _on_message receives a ticker with invalid status value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "channel": "ticker",
            "event": "subscribe",
            "status": 123,
        }
    )


@pytest.mark.asyncio
async def test_on_message_instrument_validation_error() -> None:
    """Message handler handles instrument with invalid status field.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument with invalid status value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "channel": "instrument",
            "event": "subscribe",
            "status": 456,
        }
    )


class TestKrakenAdditionalCoverage:
    """Tests for krakenAdditionalCoverage."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    async def test_with_retry_max_retries_exceeded(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry max retries exceeded."""
        mock_func = AsyncMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
            new_callable=AsyncMock,
        ):
            mock_func.side_effect = ccxt.NetworkError("Connection failed")
            with pytest.raises(ccxt.NetworkError, match="Connection failed"):
                await kraken_client._with_retry(mock_func)
            assert mock_func.call_count == 3

    async def test_on_message_ohlc_with_symbol(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ohlc with symbol."""
        message = {
            "channel": "ohlc",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "open": 50000.0,
                    "high": 50100.0,
                    "low": 49900.0,
                    "close": 50050.0,
                    "vwap": 50025.0,
                    "trades": 100,
                    "volume": 10.0,
                    "interval": 1,
                    "interval_begin": "2024-01-01T00:00:00Z",
                }
            ],
        }
        mock_queue_1m = MagicMock()
        mock_queue_5m = MagicMock()
        with patch.object(kraken_client, "_candle_queues", {1: mock_queue_1m, 5: mock_queue_5m}):
            await kraken_client._on_message(message)
            mock_queue_1m.put_nowait.assert_called_once()
            mock_queue_5m.put_nowait.assert_not_called()
            candle_data = mock_queue_1m.put_nowait.call_args[0][0]
            assert isinstance(candle_data, CandleUpdate)
            assert candle_data.symbol == "BTC-USD"
            assert candle_data.open == pytest.approx(50000.0)
            assert candle_data.interval == 1

    async def test_on_message_ohlc_broadcast(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ohlc broadcast."""
        message = {
            "channel": "ohlc",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "open": 50000.0,
                    "high": 50010.0,
                    "low": 49990.0,
                    "close": 50005.0,
                    "vwap": 50000.0,
                    "trades": 50,
                    "volume": 5.0,
                    "interval": 99,
                    "interval_begin": "2024-01-01T00:00:00Z",
                }
            ],
        }
        mock_queue_1 = MagicMock()
        mock_queue_2 = MagicMock()
        with patch.object(kraken_client, "_candle_queues", {1: mock_queue_1, 5: mock_queue_2}):
            await kraken_client._on_message(message)
            mock_queue_1.put_nowait.assert_called_once()
            mock_queue_2.put_nowait.assert_called_once()

    async def test_on_message_ticker_update(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ticker update."""
        message = {
            "channel": "ticker",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bid": 50000.0,
                    "bid_qty": 1.0,
                    "ask": 50010.0,
                    "ask_qty": 1.5,
                    "last": 50005.0,
                    "volume": 100.0,
                    "vwap": 50002.0,
                    "low": 49900.0,
                    "high": 50100.0,
                    "change": 100.0,
                    "change_pct": 0.2,
                }
            ],
        }
        with patch.object(kraken_client, "_tick_queue") as mock_queue:
            await kraken_client._on_message(message)
            mock_queue.put_nowait.assert_called_once()
            ticker_data = mock_queue.put_nowait.call_args[0][0]
            assert isinstance(ticker_data, TickerUpdate)
            assert ticker_data.symbol == "BTC-USD"

    async def test_on_message_trade_update(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message trade update."""
        trade_message = {
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "qty": 0.5,
                    "price": 50000.0,
                    "ord_type": "limit",
                    "trade_id": 12345,
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ],
        }
        with patch.object(kraken_client, "_trade_queue") as mock_queue:
            await kraken_client._on_message(trade_message)
            mock_queue.put_nowait.assert_called_once()
            trade_data = mock_queue.put_nowait.call_args[0][0]
            assert isinstance(trade_data, TradeUpdate)
            assert trade_data.symbol == "BTC-USD"

    async def test_on_message_execution_update(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message execution update."""
        execution_message = {
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "ORDER123",
                    "exec_type": "new",
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "order_type": "limit",
                    "order_status": "new",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "cum_qty": 0.0,
                    "cum_cost": 0.0,
                }
            ],
        }
        with patch.object(kraken_client, "_execution_queue") as mock_queue:
            await kraken_client._on_message(execution_message)
            mock_queue.put_nowait.assert_called_once()
            execution_data = mock_queue.put_nowait.call_args[0][0]
            assert isinstance(execution_data, ExecutionUpdate)
            assert execution_data.order_id == "ORDER123"

    async def test_on_message_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message exception handling."""
        bad_message = {
            "channel": "ticker",
            "type": "update",
            "data": [{"symbol": "BTC/USD"}],
        }
        await kraken_client._on_message(bad_message)

    async def test_with_retry_success_resets_circuit_breaker(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry success resets circuit breaker."""
        mock_func = AsyncMock(return_value="success")
        result = await kraken_client._with_retry(mock_func)
        assert result == "success"

    async def test_with_retry_unexpected_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry unexpected exception."""
        mock_func = AsyncMock()
        mock_func.side_effect = ValueError("Unexpected error")
        with pytest.raises(ValueError, match="Unexpected error"):
            await kraken_client._with_retry(mock_func)
        assert mock_func.call_count == 1

    async def test_on_message_instrument_snapshot(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument snapshot."""
        sentinel = InstrumentPairDescriptor(
            symbol="BTC/USD",
            base="BTC",
            quote="USD",
            status="online",
            qty_precision=8,
            qty_increment=0.0001,
            qty_min=0.0001,
            price_precision=2,
            price_increment=0.5,
            cost_precision=2,
            cost_min=10.0,
            marginable=False,
            has_index=False,
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument",
            return_value=sentinel,
        ):
            message = {
                "channel": "instrument",
                "data": {
                    "pairs": [
                        {
                            "symbol": "BTC/USD",
                        }
                    ]
                },
            }
            result_queue: asyncio.Queue[InstrumentPairDescriptor] = asyncio.Queue()
            kraken_client._instrument_queue = result_queue
            await kraken_client._on_message(message)
            queued = await result_queue.get()
            assert queued is sentinel

    async def test_subscribe_instruments_yields_messages(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe instruments yields messages."""

        class _StubWsClient:
            def __init__(self) -> None:
                self.subscribe = AsyncMock()
                self.exception_occur = False

            async def __aenter__(self) -> _StubWsClient:
                return self

            async def __aexit__(
                self,
                exc_type: type | None,
                exc_val: Exception | None,
                exc_tb: object | None,
            ) -> bool:
                return False

        class _InstrumentQueue(asyncio.Queue[InstrumentPairDescriptor]):
            def __init__(self, ws_client: _StubWsClient, payload: InstrumentPairDescriptor) -> None:
                super().__init__()
                self._ws_client = ws_client
                self._payload = payload
                self._delivered = False

            async def get(self) -> InstrumentPairDescriptor:
                if self._delivered:
                    await asyncio.sleep(0)
                    raise AssertionError("Queue get called more than once")
                self._delivered = True
                self._ws_client.exception_occur = True
                return self._payload

        ws_client = _StubWsClient()
        kraken_client._ws_client = ws_client
        kraken_client._ws_connected = True
        instrument_update = InstrumentPairDescriptor(
            symbol="ETH/USD",
            base="ETH",
            quote="USD",
            status="online",
            qty_precision=8,
            qty_increment=0.0001,
            qty_min=0.0001,
            price_precision=2,
            price_increment=0.5,
            cost_precision=2,
            cost_min=10.0,
            marginable=False,
            has_index=True,
        )
        kraken_client._instrument_queue = _InstrumentQueue(ws_client, instrument_update)
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock) as ensure:
            ensure.return_value = None
            updates = [update async for update in kraken_client.subscribe_instruments()]
        ws_client.subscribe.assert_called_once()
        assert updates == [asdict(instrument_update)]

    @pytest.mark.asyncio
    async def test_close_ws_client_closes_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client closes session.

        The stub mirrors the real SDK contract: ``close()`` closes the
        client's own aiohttp session internally (``super().close()``), so a
        successful in-bound close needs no venue-layer session handling.
        """

        class _StubSession:
            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        class _StubWsClient:
            def __init__(self) -> None:
                self.closed = False
                self._SpotAsyncClient__session = _StubSession()

            async def close(self) -> None:
                self.closed = True
                await self._SpotAsyncClient__session.close()

        ws_client = _StubWsClient()
        kraken_client._ws_client = ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        assert ws_client.closed is True
        assert ws_client._SpotAsyncClient__session.closed is True
        assert kraken_client._ws_client is None
        assert kraken_client._ws_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_websocket_invokes_close(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect websocket invokes close."""
        await kraken_client.disconnect_websocket()

    @pytest.mark.asyncio
    async def test_create_order_fallback_without_price_or_asset_class(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback without price or asset class."""
        order_request = ExchangeOrderRequest(
            symbol="FOO-BAR",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=1.0,
            price=None,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: FOO-BAR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="FOOBAR/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            trade_client = MagicMock()
            mock_trade_class.return_value = trade_client
            trade_client.create_order.return_value = {"txid": ["OID123"]}
            order = await kraken_client.create_order(order_request)
            kwargs = trade_client.create_order.call_args.kwargs
            assert "price" not in kwargs
            assert kwargs["extra_params"] is None
            assert order.id == "OID123"

    @pytest.mark.asyncio
    async def test_cancel_order_fallback_errors_are_raised(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order fallback errors are raised."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            trade_client = MagicMock()
            mock_trade_class.return_value = trade_client
            trade_client.cancel_order.side_effect = RuntimeError("boom")
            with pytest.raises(RuntimeError, match="boom"):
                await kraken_client.cancel_order("OID", symbol="AAPLx-USD")

    @pytest.mark.asyncio
    async def test_get_orders_filters_status(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders filters status."""
        open_order = MagicMock(status=ExchangeOrderStatusEnum.OPEN)
        closed_order = MagicMock(status=ExchangeOrderStatusEnum.CLOSED)
        with (
            patch.object(kraken_client, "_with_retry", AsyncMock(return_value=[{}, {}])),
            patch.object(
                kraken_client,
                "_convert_ccxt_order",
                side_effect=[open_order, closed_order],
            ),
        ):
            result = await kraken_client.get_orders(status=ExchangeOrderStatusEnum.CLOSED)
        assert result == [closed_order]

    @pytest.mark.asyncio
    async def test_get_balance_skips_non_dict_entries(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance skips non dict entries."""
        payload = {
            "free": {},
            "USD": {"free": 1, "used": 2, "total": 3},
            "EUR": None,
            "timestamp": 123,
        }
        with patch.object(kraken_client, "_with_retry", AsyncMock(return_value=payload)):
            balances = await kraken_client.get_balance()
        assert list(balances) == ["USD"]
        assert balances["USD"].free == 1

    @pytest.mark.asyncio
    async def test_get_or_create_ws_client_calls_ensure(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get or create ws client calls ensure."""
        sentinel = object()
        kraken_client._ws_client = sentinel
        with patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()) as ensure:
            result = await kraken_client._get_or_create_ws_client()
        ensure.assert_awaited_once()
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_initializes_client(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected initializes client."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as mock_ws_class:
            ws_instance = MagicMock()
            ws_instance.start = AsyncMock()
            mock_ws_class.return_value = ws_instance
            await kraken_client._ensure_ws_connected()
            mock_ws_class.assert_called_once()
            ws_instance.start.assert_awaited_once()
            assert kraken_client._ws_client is ws_instance
            assert kraken_client._ws_connected is True

    @pytest.mark.asyncio
    async def test_with_retry_opens_circuit_after_network_errors(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry opens circuit after network errors."""
        kraken_client._circuit_failures = 5
        failing_call = AsyncMock(side_effect=ccxt.NetworkError("offline"))
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep", AsyncMock()
            ),
            pytest.raises(ccxt.NetworkError),
        ):
            await kraken_client._with_retry(failing_call)
        assert kraken_client._circuit_open_until > time.time()


class TestKrakenFallbackToNativeAPI:
    """Tests for krakenFallbackToNativeAPI."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_create_order_fallback_to_native_api(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback to native api."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {
                "txid": ["ORDER123"],
                "descr": {"order": "buy 10 AAPLx/USD @ limit 150.0"},
            }
            order_request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=float("10"),
                price=float("150.0"),
                client_order_id="CLIENT123",
            )
            result = await kraken_client.create_order(order_request)
            mock_trade_client.create_order.assert_called_once()
            call_kwargs = mock_trade_client.create_order.call_args.kwargs
            assert call_kwargs["ordertype"] == "limit"
            assert call_kwargs["side"] == "buy"
            assert call_kwargs["pair"] == "AAPLx/USD"
            assert call_kwargs["volume"] == "10.0"
            assert call_kwargs["price"] == "150.0"
            assert call_kwargs["extra_params"] is not None
            assert call_kwargs["extra_params"]["cl_ord_id"] == "CLIENT123"
            assert call_kwargs["extra_params"]["asset_class"] == "tokenized_asset"
            assert result.id == "ORDER123"
            assert result.symbol == "AAPLx-USD"
            assert result.side == OrderSideEnum.BUY
            assert result.type == ExchangeOrderTypeEnum.LIMIT
            assert result.amount == float("10")
            assert result.price == float("150.0")
            assert result.status == ExchangeOrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_cancel_order_fallback_to_native_api(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order fallback to native api."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.cancel_order.return_value = {"count": 1}
            result = await kraken_client.cancel_order("ORDER123", symbol="AAPLx-USD")
            mock_trade_client.cancel_order.assert_called_once_with(txid="ORDER123")
            assert result.id == "ORDER123"
            assert result.symbol == "AAPLx-USD"
            assert result.status == ExchangeOrderStatusEnum.CANCELED

    @pytest.mark.asyncio
    async def test_create_order_no_fallback_for_other_errors(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order no fallback for other errors."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
            side_effect=ValueError("Different error"),
        ):
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            with pytest.raises(ValueError, match="Different error"):
                await kraken_client.create_order(order_request)

    @pytest.mark.asyncio
    async def test_cancel_order_no_fallback_without_symbol(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order no fallback without symbol."""
        with (
            patch.object(kraken_client, "_ccxt_client") as mock_ccxt_client,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol"),
            ),
            pytest.raises(ValueError, match="Unknown native symbol"),
        ):
            mock_ccxt_client.cancel_order = AsyncMock(
                side_effect=ValueError("Unknown native symbol")
            )
            await kraken_client.cancel_order("ORDER123", symbol=None)

    @pytest.mark.asyncio
    async def test_get_trade_client_lazy_initialization(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get trade client lazy initialization."""
        assert kraken_client._trade_client is None
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.Trade"
        ) as mock_trade_class:
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            client1 = kraken_client._get_trade_client()
            assert client1 is mock_trade_client
            mock_trade_class.assert_called_once_with(key="test_key", secret="test_secret")
            client2 = kraken_client._get_trade_client()
            assert client2 is mock_trade_client
            assert mock_trade_class.call_count == 1

    def test_get_trade_client_no_credentials(self) -> None:
        """Verify get trade client no credentials."""
        client = KrakenExchangeClient()
        with pytest.raises(RuntimeError, match="API credentials required"):
            client._get_trade_client()

    @pytest.mark.asyncio
    async def test_create_order_fallback_handles_native_api_error(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback handles native api error."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.side_effect = Exception("Native API error")
            order_request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=float("10"),
                price=float("150.0"),
            )
            with pytest.raises(Exception, match="Native API error"):
                await kraken_client.create_order(order_request)


class TestKrakenExchangeClientSimpleEdgeCases:
    """Tests for krakenExchangeClientSimpleEdgeCases."""

    @pytest.fixture
    def client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.fixture(autouse=True)
    def _patch_symbol_mapper(self, monkeypatch: MonkeyPatch) -> None:
        def _identity(symbol: str) -> str:
            return symbol

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt", _identity
        )

    def test_init_without_credentials(self) -> None:
        """Verify init without credentials."""
        client = KrakenExchangeClient()
        assert client.api_key is None
        assert client.api_secret is None
        assert not client.sandbox

    def test_init_with_credentials(self) -> None:
        """Verify init with credentials."""
        client = KrakenExchangeClient(api_key="key", api_secret="secret")
        assert client.api_key == "key"
        assert client.api_secret == "secret"
        assert not client.sandbox

    def test_init_sandbox_mode(self) -> None:
        """Verify init sandbox mode raises when Kraken lacks a sandbox URL.

        Kraken Spot has no public sandbox endpoint, so initialising
        with ``sandbox=True`` MUST raise. Older ccxt versions
        produced a ``NotSupported`` with the explicit
        ``"does not have a sandbox URL"`` message; newer ccxt
        releases (4.5+) raise a bare ``TypeError`` because the
        ``urls['test']`` lookup yields ``None`` and downstream
        ``self.extend(None)`` fails. We accept either as proof that
        the sandbox flag does not silently fall through to a live
        URL.
        """
        try:
            KrakenExchangeClient(api_key="key", api_secret="secret", sandbox=True)
        except Exception as exc:
            message = str(exc)
            assert "does not have a sandbox URL" in message or "NoneType" in message
        else:
            raise AssertionError("Expected sandbox initialisation to raise")

    @pytest.mark.asyncio
    async def test_disconnect_without_connection(self, client: KrakenExchangeClient) -> None:
        """Verify disconnect without connection."""
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_multiple_disconnects(self, client: KrakenExchangeClient) -> None:
        """Verify multiple disconnects."""
        await client.disconnect()
        await client.disconnect()
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_create_order_without_credentials(self) -> None:
        """Verify create order without credentials."""
        client = KrakenExchangeClient()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
        )
        with pytest.raises(RuntimeError, match="API credentials required for trading"):
            await client.create_order(request)

    @pytest.mark.asyncio
    async def test_get_balance_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get balance api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_balance = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_balance()

    @pytest.mark.asyncio
    async def test_order_creation_edge_cases(self, client: KrakenExchangeClient) -> None:
        """Verify order creation edge cases."""
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.00001,
            price=1.0,
        )
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.create_order = AsyncMock(
                return_value={
                    "id": "test_id",
                    "symbol": "BTC/USD",
                    "amount": 0.00001,
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "price": 1.0,
                    "filled": 0.0,
                    "remaining": 0.00001,
                    "timestamp": 1640995200000,
                    "datetime": "2022-01-01T00:00:00.000Z",
                    "fee": None,
                    "trades": [],
                    "info": {},
                }
            )
            result = await client.create_order(request)
            assert result is not None

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, client: KrakenExchangeClient) -> None:
        """Verify cancel nonexistent order."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(side_effect=Exception("Order not found"))
            with pytest.raises(Exception, match="Order not found"):
                await client.cancel_order("nonexistent_id", "BTC/USD")

    @pytest.mark.asyncio
    async def test_get_order_nonexistent(self, client: KrakenExchangeClient) -> None:
        """Verify get order nonexistent."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(side_effect=Exception("Order not found"))
            with pytest.raises(Exception, match="Order not found"):
                await client.get_order("nonexistent_id", "BTC/USD")

    @pytest.mark.asyncio
    async def test_get_orders_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get orders api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_open_orders = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_orders("BTC/USD", status=ExchangeOrderStatusEnum.OPEN)

    @pytest.mark.asyncio
    async def test_get_ticker_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get ticker api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_ticker = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_ticker("BTC/USD")

    @pytest.mark.asyncio
    async def test_disconnect_with_cleanup_error(self, client: KrakenExchangeClient) -> None:
        """Verify disconnect with cleanup error."""
        with patch.object(client, "_close_ws_client", new_callable=AsyncMock) as mock_close:
            mock_close.side_effect = Exception("Cleanup error")
            await client.disconnect()
            mock_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ohlcv_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get ohlcv api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_ohlcv = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_ohlcv("BTC/USD")

    @pytest.mark.asyncio
    async def test_connect_with_error(self, client: KrakenExchangeClient) -> None:
        """Verify connect with error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.load_markets = AsyncMock(side_effect=Exception("Connection Error"))
            with pytest.raises(Exception, match="Connection Error"):
                await client.connect()


class TestCcxtOrderLeverageAndPostOnly:
    """Tests for leverage and post_only passthrough in _create_order_via_ccxt."""

    @pytest.fixture
    def client(self) -> KrakenExchangeClient:
        """Provide authenticated test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.fixture(autouse=True)
    def _patch_symbol_mapper(self, monkeypatch: MonkeyPatch) -> None:
        """Map native symbols to CCXT format via identity function."""

        def _identity(symbol: str) -> str:
            return symbol

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt", _identity
        )

    @pytest.mark.asyncio
    async def test_ccxt_order_with_leverage(self, client: KrakenExchangeClient) -> None:
        """Verify leverage is passed through ccxt_params when set.

        Given: An ExchangeOrderRequest with leverage=3,
        When: _create_order_via_ccxt is called,
        Then: The ccxt create_order call includes leverage=3 in params.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.create_order = AsyncMock(
                return_value={
                    "id": "lev-id",
                    "symbol": "BTC/USD",
                    "amount": 1.0,
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "price": 50000.0,
                    "filled": 0.0,
                    "remaining": 1.0,
                    "timestamp": 1640995200000,
                    "datetime": "2022-01-01T00:00:00.000Z",
                    "fee": None,
                    "trades": [],
                    "info": {},
                }
            )
            request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=1.0,
                price=50000.0,
                leverage=3,
            )
            with patch.object(client, "_log_order_to_db", new_callable=AsyncMock) as mock_log:
                mock_log.return_value = None
                result = await client.create_order(request)
            call_args = mock_ccxt.create_order.call_args
            params = call_args[0][5]
            assert params["leverage"] == 3
            assert result.id == "lev-id"

    @pytest.mark.asyncio
    async def test_ccxt_order_with_post_only(self, client: KrakenExchangeClient) -> None:
        """Verify post_only is passed through ccxt_params when True.

        Given: An ExchangeOrderRequest with post_only=True,
        When: _create_order_via_ccxt is called,
        Then: The ccxt create_order call includes postOnly=True in params.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.create_order = AsyncMock(
                return_value={
                    "id": "post-id",
                    "symbol": "BTC/USD",
                    "amount": 1.0,
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "price": 50000.0,
                    "filled": 0.0,
                    "remaining": 1.0,
                    "timestamp": 1640995200000,
                    "datetime": "2022-01-01T00:00:00.000Z",
                    "fee": None,
                    "trades": [],
                    "info": {},
                }
            )
            request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=1.0,
                price=50000.0,
                post_only=True,
            )
            with patch.object(client, "_log_order_to_db", new_callable=AsyncMock) as mock_log:
                mock_log.return_value = None
                result = await client.create_order(request)
            call_args = mock_ccxt.create_order.call_args
            params = call_args[0][5]
            assert params["postOnly"] is True
            assert result.id == "post-id"


class TestNativeOrderLeverageAndPostOnly:
    """Tests for leverage and post_only in _create_order_via_native."""

    @pytest.fixture
    def client(self) -> KrakenExchangeClient:
        """Provide authenticated test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_native_order_with_leverage(self, client: KrakenExchangeClient) -> None:
        """Verify leverage is converted to string in native API params.

        Given: An ExchangeOrderRequest with leverage=5,
        When: _create_order_via_native is called (via fallback),
        Then: The native Trade.create_order call includes leverage='5'.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {
                "txid": ["NAT-LEV"],
                "descr": {"order": "buy 10 AAPLx/USD @ limit 150.0"},
            }
            request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=10.0,
                price=150.0,
                leverage=5,
            )
            result = await client.create_order(request)
            call_kwargs = mock_trade_client.create_order.call_args.kwargs
            assert call_kwargs["leverage"] == "5"
            assert result.id == "NAT-LEV"

    @pytest.mark.asyncio
    async def test_native_order_with_post_only(self, client: KrakenExchangeClient) -> None:
        """Verify post_only sets oflags to 'post' in native API params.

        Given: An ExchangeOrderRequest with post_only=True,
        When: _create_order_via_native is called (via fallback),
        Then: The native Trade.create_order call includes oflags='post'.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {
                "txid": ["NAT-POST"],
                "descr": {"order": "buy 10 AAPLx/USD @ limit 150.0"},
            }
            request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=10.0,
                price=150.0,
                post_only=True,
            )
            result = await client.create_order(request)
            call_kwargs = mock_trade_client.create_order.call_args.kwargs
            assert call_kwargs["oflags"] == "post"
            assert result.id == "NAT-POST"


class TestKrakenLiveFixtures:
    """Tests using real Kraken API response data as mock fixtures.

    All fixture data comes from live integration tests against Kraken spot
    for BTC-EUR. These tests verify that ExchangeOrderSnapshot fields are
    correctly populated when the client processes real exchange responses
    across three execution paths: CCXT, native REST, and WebSocket.
    """

    CCXT_PASSIVE_CREATE: dict[str, Any] = {
        "id": "OODTGX",
        "clientOrderId": "019d5d4c-d6ab",
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "buy",
        "amount": 0.0001,
        "price": 29119.5,
        "status": "open",
        "timestamp": 1743800000000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_PASSIVE_FETCH: dict[str, Any] = {
        "id": "OODTGX",
        "clientOrderId": "019d5d4c-d6ab",
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "buy",
        "amount": 0.0001,
        "price": 29119.5,
        "status": "open",
        "timestamp": 1743800000000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_PASSIVE_CANCEL_FETCH: dict[str, Any] = {
        "id": "OODTGX",
        "clientOrderId": "019d5d4c-d6ab",
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "buy",
        "amount": 0.0001,
        "price": 29119.5,
        "status": "canceled",
        "timestamp": 1743800000000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_TOPBOOK_CREATE: dict[str, Any] = {
        "id": "ONHBU2",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "sell",
        "amount": 0.0001,
        "price": 58239.1,
        "status": "open",
        "timestamp": 1743800100000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_TOPBOOK_FETCH_FILLED: dict[str, Any] = {
        "id": "ONHBU2",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "sell",
        "amount": 0.0001,
        "price": 58239.1,
        "status": "closed",
        "timestamp": 1743800100000,
        "fee": None,
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_TOPBOOK_CANCEL_UNFILLED: dict[str, Any] = {
        "id": "ONE2HB",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "sell",
        "amount": 0.0001,
        "price": 58239.1,
        "status": "canceled",
        "timestamp": 1743800200000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_AGGRESSIVE_CREATE: dict[str, Any] = {
        "id": "O2CMWJ",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "sell",
        "amount": 0.0001,
        "price": 58238.0,
        "status": "closed",
        "timestamp": 1743800300000,
        "fee": {"cost": 0.01456, "currency": "EUR"},
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_AGGRESSIVE_FETCH: dict[str, Any] = {
        "id": "O2CMWJ",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "sell",
        "amount": 0.0001,
        "price": 58238.0,
        "status": "closed",
        "timestamp": 1743800300000,
        "fee": {"cost": 0.01456, "currency": "EUR"},
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_MARKET_CREATE: dict[str, Any] = {
        "id": "OJWE3F",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "market",
        "side": "sell",
        "amount": 0.0001,
        "price": 58232.7,
        "status": "closed",
        "timestamp": 1743800400000,
        "fee": {"cost": 0.02329, "currency": "EUR"},
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_MARKET_FETCH: dict[str, Any] = {
        "id": "OJWE3F",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "market",
        "side": "sell",
        "amount": 0.0001,
        "price": 58232.7,
        "status": "closed",
        "timestamp": 1743800400000,
        "fee": {"cost": 0.02329, "currency": "EUR"},
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_MARKET_AVG_FETCH: dict[str, Any] = {
        "id": "OAVG01",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "market",
        "side": "sell",
        "amount": 0.0001,
        "price": None,
        "average": 58000.0,
        "status": "closed",
        "timestamp": 1743800400000,
        "fee": {"cost": 0.02329, "currency": "EUR"},
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_MARKET_NOPRICE_NOAVG_FETCH: dict[str, Any] = {
        "id": "ONOPX1",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "market",
        "side": "sell",
        "amount": 0.0001,
        "price": None,
        "average": None,
        "status": "closed",
        "timestamp": 1743800400000,
        "fee": None,
        "filled": 0.0001,
        "remaining": 0.0,
    }

    CCXT_MARKET_AVG_UNFILLED_FETCH: dict[str, Any] = {
        "id": "OUNFIL",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "market",
        "side": "sell",
        "amount": 0.0001,
        "price": None,
        "average": 58000.0,
        "status": "open",
        "timestamp": 1743800400000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_CANCEL_INFLIGHT_CREATE: dict[str, Any] = {
        "id": "OLKARR",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "buy",
        "amount": 0.0001,
        "price": 29119.5,
        "status": "open",
        "timestamp": 1743800500000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    CCXT_CANCEL_INFLIGHT_CANCEL_FETCH: dict[str, Any] = {
        "id": "OLKARR",
        "clientOrderId": None,
        "symbol": "BTC/EUR",
        "type": "limit",
        "side": "buy",
        "amount": 0.0001,
        "price": 29119.5,
        "status": "canceled",
        "timestamp": 1743800500000,
        "fee": None,
        "filled": 0.0,
        "remaining": 0.0001,
    }

    @pytest.fixture
    def client(self) -> KrakenExchangeClient:
        """Provide authenticated test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.fixture(autouse=True)
    def _patch_symbol_mapper(self, monkeypatch: MonkeyPatch) -> None:
        """Map native symbols to CCXT format for BTC-EUR."""

        def _native_to_ccxt(symbol: str) -> str:
            return symbol.replace("-", "/")

        def _ccxt_to_native(symbol: str) -> str:
            return symbol.replace("/", "-")

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
            _native_to_ccxt,
        )
        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.kraken.ccxt_to_native",
            _ccxt_to_native,
        )

    @pytest.mark.asyncio
    async def test_ccxt_passive_create(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT passive limit buy creates PENDING snapshot from request data.

        Given: A limit buy at 29119.5 EUR for 0.0001 BTC (well below market),
        When: create_order is called via CCXT path,
        Then: The snapshot has status=PENDING, filled=0, remaining=amount,
              client_order_id from request, no fee.
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "OODTGX"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=29119.5,
                client_order_id="019d5d4c-d6ab",
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "OODTGX"
        assert snapshot.client_order_id == "019d5d4c-d6ab"
        assert snapshot.symbol == "BTC-EUR"
        assert snapshot.side == OrderSideEnum.BUY
        assert snapshot.type == ExchangeOrderTypeEnum.LIMIT
        assert snapshot.amount == pytest.approx(0.0001)
        assert snapshot.price == pytest.approx(29119.5)
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.fee is None

    @pytest.mark.asyncio
    async def test_ccxt_passive_fetch(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT fetch of passive open order returns same data.

        Given: A passive limit buy order OODTGX is resting on the book,
        When: get_order is called,
        Then: The snapshot matches the create response exactly.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_PASSIVE_FETCH))
            snapshot = await client.get_order("OODTGX", "BTC-EUR")
        assert snapshot.id == "OODTGX"
        assert snapshot.status == ExchangeOrderStatusEnum.OPEN
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.price == pytest.approx(29119.5)

    @pytest.mark.asyncio
    async def test_ccxt_passive_cancel(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT cancel of passive order returns canceled status.

        Given: A passive limit buy order OODTGX,
        When: cancel_order is called (calls cancel then fetch),
        Then: The snapshot has status=CANCELED with filled=0.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(return_value={"info": {"result": {"count": 1}}})
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_PASSIVE_CANCEL_FETCH))
            snapshot = await client.cancel_order("OODTGX", "BTC-EUR")
        assert snapshot.id == "OODTGX"
        assert snapshot.status == ExchangeOrderStatusEnum.CANCELED
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.client_order_id == "019d5d4c-d6ab"
        mock_ccxt.cancel_order.assert_called_once_with("OODTGX", "BTC/EUR")
        mock_ccxt.fetch_order.assert_called_once_with("OODTGX", "BTC/EUR")

    @pytest.mark.asyncio
    async def test_ccxt_topbook_create_post_only(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT topbook post_only sell creates PENDING snapshot.

        Given: A limit sell at 58239.1 EUR with post_only near top of book,
        When: create_order is called with post_only=True,
        Then: The snapshot is PENDING, unfilled, and postOnly is in CCXT params.
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "ONHBU2"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=58239.1,
                post_only=True,
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "ONHBU2"
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.filled == pytest.approx(0.0)
        call_args = mock_ccxt.create_order.call_args
        params = call_args[0][5]
        assert params["postOnly"] is True

    @pytest.mark.asyncio
    async def test_ccxt_topbook_fetch_filled_as_maker(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT fetch shows topbook order got filled as maker.

        Given: A post_only limit sell ONHBU2 that was matched as maker,
        When: get_order is called,
        Then: The snapshot has status=CLOSED, filled=0.0001, remaining=0.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_TOPBOOK_FETCH_FILLED))
            snapshot = await client.get_order("ONHBU2", "BTC-EUR")
        assert snapshot.id == "ONHBU2"
        assert snapshot.status == ExchangeOrderStatusEnum.CLOSED
        assert snapshot.filled == pytest.approx(0.0001)
        assert snapshot.remaining == pytest.approx(0.0)
        assert snapshot.price == pytest.approx(58239.1)
        assert snapshot.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_ccxt_topbook_cancel_unfilled(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT cancel of unfilled topbook order from different run.

        Given: A topbook limit sell ONE2HB that was not filled,
        When: cancel_order is called,
        Then: The snapshot has status=CANCELED, filled=0.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(return_value={"info": {"result": {"count": 1}}})
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_TOPBOOK_CANCEL_UNFILLED))
            snapshot = await client.cancel_order("ONE2HB", "BTC-EUR")
        assert snapshot.id == "ONE2HB"
        assert snapshot.status == ExchangeOrderStatusEnum.CANCELED
        assert snapshot.filled == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_ccxt_aggressive_create_immediately_filled(
        self, client: KrakenExchangeClient
    ) -> None:
        """Verify CCXT aggressive limit sell create returns PENDING snapshot.

        Given: A limit sell at 58238.0 (crossing the spread),
        When: create_order is called,
        Then: The snapshot has status=PENDING, filled=0, remaining=amount, fee=None
              (create_order always returns PENDING without follow-up fetch).
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "O2CMWJ"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=58238.0,
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "O2CMWJ"
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.fee is None

    @pytest.mark.asyncio
    async def test_ccxt_aggressive_fetch(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT fetch of aggressively filled order is idempotent.

        Given: An immediately filled limit sell O2CMWJ,
        When: get_order is called,
        Then: The snapshot matches the create response.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_AGGRESSIVE_FETCH))
            snapshot = await client.get_order("O2CMWJ", "BTC-EUR")
        assert snapshot.id == "O2CMWJ"
        assert snapshot.status == ExchangeOrderStatusEnum.CLOSED
        assert snapshot.filled == pytest.approx(0.0001)
        assert snapshot.fee == pytest.approx(0.01456)

    @pytest.mark.asyncio
    async def test_ccxt_market_create(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT market sell create returns PENDING snapshot.

        Given: A market sell for 0.0001 BTC,
        When: create_order is called,
        Then: The snapshot has status=PENDING, filled=0, remaining=amount, fee=None.
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "OJWE3F"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.0001,
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "OJWE3F"
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.type == ExchangeOrderTypeEnum.MARKET
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.price is None
        assert snapshot.fee is None

    @pytest.mark.asyncio
    async def test_ccxt_market_fetch(self, client: KrakenExchangeClient) -> None:
        """Verify CCXT fetch of filled market order is idempotent.

        Given: An immediately filled market sell OJWE3F,
        When: get_order is called,
        Then: The snapshot matches the create response.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_MARKET_FETCH))
            snapshot = await client.get_order("OJWE3F", "BTC-EUR")
        assert snapshot.id == "OJWE3F"
        assert snapshot.status == ExchangeOrderStatusEnum.CLOSED
        assert snapshot.type == ExchangeOrderTypeEnum.MARKET
        assert snapshot.fee == pytest.approx(0.02329)

    @pytest.mark.asyncio
    async def test_ccxt_market_fetch_backfills_price_from_average(
        self, client: KrakenExchangeClient
    ) -> None:
        """Verify a filled market order with no price backfills from average.

        Given: A filled market order whose snapshot has ``price=None`` but
            an executed ``average`` (the venue VWAP),
        When: get_order is called,
        Then: The snapshot price is taken from ``average`` so fill-gap
            reconciliation can emit a corrective fill.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_MARKET_AVG_FETCH))
            snapshot = await client.get_order("OAVG01", "BTC-EUR")
        assert snapshot.price == pytest.approx(58000.0)

    @pytest.mark.asyncio
    async def test_ccxt_market_fetch_no_price_no_average_is_none(
        self, client: KrakenExchangeClient
    ) -> None:
        """Verify a market order with neither price nor average stays None.

        Given: A filled market order whose snapshot has ``price=None`` and
            ``average=None``,
        When: get_order is called,
        Then: The snapshot price is None (nothing to backfill from).
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(
                return_value=dict(self.CCXT_MARKET_NOPRICE_NOAVG_FETCH)
            )
            snapshot = await client.get_order("ONOPX1", "BTC-EUR")
        assert snapshot.price is None

    @pytest.mark.asyncio
    async def test_ccxt_market_fetch_average_ignored_when_unfilled(
        self, client: KrakenExchangeClient
    ) -> None:
        """Verify an unfilled market order does not backfill from average.

        Given: An unfilled market order with ``price=None`` but a non-zero
            ``average`` (filled=0),
        When: get_order is called,
        Then: The snapshot price stays None — average is only trusted once
            the order has actually filled.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(
                return_value=dict(self.CCXT_MARKET_AVG_UNFILLED_FETCH)
            )
            snapshot = await client.get_order("OUNFIL", "BTC-EUR")
        assert snapshot.price is None

    @pytest.mark.asyncio
    async def test_ccxt_cancel_inflight_create_then_cancel(
        self, client: KrakenExchangeClient
    ) -> None:
        """Verify CCXT create + immediate cancel flow with real IDs.

        Given: A limit buy OLKARR that is created then immediately canceled,
        When: create_order then cancel_order are called in sequence,
        Then: Create returns PENDING, cancel returns CANCELED.
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "OLKARR"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=29119.5,
            )
            create_snap = await client.create_order(request)
        assert create_snap.id == "OLKARR"
        assert create_snap.status == ExchangeOrderStatusEnum.PENDING

        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(return_value={"info": {"result": {"count": 1}}})
            mock_ccxt.fetch_order = AsyncMock(
                return_value=dict(self.CCXT_CANCEL_INFLIGHT_CANCEL_FETCH)
            )
            cancel_snap = await client.cancel_order("OLKARR", "BTC-EUR")
        assert cancel_snap.id == "OLKARR"
        assert cancel_snap.status == ExchangeOrderStatusEnum.CANCELED
        assert cancel_snap.filled == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_native_passive_create(self, client: KrakenExchangeClient) -> None:
        """Verify native REST path creates order with real fixture data.

        Given: A symbol unsupported by CCXT that falls back to native API,
        When: create_order is called and native Trade client returns a txid,
        Then: The snapshot has status=PENDING with the real order ID.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: BTC-EUR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="XBTEUR",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {"txid": ["OODTGX"]}
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=29119.5,
                client_order_id="019d5d4c-d6ab",
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "OODTGX"
        assert snapshot.symbol == "BTC-EUR"
        assert snapshot.side == OrderSideEnum.BUY
        assert snapshot.type == ExchangeOrderTypeEnum.LIMIT
        assert snapshot.amount == pytest.approx(0.0001)
        assert snapshot.price == pytest.approx(29119.5)
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.client_order_id == "019d5d4c-d6ab"

    @pytest.mark.asyncio
    async def test_native_topbook_create_post_only(self, client: KrakenExchangeClient) -> None:
        """Verify native REST path passes post_only as oflags=post.

        Given: A post_only limit sell via native API fallback,
        When: create_order falls back to native API,
        Then: The native Trade client receives oflags='post'.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: BTC-EUR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="XBTEUR",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {"txid": ["ONHBU2"]}
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=58239.1,
                post_only=True,
            )
            snapshot = await client.create_order(request)
        call_kwargs = mock_trade_client.create_order.call_args.kwargs
        assert call_kwargs["oflags"] == "post"
        assert snapshot.id == "ONHBU2"
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_native_aggressive_create(self, client: KrakenExchangeClient) -> None:
        """Verify native REST path for aggressive limit sell.

        Given: An aggressive limit sell via native API fallback,
        When: create_order falls back to native API,
        Then: The snapshot has PENDING status (native doesn't return fill info).
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: BTC-EUR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="XBTEUR",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {"txid": ["O2CMWJ"]}
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=58238.0,
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "O2CMWJ"
        assert snapshot.side == OrderSideEnum.SELL
        assert snapshot.price == pytest.approx(58238.0)
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_native_market_create(self, client: KrakenExchangeClient) -> None:
        """Verify native REST path for market sell.

        Given: A market sell via native API fallback,
        When: create_order falls back to native API,
        Then: The snapshot has PENDING status with no price.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: BTC-EUR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="XBTEUR",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {"txid": ["OJWE3F"]}
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.0001,
            )
            snapshot = await client.create_order(request)
        assert snapshot.id == "OJWE3F"
        assert snapshot.type == ExchangeOrderTypeEnum.MARKET
        assert snapshot.status == ExchangeOrderStatusEnum.PENDING
        assert snapshot.price is None

    @pytest.mark.asyncio
    async def test_native_cancel_inflight(self, client: KrakenExchangeClient) -> None:
        """Verify native REST path cancel returns CANCELED status.

        Given: An order OLKARR created via native API that needs canceling,
        When: cancel_order falls back to native API,
        Then: The snapshot has status=CANCELED.
        """
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: BTC-EUR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.cancel_order.return_value = {"count": 1}
            snapshot = await client.cancel_order("OLKARR", symbol="BTC-EUR")
        assert snapshot.id == "OLKARR"
        assert snapshot.status == ExchangeOrderStatusEnum.CANCELED
        mock_trade_client.cancel_order.assert_called_once_with(txid="OLKARR")

    @pytest.mark.asyncio
    async def test_ccxt_cancel_does_fetch_cancel_fetch(self, client: KrakenExchangeClient) -> None:
        """Verify cancel_order calls cancel then fetch_order for full data.

        Given: A resting order OODTGX,
        When: cancel_order is called via CCXT path,
        Then: It calls _ccxt_client.cancel_order then _ccxt_client.fetch_order
              and returns full snapshot data from the fetch.
        """
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(return_value={"info": {"result": {"count": 1}}})
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_PASSIVE_CANCEL_FETCH))
            snapshot = await client.cancel_order("OODTGX", "BTC-EUR")
        mock_ccxt.cancel_order.assert_called_once_with("OODTGX", "BTC/EUR")
        mock_ccxt.fetch_order.assert_called_once_with("OODTGX", "BTC/EUR")
        assert snapshot.id == "OODTGX"
        assert snapshot.status == ExchangeOrderStatusEnum.CANCELED
        assert snapshot.side == OrderSideEnum.BUY
        assert snapshot.type == ExchangeOrderTypeEnum.LIMIT
        assert snapshot.amount == pytest.approx(0.0001)
        assert snapshot.price == pytest.approx(29119.5)
        assert snapshot.filled == pytest.approx(0.0)
        assert snapshot.remaining == pytest.approx(0.0001)
        assert snapshot.client_order_id == "019d5d4c-d6ab"

    @pytest.mark.asyncio
    async def test_ccxt_create_then_fetch_lifecycle(self, client: KrakenExchangeClient) -> None:
        """Verify full create-then-fetch lifecycle with real fixture data.

        Given: A passive limit buy created via CCXT,
        When: create_order is followed by get_order,
        Then: Both snapshots share the same id, price, amount, side, and type;
              create returns PENDING, fetch returns the live status from the exchange.
        """
        with (
            patch.object(client, "_ccxt_client") as mock_ccxt,
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            mock_ccxt.create_order = AsyncMock(return_value={"id": "OODTGX"})
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=0.0001,
                price=29119.5,
                client_order_id="019d5d4c-d6ab",
            )
            create_snap = await client.create_order(request)

        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(return_value=dict(self.CCXT_PASSIVE_FETCH))
            fetch_snap = await client.get_order("OODTGX", "BTC-EUR")

        assert create_snap.id == fetch_snap.id
        assert create_snap.status == ExchangeOrderStatusEnum.PENDING
        assert fetch_snap.status == ExchangeOrderStatusEnum.OPEN
        assert create_snap.price == fetch_snap.price
        assert create_snap.amount == fetch_snap.amount
        assert create_snap.side == fetch_snap.side
        assert create_snap.type == fetch_snap.type


class TestEnqueueOrDropOldest:
    """Tests for the ``_enqueue_or_drop_oldest`` bounded-queue helper."""

    def test_enqueue_when_space_available(self) -> None:
        """Enqueue item normally when queue has capacity.

        Given: Queue with maxsize=2 and one existing item,
        When: _enqueue_or_drop_oldest is called with a new item,
        Then: New item is added, queue has two items total.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2)
        queue.put_nowait("first")
        _enqueue_or_drop_oldest(queue, "second", "test")
        assert queue.qsize() == 2

    def test_drops_oldest_when_full(self) -> None:
        """Drop oldest item and enqueue newest when queue is full.

        Given: Queue with maxsize=1 already containing 'old',
        When: _enqueue_or_drop_oldest is called with 'new',
        Then: Queue contains only 'new'.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("old")
        _enqueue_or_drop_oldest(queue, "new", "test")
        assert queue.qsize() == 1
        assert queue.get_nowait() == "new"

    def test_drop_log_is_rate_limited_to_one_per_interval(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Sustained drops emit a single summary line per interval, not per drop.

        Given: A bounded queue at capacity 1 and a freshly-reset counter,
        When: 100 drop-oldest events fire within the same interval,
        Then: At most one warning line lands in the captured log (per-tick
            spam is eliminated; the line carries the count summary).
        """
        kr._drop_counters.clear()
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("seed")
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for i in range(100):
                _enqueue_or_drop_oldest(queue, f"item-{i}", "kraken-spot-tick")
        finally:
            logger.remove(sink_id)
        kr._drop_counters.clear()

        summaries = [
            rec
            for rec in caplog.records
            if rec.levelname == "WARNING" and "kraken-spot-tick queue full" in rec.message
        ]
        assert len(summaries) <= 1
        if summaries:
            assert "drop-oldest backpressure" in summaries[0].message

    def test_drop_log_emits_summary_after_interval(self, caplog: pytest.LogCaptureFixture) -> None:
        """A second summary line lands once ``_DROP_LOG_INTERVAL_S`` has elapsed.

        Given: A counter pre-loaded to a "long ago" last-logged timestamp,
        When: A single drop event fires,
        Then: A warning is emitted (proves the interval gate eventually
            re-opens, not just suppresses everything after the first).
        """
        kr._drop_counters["kraken-spot-tick"] = [0.0, 0.0]
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("seed")
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            _enqueue_or_drop_oldest(queue, "later", "kraken-spot-tick")
        finally:
            logger.remove(sink_id)
        kr._drop_counters.clear()

        summaries = [rec for rec in caplog.records if "queue full, dropped" in rec.message]
        assert len(summaries) == 1


class TestKrakenSpotQueuesAreBounded:
    """Pin the bounded-queue invariant for the Kraken spot client.

    A regression that drops ``maxsize`` would let a slow downstream
    consumer grow the per-queue backlog without bound — exactly the
    risk the finite maxsize bound closes.
    """

    def test_default_queues_have_finite_maxsize(self) -> None:
        """Every WS-facing queue carries a finite (non-zero) maxsize.

        Given: A fresh KrakenExchangeClient,
        When: queue maxsize is read,
        Then: every WS-facing queue carries a finite (non-zero) maxsize.
        ``asyncio.Queue`` defaults ``maxsize=0`` for unbounded — the
        assertion ``maxsize > 0`` is the regression gate.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        assert client._tick_queue.maxsize > 0
        assert client._trade_queue.maxsize > 0
        assert client._execution_queue.maxsize > 0
        assert client._raw_instrument_queue.maxsize > 0
        assert client._instrument_queue.maxsize > 0

    def test_tick_queue_absorbs_boot_burst(self) -> None:
        """Spec — the ticker producer queue is sized for boot-time replay.

        Given: A fresh KrakenExchangeClient,
        When: tick queue maxsize is compared to the standard queue
            bound,
        Then: tick queue maxsize is strictly greater than the
            non-tick ``_QUEUE_MAX_SIZE`` — the Kraken Spot broker
            replays a wildcard ticker snapshot of every subscribed
            pair in the first ~60 s after WS handshake, and the
            2026-05-25 post-restart log review counted 34 drop-oldest
            WARN records on the tick queue alone in that window.
            The larger ``_TICK_QUEUE_MAX_SIZE``
            absorbs the burst without dropping; the smaller default
            applies to ``trade``/``candle``/``instrument``/``execution``
            paths where no drops have been observed.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        assert client._tick_queue.maxsize >= 50_000
        assert client._tick_queue.maxsize == kr._TICK_QUEUE_MAX_SIZE
        assert client._tick_queue.maxsize > client._trade_queue.maxsize
        assert client._trade_queue.maxsize == kr._QUEUE_MAX_SIZE
        client._ensure_candle_queue(60)
        assert client._candle_queues[60].maxsize >= 50_000
        assert client._candle_queues[60].maxsize == kr._TICK_QUEUE_MAX_SIZE


class TestInvokeFuncOffloadsSyncCalls:
    """Pin the event-loop-unblock contract for ``_invoke_func``.

    The CCXT sync API and the native Kraken Trade REST SDK are
    blocking. Routing them through the client-owned bounded REST pool
    means a slow Kraken REST round-trip cannot stall the executor
    coroutine, and cannot saturate the loop's shared default executor
    either (audit P1-5). These tests fail loudly if a regression
    reintroduces a direct sync call on the event-loop thread.
    """

    @pytest.mark.asyncio
    async def test_sync_callable_runs_on_client_pool_thread(self) -> None:
        """Sync callables go through the client's bounded REST pool.

        Given: A KrakenExchangeClient and a sync callable that records
        the OS thread it executes on,
        When: ``_invoke_func(callable)`` is awaited,
        Then: The callable runs off the event-loop thread, on a thread
        carrying the client's ``kraken-rest`` pool prefix — proving the
        dispatch went to the client-owned pool, not the shared default
        executor.
        """
        client = KrakenExchangeClient()
        main_loop = asyncio.get_running_loop()
        main_thread_id = threading.get_ident()
        seen: list[tuple[int, str]] = []

        def sync_call() -> str:
            seen.append((threading.get_ident(), threading.current_thread().name))
            return "ok"

        result = await client._invoke_func(sync_call)
        assert result == "ok"
        assert seen[0][0] != main_thread_id, (
            "sync callables must run on a worker thread (client REST pool),"
            " not the asyncio event loop thread"
        )
        assert seen[0][1].startswith("kraken-rest"), (
            "sync callables must run on the client-owned bounded pool,"
            " not the loop's shared default executor"
        )
        assert main_loop.is_running(), "event loop must still be running after the call"
        client._shutdown_rest_pool()

    @pytest.mark.asyncio
    async def test_async_callable_awaited_directly(self) -> None:
        """Async callables are awaited inline (no thread hop).

        Given: A coroutine function,
        When: ``_invoke_func(coro_func)`` is awaited,
        Then: It runs on the event loop thread (no pool dispatch).
        """
        client = KrakenExchangeClient()
        main_thread_id = threading.get_ident()
        async_thread_id: list[int | None] = []

        async def async_call() -> str:
            async_thread_id.append(threading.get_ident())
            return "ok"

        result = await client._invoke_func(async_call)
        assert result == "ok"
        assert async_thread_id[0] == main_thread_id

    @pytest.mark.asyncio
    async def test_routed_sync_callable_requires_operation_and_target(self) -> None:
        """Routed sync callables require a complete egress target.

        Given: A sync callable marked for Kraken REST egress routing,
        When: operation and proxy target metadata are missing,
        Then: the client raises before dispatching the callable.
        """
        client = KrakenExchangeClient()
        called: list[bool] = []

        def sync_call() -> str:
            called.append(True)
            return "ok"

        with pytest.raises(RuntimeError, match="requires operation and target"):
            await client._invoke_func(sync_call, egress_kind="public_read")

        assert called == []
        client._shutdown_rest_pool()


class TestRestPoolLifecycle:
    """Pin the REST-pool lifecycle contract on the spot client.

    A pool created during a failed ``connect()`` must never outlive the
    failure: ``__aexit__`` only runs after a successful ``__aenter__``,
    so connect itself owns the cleanup. Disconnect must close the pool
    and a same-instance reconnect must reopen it.
    """

    @pytest.mark.asyncio
    async def test_connect_failure_shuts_rest_pool(self) -> None:
        """A failed load_markets leaves no live pool behind.

        Given: A client whose ``_with_retry`` raises,
        When: ``connect()`` is awaited,
        Then: The error propagates and the pool slot is cleared with
        the closed flag set — no stranded worker threads.
        """
        client = KrakenExchangeClient()
        with (
            patch.object(client, "_with_retry", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await client.connect()
        assert client._rest_pool is None
        assert client._rest_pool_closed

    @pytest.mark.asyncio
    async def test_connect_failure_closes_rest_session(self) -> None:
        """A failed connect closes the ccxt REST session.

        Given: A client whose ``_with_retry`` raises,
        When: ``connect()`` is awaited,
        Then: The session is closed — manual ``connect()`` callers
        (publishers, updaters) have no ``__aexit__``/stop path that
        would release it after a failed connect.
        """
        client = KrakenExchangeClient()
        client._ccxt_client = MagicMock()
        with (
            patch.object(client, "_with_retry", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await client.connect()
        client._ccxt_client.session.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_connect_failure_session_close_error_suppressed(self) -> None:
        """A failing session close cannot mask the connect failure.

        Given: A failing connect whose session close also raises,
        When: ``connect()`` is awaited,
        Then: The ORIGINAL connect error propagates.
        """
        client = KrakenExchangeClient()
        client._ccxt_client = MagicMock()
        client._ccxt_client.session.close.side_effect = RuntimeError("already closed")
        with (
            patch.object(client, "_with_retry", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await client.connect()

    @pytest.mark.asyncio
    async def test_connect_failure_without_session_attribute(self) -> None:
        """Connect-failure cleanup tolerates session-less ccxt clients.

        Given: A ccxt client without a ``session`` attribute,
        When: ``connect()`` fails,
        Then: The error propagates and the pool is still shut down.
        """
        client = KrakenExchangeClient()
        client._ccxt_client = SimpleNamespace(load_markets=lambda: None)
        with (
            patch.object(client, "_with_retry", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await client.connect()
        assert client._rest_pool_closed

    @pytest.mark.asyncio
    async def test_connect_failure_shuts_real_pool(self) -> None:
        """The REAL pool built during a failing connect is shut down.

        Given: A ``_with_retry`` that performs a real pool dispatch and
        then fails (models load_markets dying after the pool exists),
        When: ``connect()`` is awaited,
        Then: The captured executor refuses new work — proving actual
        ``shutdown()`` ran, not just flag bookkeeping.
        """
        client = KrakenExchangeClient()
        captured: list[Any] = []

        async def _dispatch_then_fail(fn: Any, *args: Any, **kwargs: Any) -> Any:
            await client._dispatch_blocking(lambda: None)
            captured.append(client._rest_pool)
            raise RuntimeError("boom")

        with (
            patch.object(client, "_with_retry", side_effect=_dispatch_then_fail),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await client.connect()
        assert client._rest_pool is None
        assert client._rest_pool_closed
        with pytest.raises(RuntimeError):
            captured[0].submit(lambda: None)

    @pytest.mark.asyncio
    async def test_connect_cancellation_shuts_rest_pool(self) -> None:
        """A cancelled connect cleans up exactly like a failed one.

        Given: A client whose ``_with_retry`` raises ``CancelledError``,
        When: ``connect()`` is awaited,
        Then: The cancellation propagates (BaseException path) and the
        pool is shut down — crash-restart cycles cannot strand threads.
        """
        client = KrakenExchangeClient()
        with (
            patch.object(client, "_with_retry", side_effect=asyncio.CancelledError()),
            pytest.raises(asyncio.CancelledError),
        ):
            await client.connect()
        assert client._rest_pool is None
        assert client._rest_pool_closed

    @pytest.mark.asyncio
    async def test_disconnect_shuts_pool_and_reconnect_reopens(self) -> None:
        """Disconnect closes the pool; a later connect reopens it.

        Given: A connected client with a live pool,
        When: ``disconnect()`` then ``connect()`` run on the same instance,
        Then: Dispatch fails loudly between the two and works again after
        the reconnect (publisher rebuild cycles reuse instances).
        """
        client = KrakenExchangeClient()
        with patch.object(client, "_with_retry", new_callable=AsyncMock):
            await client.connect()
            await client._dispatch_blocking(lambda: "ok")
            assert client._rest_pool is not None
            await client.disconnect()
            assert client._rest_pool is None
            assert client._rest_pool_closed
            with pytest.raises(RuntimeError, match="REST thread pool is closed"):
                await client._dispatch_blocking(lambda: "ok")
            await client.connect()
            result = await client._dispatch_blocking(lambda: "again")
        assert result == "again"
        client._shutdown_rest_pool()


class TestRawTickerCapture:
    """Tests for the discovery-mode raw ticker capture path.

    The capture path exists so that the Kraken symbol updater can
    discover product-type listings (e.g. ``BTC/USD:BTNL`` Bitnomial
    perpetuals) that don't yet have aliases in the mapper; the regular
    parse pipeline silently drops their frames.
    """

    def test_handle_ticker_data_captures_raw_symbols_when_capture_active(
        self,
    ) -> None:
        """Verify _handle_ticker_data records raw frames into the capture dict.

        Given: A client with ``_raw_ticker_capture`` set to a dict,
        When: ``_handle_ticker_data`` runs with a batch of frames,
        Then: Each distinct wire symbol is recorded as a key mapping
        to the first observed frame.
        """
        client = KrakenExchangeClient()
        capture: dict[str, dict[str, Any]] = {}
        client._raw_ticker_capture = capture
        frames = [
            {"symbol": "BTC/USD:BTNL", "last": 100000.0},
            {"symbol": "ETH/USD", "last": 3500.0},
            {"symbol": "BTC/USD:BTNL", "last": 100001.0},
        ]
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
            return_value=[],
        ):
            client._handle_ticker_data(frames)
        assert set(capture.keys()) == {"BTC/USD:BTNL", "ETH/USD"}
        assert capture["BTC/USD:BTNL"]["last"] == pytest.approx(100000.0)

    def test_handle_ticker_data_skips_non_dict_and_non_string_symbols(
        self,
    ) -> None:
        """Verify capture mode tolerates malformed frames without raising.

        Given: A capture dict and frames with malformed entries,
        When: ``_handle_ticker_data`` runs,
        Then: Only well-formed entries land in the capture; no error
        propagates.
        """
        client = KrakenExchangeClient()
        capture: dict[str, dict[str, Any]] = {}
        client._raw_ticker_capture = capture
        frames = [
            "not-a-dict",
            {"symbol": None},
            {"symbol": "BTC/USD:BTNL"},
            {"no_symbol_key": "x"},
        ]
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
            return_value=[],
        ):
            client._handle_ticker_data(frames)
        assert set(capture.keys()) == {"BTC/USD:BTNL"}

    def test_handle_ticker_data_returns_early_for_non_list_payload(self) -> None:
        """Verify a non-list payload is ignored even with capture active.

        Given: A capture dict and a non-list payload,
        When: ``_handle_ticker_data`` runs,
        Then: Capture stays empty (no entries added).
        """
        client = KrakenExchangeClient()
        capture: dict[str, dict[str, Any]] = {}
        client._raw_ticker_capture = capture
        client._handle_ticker_data({"symbol": "BTC/USD:BTNL"})
        assert capture == {}

    def test_handle_ticker_data_skips_parse_pipeline_when_capture_active(
        self,
    ) -> None:
        """Verify parse_kraken_ticker_list is NOT invoked during capture.

        Given: A capture dict is set on the client,
        When: ``_handle_ticker_data`` runs with a valid frame list,
        Then: The parse pipeline is skipped (no warnings for stale
        mapper, no wasted enqueue on ``_tick_queue``). The capture is
        the only side effect during discovery.
        """
        client = KrakenExchangeClient()
        capture: dict[str, dict[str, Any]] = {}
        client._raw_ticker_capture = capture
        frames = [{"symbol": "GAS/EUR", "last": 1.5}]
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
        ) as mock_parse:
            client._handle_ticker_data(frames)
            mock_parse.assert_not_called()
        assert capture == {"GAS/EUR": {"symbol": "GAS/EUR", "last": 1.5}}
        assert client._tick_queue.qsize() == 0

    def test_handle_ticker_data_runs_parse_pipeline_when_no_capture(self) -> None:
        """Verify parse_kraken_ticker_list IS invoked when capture is inactive.

        Given: A client with ``_raw_ticker_capture`` set to ``None``
        (the production publisher path),
        When: ``_handle_ticker_data`` runs,
        Then: The parse pipeline runs and successful parses land on
        ``_tick_queue`` exactly as when capture mode is inactive.
        """
        client = KrakenExchangeClient()
        assert client._raw_ticker_capture is None
        sentinel = MagicMock()
        frames = [{"symbol": "BTC/USD", "last": 100000.0}]
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
            return_value=[sentinel],
        ) as mock_parse:
            client._handle_ticker_data(frames)
            mock_parse.assert_called_once_with(frames)
        assert client._tick_queue.qsize() == 1

    @pytest.mark.asyncio
    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_collect_raw_ticker_symbols_returns_capture_after_sleep(
        self,
        mock_ws_class: MagicMock,
    ) -> None:
        """Verify collect_raw_ticker_symbols subscribes and returns the capture.

        Given: A mocked WS client and ``asyncio.sleep`` patched to a noop,
        When: ``collect_raw_ticker_symbols`` is awaited,
        Then: The wildcard ticker subscription is issued and the capture
        dict (populated by the handler during the window) is returned.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)

        async def fake_sleep(_duration: float) -> None:
            assert client._raw_ticker_capture is not None
            client._handle_ticker_data(
                [
                    {"symbol": "BTC/USD:BTNL"},
                    {"symbol": "ETH/USD:BTNL"},
                ]
            )

        with (
            patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                side_effect=fake_sleep,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
                return_value=[],
            ),
        ):
            client._ws_client = mock_ws_client
            captured = await client.collect_raw_ticker_symbols(window_seconds=0.5)
        assert set(captured.keys()) == {"BTC/USD:BTNL", "ETH/USD:BTNL"}
        mock_ws_client.subscribe.assert_called_once()
        assert client._raw_ticker_capture is None

    @pytest.mark.asyncio
    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_collect_raw_ticker_symbols_emits_periodic_progress_log(
        self,
        mock_ws_class: MagicMock,
    ) -> None:
        """Verify the periodic progress log fires inside the discovery loop.

        Given: A ``window_seconds`` larger than ``progress_interval``
        (15s), so the loop iterates more than once,
        When: ``collect_raw_ticker_symbols`` is awaited,
        Then: A "X symbols seen so far" progress message is emitted
        from at least one non-final iteration (so operators see the
        updater is still alive during the long discovery window).
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock),
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch("snapper.infrastructure.exchanges.implementations.kraken.logger") as mock_logger,
        ):
            client._ws_client = mock_ws_client
            await client.collect_raw_ticker_symbols(window_seconds=20.0)
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any(
            "symbols seen so far" in c for c in info_calls
        ), f"expected periodic progress log, got: {info_calls}"

    @pytest.mark.asyncio
    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_collect_raw_ticker_symbols_restores_previous_capture_on_error(
        self,
        mock_ws_class: MagicMock,
    ) -> None:
        """Verify _raw_ticker_capture is restored even when the WS raises.

        Given: A pre-existing outer capture dict + a WS subscribe that throws,
        When: ``collect_raw_ticker_symbols`` is awaited,
        Then: The original outer capture dict is restored after the call
        unwinds (no nested-capture leak).
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        outer_capture: dict[str, dict[str, Any]] = {"outer": {"symbol": "outer"}}
        client._raw_ticker_capture = outer_capture
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.subscribe = AsyncMock(side_effect=RuntimeError("boom"))
        with (
            patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="boom"),
        ):
            client._ws_client = mock_ws_client
            await client.collect_raw_ticker_symbols(window_seconds=0.0)
        assert client._raw_ticker_capture is outer_capture


class TestConnectRaceHardening:
    """Spot parity tests for the futures client's connect-race hardening."""

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_coalesces_concurrent_callers(self) -> None:
        """Concurrent connect callers build exactly one SpotWSClient.

        Given: Two concurrent _ensure_ws_connected callers and a slow start,
        When: Both run,
        Then: Only one client is constructed — the second caller parks on
            the connect lock and adopts the first one's client.
        """
        client = KrakenExchangeClient(api_key="key", api_secret="secret")
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_start() -> None:
            started.set()
            await release.wait()

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as ws_cls:
            ws_cls.return_value.start = _slow_start
            ws_cls.return_value.exception_occur = False
            first = asyncio.create_task(client._ensure_ws_connected())
            await started.wait()
            second = asyncio.create_task(client._ensure_ws_connected())
            await asyncio.sleep(0.01)
            release.set()
            await asyncio.gather(first, second)
        assert ws_cls.call_count == 1
        assert client._ws_connected is True

    @pytest.mark.asyncio
    async def test_close_ws_client_does_not_clobber_newer_client(self) -> None:
        """_close_ws_client leaves a newer concurrently-installed client alone.

        Given: A close() during which a concurrent path installs a NEW client
            and marks it connected,
        When: _close_ws_client finishes,
        Then: The new client remains in the slot (compare-and-clear) and its
            connected flag survives — the losing closer must not stamp the
            freshly built connection as disconnected.
        """
        client = KrakenExchangeClient(api_key="key", api_secret="secret")
        newer = AsyncMock()
        old = AsyncMock()

        async def _close_and_install() -> None:
            client._ws_client = newer
            client._ws_connected = True

        old.close = AsyncMock(side_effect=_close_and_install)
        client._ws_client = old
        await client._close_ws_client()
        assert client._ws_client is newer
        assert client._ws_connected is True

    @pytest.mark.asyncio
    async def test_replay_aborts_when_client_swapped_mid_replay(self) -> None:
        """Spot replay raises when the client slot is swapped between chunks.

        Given: Two cached chunk requests and a subscribe that swaps the slot,
        When: _replay_subscriptions runs,
        Then: It raises after the first chunk instead of aiming the rest of
            the replay at the wrong connection.
        """
        client = KrakenExchangeClient(api_key="key", api_secret="secret")
        ws = AsyncMock()

        async def _swap(**_kwargs: Any) -> None:
            client._ws_client = AsyncMock()

        ws.subscribe = AsyncMock(side_effect=_swap)
        client._ws_client = ws
        first = SubscriptionRequest(channel="trade", symbols=("BTC/USD",), parameters_json="{}")
        second = SubscriptionRequest(channel="trade", symbols=("ETH/USD",), parameters_json="{}")
        client._subscription_cache[first.key()] = first
        client._subscription_cache[second.key()] = second
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(RuntimeError, match="replaced during subscription replay"),
        ):
            await client._replay_subscriptions()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_detects_ownership_loss(self) -> None:
        """A spot connect raced by a disconnect closes its own client.

        Given: An empty replay cache and a start() during which a concurrent
            path nulls the slot,
        When: _ensure_ws_connected finishes starting,
        Then: It raises the ownership-lost error and closes the DISOWNED
            client directly (the slot-scoped _close_ws_client would be a
            no-op on the emptied slot, leaking the started client).
        """
        client = KrakenExchangeClient(api_key="key", api_secret="secret")

        async def _start_while_disconnected() -> None:
            client._ws_client = None

        with (
            patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient") as ws_cls,
            patch.object(client, "_close_ws_client", new_callable=AsyncMock) as slot_close,
        ):
            ws_cls.return_value.start = _start_while_disconnected
            ws_cls.return_value.close = AsyncMock()
            with pytest.raises(RuntimeError, match="replaced during connect"):
                await client._ensure_ws_connected()
            ws_cls.return_value.close.assert_awaited_once()
            slot_close.assert_not_awaited()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disowned_close_error_is_swallowed(self) -> None:
        """A failing disowned close never masks the ownership error.

        Given: An ownership-lost connect whose direct close also raises,
        When: _ensure_ws_connected tears down,
        Then: The close error is swallowed and the ownership RuntimeError
            propagates.
        """
        client = KrakenExchangeClient(api_key="key", api_secret="secret")

        async def _start_while_disconnected() -> None:
            client._ws_client = None

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as ws_cls:
            ws_cls.return_value.start = _start_while_disconnected
            ws_cls.return_value.close = AsyncMock(side_effect=RuntimeError("close fail"))
            with pytest.raises(RuntimeError, match="replaced during connect"):
                await client._ensure_ws_connected()


class TestFindOrderByClientId:
    """Spot client-id verification lookups for ambiguous submits."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def _ccxt_order(self, client_order_id: str) -> dict[str, Any]:
        """Build a minimal ccxt order dict echoing the client id."""
        return {
            "id": "OID-1",
            "clientOrderId": client_order_id,
            "symbol": "BTC/USD",
            "side": "buy",
            "type": "market",
            "amount": 0.1,
            "filled": 0.0,
            "remaining": 0.1,
            "status": "open",
            "timestamp": 1718000000000,
        }

    @pytest.mark.asyncio
    async def test_found_in_open_orders(self, kraken_client: KrakenExchangeClient) -> None:
        """An open order with the client id resolves without the closed query.

        Given: fetch_open_orders returning the order,
        When: find_order_by_client_id is called,
        Then: The snapshot is returned, the clientOrderId param reached
            the venue call, and fetch_closed_orders is never queried.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = [self._ccxt_order("cid-find-1")]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            snapshot = await kraken_client.find_order_by_client_id("cid-find-1", "BTC-USD")
        assert snapshot is not None
        assert snapshot.id == "OID-1"
        params = mock_client.fetch_open_orders.call_args.args[3]
        assert params == {"clientOrderId": "cid-find-1"}
        mock_client.fetch_closed_orders.assert_not_called()

    @pytest.mark.asyncio
    async def test_found_in_closed_orders(self, kraken_client: KrakenExchangeClient) -> None:
        """A terminal order surfaces through the closed-orders query.

        Given: An empty open list and the order in the closed list,
        When: find_order_by_client_id is called,
        Then: The snapshot is returned from the second query.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = []
        mock_client.fetch_closed_orders.return_value = [self._ccxt_order("cid-find-2")]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            snapshot = await kraken_client.find_order_by_client_id("cid-find-2", "BTC-USD")
        assert snapshot is not None
        assert snapshot.id == "OID-1"

    @pytest.mark.asyncio
    async def test_absent_in_both_returns_none(self, kraken_client: KrakenExchangeClient) -> None:
        """None is returned only after BOTH queries succeed empty.

        Given: Open and closed queries both succeeding with no match,
        When: find_order_by_client_id is called,
        Then: None signals authoritative absence.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = []
        mock_client.fetch_closed_orders.return_value = []
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            snapshot = await kraken_client.find_order_by_client_id("cid-find-3", "BTC-USD")
        assert snapshot is None
        mock_client.fetch_closed_orders.assert_called_once()

    @pytest.mark.asyncio
    async def test_mismatched_echo_is_not_a_match(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A venue ignoring the filter cannot produce a false positive.

        Given: Queries returning orders whose echoed client id differs,
        When: find_order_by_client_id is called,
        Then: None is returned — only an exact echo counts as found.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = [self._ccxt_order("other-cid")]
        mock_client.fetch_closed_orders.return_value = [self._ccxt_order("another-cid")]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            snapshot = await kraken_client.find_order_by_client_id("cid-find-4", "BTC-USD")
        assert snapshot is None

    @pytest.mark.asyncio
    async def test_query_failure_propagates(self, kraken_client: KrakenExchangeClient) -> None:
        """A failing venue query raises instead of claiming absence.

        Given: fetch_open_orders raising a network error,
        When: find_order_by_client_id is called,
        Then: The error propagates (could-not-verify, never absence).
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.side_effect = ccxt.NetworkError("down")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken._retry_sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(ccxt.NetworkError),
        ):
            await kraken_client.find_order_by_client_id("cid-find-5", "BTC-USD")

    @pytest.mark.asyncio
    async def test_requires_credentials(self) -> None:
        """Missing credentials raise before any venue call."""
        client = KrakenExchangeClient(api_key="", api_secret="", sandbox=False)
        with pytest.raises(RuntimeError, match="API credentials"):
            await client.find_order_by_client_id("cid-find-6", "BTC-USD")

    @pytest.mark.asyncio
    async def test_non_list_order_set_raises(self, kraken_client: KrakenExchangeClient) -> None:
        """A non-list venue answer is could-not-verify, not absence.

        Given: fetch_open_orders returning None instead of a list,
        When: find_order_by_client_id is called,
        Then: RuntimeError propagates — a defensive guard so a
            misbehaving transport can never convert into an
            authoritative-absence answer.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = None
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            pytest.raises(RuntimeError, match="non-list order set"),
        ):
            await kraken_client.find_order_by_client_id("cid-find-7", "BTC-USD")

    @pytest.mark.asyncio
    async def test_no_symbol_queries_ccxt_unfiltered(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A symbol-less lookup stays on the CCXT path with no pair filter.

        Given: No symbol argument and both queries succeeding empty,
        When: find_order_by_client_id is called,
        Then: The CCXT fetchers run with a None symbol and None is the
            authoritative-absence answer.
        """
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = []
        mock_client.fetch_closed_orders.return_value = []
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            snapshot = await kraken_client.find_order_by_client_id("cid-find-8")
        assert snapshot is None
        assert mock_client.fetch_open_orders.call_args.args[0] is None


class TestFindOrderByClientIdNativeFallback:
    """Native cl_ord_id verification for CCXT-unmapped (native-only) symbols.

    P0-1 slice 5: a symbol without a CCXT mapping used to raise
    ``ValueError`` out of the verifier (entry stayed parked forever);
    it now falls back to the native ``kraken.spot.User`` open/closed
    endpoints under the same authority contract as the CCXT path.
    """

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide a client whose symbol has no CCXT mapping."""
        client = KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )
        return client

    def _native_order(self, client_order_id: str, status: str = "open") -> dict[str, Any]:
        """Build a native open/closed-orders entry echoing the client id."""
        return {
            "cl_ord_id": client_order_id,
            "status": status,
            "vol": "0.25",
            "vol_exec": "0.10",
            "price": "104.50",
            "fee": "0.20",
            "oflags": "fciq",
            "opentm": 1718000000.5,
            "descr": {
                "pair": "FOOEUR",
                "type": "sell",
                "ordertype": "stop-loss",
                "price": "105.00",
            },
        }

    def _patched(self, kraken_client: KrakenExchangeClient, user_mock: MagicMock) -> tuple[
        contextlib.AbstractContextManager[MagicMock],
        contextlib.AbstractContextManager[MagicMock],
    ]:
        """Patch the symbol mapper to native-only and inject the User client."""
        return (
            patch.object(kr, "native_to_ccxt", side_effect=ValueError("Unknown native symbol")),
            patch.object(kraken_client, "_user_client", user_mock),
        )

    @pytest.mark.asyncio
    async def test_found_in_native_open_orders(self, kraken_client: KrakenExchangeClient) -> None:
        """An open native order resolves with full field fidelity.

        Given: The native open-orders answer containing the order,
        When: find_order_by_client_id is called with a native-only symbol,
        Then: The snapshot carries txid, echoed client id, the requested
            native symbol, the RAW stop-loss ordertype, the executed
            average as price (descr.price is the TRIGGER on stop types,
            never a usable price), and the cl_ord_id filter reached the
            venue call; the closed endpoint and the CCXT client are
            never queried.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TXID-N1": self._native_order("cid-n1")}}
        ccxt_mock = AsyncMock()
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, patch.object(kraken_client, "_ccxt_client", ccxt_mock):
            snapshot = await kraken_client.find_order_by_client_id("cid-n1", "FOO-EUR")
        assert snapshot is not None
        assert snapshot.id == "TXID-N1"
        assert snapshot.client_order_id == "cid-n1"
        assert snapshot.symbol == "FOO-EUR"
        assert snapshot.side == OrderSideEnum.SELL
        assert snapshot.type == ExchangeOrderTypeEnum.STOP_LOSS
        assert snapshot.status == ExchangeOrderStatusEnum.OPEN
        assert snapshot.amount == pytest.approx(0.25)
        assert snapshot.filled == pytest.approx(0.10)
        assert snapshot.remaining == pytest.approx(0.15)
        assert snapshot.price == pytest.approx(104.50)
        assert snapshot.fee == pytest.approx(0.20)
        assert snapshot.fee_currency == "EUR"
        assert snapshot.timestamp == pytest.approx(1718000000.5)
        assert user_mock.get_open_orders.call_args.kwargs["extra_params"] == {"cl_ord_id": "cid-n1"}
        user_mock.get_closed_orders.assert_not_called()
        ccxt_mock.fetch_open_orders.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_limit_uses_price2_leg(self, kraken_client: KrakenExchangeClient) -> None:
        """A stop-loss-limit order takes its price from the price2 leg.

        Given: An open stop-loss-limit whose descr carries trigger
            price=105 and limit leg price2=104,
        When: find_order_by_client_id is called,
        Then: The snapshot price is the executable limit leg, never the
            trigger.
        """
        payload = self._native_order("cid-n10")
        descr = dict(payload["descr"])
        descr["ordertype"] = "stop-loss-limit"
        descr["price2"] = "104.00"
        payload["descr"] = descr
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TXID-N10": payload}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch:
            snapshot = await kraken_client.find_order_by_client_id("cid-n10", "FOO-EUR")
        assert snapshot is not None
        assert snapshot.type == ExchangeOrderTypeEnum.STOP_LOSS_LIMIT
        assert snapshot.price == pytest.approx(104.00)

    @pytest.mark.asyncio
    async def test_found_in_native_closed_orders_market_defaults(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A descr-less closed order falls back to executed-average pricing.

        Given: An empty open answer and a closed order without a descr
            block (no limit price, no ordertype) but with executed volume,
        When: find_order_by_client_id is called,
        Then: The snapshot uses the average price, the LIMIT type
            fallback, a wall-clock timestamp, BUY side default, and no fee.
        """
        payload = {
            "cl_ord_id": "cid-n2",
            "status": "closed",
            "vol": "1.0",
            "vol_exec": "1.0",
            "price": "99.5",
        }
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {}}
        user_mock.get_closed_orders.return_value = {"closed": {"TXID-N2": payload}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch:
            snapshot = await kraken_client.find_order_by_client_id("cid-n2", "FOO-EUR")
        assert snapshot is not None
        assert snapshot.status == ExchangeOrderStatusEnum.CLOSED
        assert snapshot.type == ExchangeOrderTypeEnum.LIMIT
        assert snapshot.side == OrderSideEnum.BUY
        assert snapshot.price == pytest.approx(99.5)
        assert snapshot.fee is None
        assert snapshot.timestamp > 1718000000.5
        assert user_mock.get_closed_orders.call_args.kwargs["extra_params"] == {
            "cl_ord_id": "cid-n2"
        }

    @pytest.mark.asyncio
    async def test_unfilled_priceless_order_has_none_price(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """An unfilled order without a limit price keeps price=None.

        Given: An open market order with zero executed volume and no
            limit price,
        When: find_order_by_client_id is called,
        Then: The snapshot price is None rather than a fabricated zero.
        """
        payload = {
            "cl_ord_id": "cid-n3",
            "status": "open",
            "vol": "1.0",
            "vol_exec": "0",
            "price": "0.00000",
            "opentm": 1718000001.0,
            "descr": {"type": "buy", "ordertype": "market"},
        }
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TXID-N3": payload}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch:
            snapshot = await kraken_client.find_order_by_client_id("cid-n3", "FOO-EUR")
        assert snapshot is not None
        assert snapshot.price is None
        assert snapshot.type == ExchangeOrderTypeEnum.MARKET

    @pytest.mark.asyncio
    async def test_absent_in_both_returns_none(self, kraken_client: KrakenExchangeClient) -> None:
        """None is returned only after BOTH native queries succeed empty.

        Given: Open and closed answers both well-formed and empty,
        When: find_order_by_client_id is called,
        Then: None signals authoritative absence and both endpoints ran.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {}}
        user_mock.get_closed_orders.return_value = {"closed": {}, "count": 0}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch:
            snapshot = await kraken_client.find_order_by_client_id("cid-n4", "FOO-EUR")
        assert snapshot is None
        user_mock.get_closed_orders.assert_called_once()

    @pytest.mark.asyncio
    async def test_paged_closed_answer_cannot_prove_absence(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A closed-orders page smaller than count refuses to claim absence.

        Given: A closed answer whose count exceeds the returned rows
            (the venue paged or ignored the cl_ord_id filter) and no
            entry matching the client id,
        When: find_order_by_client_id is called,
        Then: RuntimeError propagates — a non-exhaustive answer may hide
            the order, so it must never read as venue-confirmed absence.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {}}
        user_mock.get_closed_orders.return_value = {
            "closed": {"TX-B": self._native_order("other-cid", status="closed")},
            "count": 2,
        }
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(RuntimeError, match="provably exhaustive"):
            await kraken_client.find_order_by_client_id("cid-n11", "FOO-EUR")

    @pytest.mark.asyncio
    async def test_mismatched_echo_is_not_a_match(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A venue ignoring the cl_ord_id filter cannot false-match.

        Given: Both answers containing orders with a different echoed
            client id,
        When: find_order_by_client_id is called,
        Then: None is returned — only an exact echo counts as found.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TX-A": self._native_order("other-cid")}}
        user_mock.get_closed_orders.return_value = {
            "closed": {"TX-B": self._native_order("another-cid", status="closed")},
            "count": 1,
        }
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch:
            snapshot = await kraken_client.find_order_by_client_id("cid-n5", "FOO-EUR")
        assert snapshot is None

    @pytest.mark.asyncio
    async def test_countless_closed_answer_cannot_prove_absence(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A closed answer without a count field refuses to claim absence.

        Given: A well-formed but count-less closed answer with no match
            (a shape anomaly — Kraken always returns count),
        When: find_order_by_client_id is called,
        Then: RuntimeError propagates — exhaustiveness cannot be proven.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {}}
        user_mock.get_closed_orders.return_value = {"closed": {}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(RuntimeError, match="provably exhaustive"):
            await kraken_client.find_order_by_client_id("cid-n14", "FOO-EUR")

    def test_native_fee_currency_resolution(self) -> None:
        """Oflags drive fee currency exactly like the vendored ccxt parser."""
        assert kr._native_fee_currency("FOO-EUR", "fciq,post") == "EUR"
        assert kr._native_fee_currency("FOO-EUR", "fcib") == "FOO"
        assert kr._native_fee_currency("FOO-EUR", "post") is None
        assert kr._native_fee_currency("WEIRD", "fciq") is None

    @pytest.mark.asyncio
    async def test_malformed_orders_set_raises(self, kraken_client: KrakenExchangeClient) -> None:
        """A non-dict orders set is could-not-verify, never absence.

        Given: get_open_orders returning a dict whose open member is a list,
        When: find_order_by_client_id is called,
        Then: RuntimeError propagates instead of an absence claim.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": ["not-a-dict"]}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(RuntimeError, match="malformed open-orders"):
            await kraken_client.find_order_by_client_id("cid-n6", "FOO-EUR")

    @pytest.mark.asyncio
    async def test_malformed_order_entry_raises(self, kraken_client: KrakenExchangeClient) -> None:
        """A non-dict order entry is could-not-verify, never absence.

        Given: A well-formed open set whose entry payload is a string,
        When: find_order_by_client_id is called,
        Then: RuntimeError propagates instead of skipping the entry.
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TX-X": "garbage"}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with (
            mapper_patch,
            user_patch,
            pytest.raises(RuntimeError, match="malformed open order entry"),
        ):
            await kraken_client.find_order_by_client_id("cid-n7", "FOO-EUR")

    @pytest.mark.asyncio
    async def test_unknown_status_raises(self, kraken_client: KrakenExchangeClient) -> None:
        """An uninterpretable status raises instead of guessing.

        Given: A matched order whose status is outside the known map,
        When: find_order_by_client_id is called,
        Then: KeyError propagates — could-not-verify, never a fabricated
            state.
        """
        payload = self._native_order("cid-n8", status="mystery")
        user_mock = MagicMock()
        user_mock.get_open_orders.return_value = {"open": {"TX-Y": payload}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(KeyError):
            await kraken_client.find_order_by_client_id("cid-n8", "FOO-EUR")

    @pytest.mark.asyncio
    async def test_query_failure_propagates(self, kraken_client: KrakenExchangeClient) -> None:
        """A failing native query raises instead of claiming absence.

        Given: get_open_orders raising a transport error,
        When: find_order_by_client_id is called,
        Then: The error propagates (could-not-verify, never absence).
        """
        user_mock = MagicMock()
        user_mock.get_open_orders.side_effect = requests.exceptions.ConnectionError("down")
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(requests.exceptions.ConnectionError):
            await kraken_client.find_order_by_client_id("cid-n9", "FOO-EUR")

    def test_get_user_client_requires_credentials(self) -> None:
        """Missing credentials raise before any native client is built."""
        client = KrakenExchangeClient(api_key="", api_secret="", sandbox=False)
        with pytest.raises(RuntimeError, match="API credentials"):
            client._get_user_client()

    def test_get_user_client_is_cached(self, kraken_client: KrakenExchangeClient) -> None:
        """The native User client is built once and reused."""
        sentinel = MagicMock()
        with patch.object(kr, "User", return_value=sentinel) as user_ctor:
            first = kraken_client._get_user_client()
            second = kraken_client._get_user_client()
        assert first is sentinel
        assert second is sentinel
        user_ctor.assert_called_once_with(key="test_key", secret="test_secret")

    @pytest.mark.asyncio
    async def test_get_order_native_fallback(self, kraken_client: KrakenExchangeClient) -> None:
        """get_order re-fetches native-only orders through orders-info.

        Given: A native-only symbol (no CCXT mapping) and an orders-info
            answer keyed by the requested txid,
        When: get_order is called,
        Then: The snapshot comes from the native converter with the
            requested symbol stamped, and the CCXT fetch_order path is
            never used — post-adoption reconciliation of natively
            adopted orders cannot wedge on the symbol mapping.
        """
        payload = self._native_order("cid-n12", status="closed")
        user_mock = MagicMock()
        user_mock.get_orders_info.return_value = {"TXID-N12": payload}
        ccxt_mock = AsyncMock()
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, patch.object(kraken_client, "_ccxt_client", ccxt_mock):
            snapshot = await kraken_client.get_order("TXID-N12", "FOO-EUR")
        assert snapshot.id == "TXID-N12"
        assert snapshot.symbol == "FOO-EUR"
        assert snapshot.status == ExchangeOrderStatusEnum.CLOSED
        assert user_mock.get_orders_info.call_args.args == ("TXID-N12",)
        ccxt_mock.fetch_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_native_malformed_answer_raises(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A malformed orders-info answer raises instead of guessing.

        Given: An orders-info answer lacking the requested txid entry,
        When: get_order is called for a native-only symbol,
        Then: RuntimeError propagates (could-not-interpret).
        """
        user_mock = MagicMock()
        user_mock.get_orders_info.return_value = {"SOMETHING-ELSE": {}}
        mapper_patch, user_patch = self._patched(kraken_client, user_mock)
        with mapper_patch, user_patch, pytest.raises(RuntimeError, match="malformed orders-info"):
            await kraken_client.get_order("TXID-N13", "FOO-EUR")


class TestStopOrderTranslation:
    """Spot stop-order translation at the venue boundary (#156).

    The ccxt path passes the BASE type plus ``stopLossPrice`` and lets
    the pinned ccxt 4.5.57 kraken ``order_request`` derive the AddOrder
    ``ordertype``/``price``/``price2``; the native fallback maps the
    documented AddOrder fields directly (price = trigger, price2 =
    limit leg).
    """

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def _stop_request(
        self,
        order_type: ExchangeOrderTypeEnum,
        *,
        price: float | None = None,
        stop_price: float | None = 48000.0,
    ) -> ExchangeOrderRequest:
        """Build a stop-typed order request."""
        return ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.SELL,
            type=order_type,
            amount=0.25,
            price=price,
            stop_price=stop_price,
            client_order_id="cid-stop-spot",
        )

    @pytest.mark.asyncio
    async def test_ccxt_stop_loss_sends_market_type_with_trigger_param(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A stop-loss submit hands ccxt the base market type plus trigger.

        Given: a stop-loss request with a trigger,
        When: _create_order_via_ccxt runs,
        Then: ccxt create_order receives type 'market', price None and
            params.stopLossPrice — the wire value is never passed as
            the ccxt type (ccxt would skip the trigger wiring).
        """
        mock_client = AsyncMock()
        mock_client.create_order.return_value = {"id": "OID-stop-1"}
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            order = await kraken_client.create_order(
                self._stop_request(ExchangeOrderTypeEnum.STOP_LOSS)
            )
        assert order.id == "OID-stop-1"
        args = mock_client.create_order.call_args[0]
        assert args[1] == "market"
        assert args[4] is None
        assert args[5]["stopLossPrice"] == 48000.0

    @pytest.mark.asyncio
    async def test_ccxt_stop_loss_limit_sends_limit_type_with_trigger_param(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A stop-loss-limit submit keeps the limit leg as the price arg.

        Given: a stop-loss-limit request with trigger and limit leg,
        When: _create_order_via_ccxt runs,
        Then: ccxt create_order receives type 'limit', the limit leg as
            price, and the trigger via params.stopLossPrice.
        """
        mock_client = AsyncMock()
        mock_client.create_order.return_value = {"id": "OID-stop-2"}
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await kraken_client.create_order(
                self._stop_request(ExchangeOrderTypeEnum.STOP_LOSS_LIMIT, price=47900.0)
            )
        args = mock_client.create_order.call_args[0]
        assert args[1] == "limit"
        assert args[4] == 47900.0
        assert args[5]["stopLossPrice"] == 48000.0

    def test_pinned_ccxt_translates_trigger_into_addorder_payload(self) -> None:
        """The pinned ccxt build produces the documented AddOrder payload.

        Given: an OFFLINE ccxt.kraken instance with a seeded market,
        When: order_request translates base type + stopLossPrice exactly
            as our _create_order_via_ccxt passes them,
        Then: the AddOrder request carries ordertype stop-loss-limit /
            stop-loss with price = TRIGGER and price2 = limit leg — the
            decision rule pin for #156; a ccxt upgrade that changes this
            translation fails here, not in production.
        """
        client = ccxt.kraken()
        client.set_markets(
            [
                {
                    "id": "XXBTZUSD",
                    "symbol": "BTC/USD",
                    "base": "BTC",
                    "quote": "USD",
                    "baseId": "XXBT",
                    "quoteId": "ZUSD",
                    "active": True,
                    "type": "spot",
                    "spot": True,
                    "precision": {"amount": 1e-8, "price": 0.1},
                    "limits": {
                        "amount": {"min": None, "max": None},
                        "price": {"min": None, "max": None},
                    },
                    "darkpool": False,
                }
            ]
        )
        base_request = {
            "pair": "XXBTZUSD",
            "type": "sell",
            "ordertype": "limit",
            "volume": client.amount_to_precision("BTC/USD", 0.25),
        }
        translated, leftover = client.order_request(
            "createOrder",
            "BTC/USD",
            "limit",
            base_request,
            0.25,
            47900.0,
            {"stopLossPrice": 48000.0},
        )
        assert translated["ordertype"] == "stop-loss-limit"
        assert translated["price"] == "48000"
        assert translated["price2"] == "47900"
        assert "stopLossPrice" not in leftover
        market_request = {
            "pair": "XXBTZUSD",
            "type": "sell",
            "ordertype": "market",
            "volume": client.amount_to_precision("BTC/USD", 0.25),
        }
        translated_stop, _ = client.order_request(
            "createOrder",
            "BTC/USD",
            "market",
            market_request,
            0.25,
            None,
            {"stopLossPrice": 48000.0},
        )
        assert translated_stop["ordertype"] == "stop-loss"
        assert translated_stop["price"] == "48000"
        assert "price2" not in translated_stop

    @pytest.mark.asyncio
    async def test_stop_without_trigger_rejected_before_any_send(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A stop request without stop_price never reaches ccxt.

        Given: a stop-loss request whose trigger is missing,
        When: create_order runs,
        Then: a pre-send ValueError surfaces and ccxt is never called —
            provably-not-placed, so the executor may definitively reject.
        """
        mock_client = AsyncMock()
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            pytest.raises(ValueError, match="requires stop_price"),
        ):
            await kraken_client.create_order(
                self._stop_request(ExchangeOrderTypeEnum.STOP_LOSS, stop_price=None)
            )
        mock_client.create_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_native_stop_loss_limit_maps_trigger_and_limit_leg(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """The native AddOrder fallback uses price=trigger, price2=limit.

        Given: a native-path stop-loss-limit request,
        When: _create_order_via_native runs,
        Then: the Trade API call carries ordertype stop-loss-limit with
            the documented price/price2 mapping.
        """
        trade_client = MagicMock()
        trade_client.create_order.return_value = {"txid": ["OID-native-1"], "descr": {}}
        with (
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await kraken_client._create_order_via_native(
                self._stop_request(ExchangeOrderTypeEnum.STOP_LOSS_LIMIT, price=47900.0)
            )
        kwargs = trade_client.create_order.call_args.kwargs
        assert kwargs["ordertype"] == "stop-loss-limit"
        assert kwargs["price"] == "48000.0"
        assert kwargs["price2"] == "47900.0"

    @pytest.mark.asyncio
    async def test_native_stop_loss_omits_limit_leg(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A plain native stop carries only the trigger.

        Given: a native-path stop-loss request without a limit leg,
        When: _create_order_via_native runs,
        Then: price is the trigger and no price2 is sent.
        """
        trade_client = MagicMock()
        trade_client.create_order.return_value = {"txid": ["OID-native-2"], "descr": {}}
        with (
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
            patch.object(
                kraken_client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await kraken_client._create_order_via_native(
                self._stop_request(ExchangeOrderTypeEnum.STOP_LOSS)
            )
        kwargs = trade_client.create_order.call_args.kwargs
        assert kwargs["ordertype"] == "stop-loss"
        assert kwargs["price"] == "48000.0"
        assert "price2" not in kwargs


@pytest.mark.asyncio
async def test_spot_stop_limit_without_limit_leg_rejected_pre_send() -> None:
    """A spot stop_limit without its limit leg is refused, not degraded (#156).

    Given: a stop-loss-limit request carrying a trigger but no price,
    When: create_order runs,
    Then: a pre-send ValueError surfaces and ccxt is never called —
        mirroring the futures client's gate; passing price=None on would
        crash inside ccxt price formatting or send a degraded order
        natively.
    """
    kraken_client = KrakenExchangeClient(
        api_key="test_key",
        api_secret="test_secret",
        sandbox=False,
    )
    mock_client = AsyncMock()
    request = ExchangeOrderRequest(
        symbol="BTC-USD",
        side=OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        amount=0.25,
        stop_price=48000.0,
        client_order_id="cid-stop-noleg",
    )
    with (
        patch.object(kraken_client, "_ccxt_client", mock_client),
        pytest.raises(ValueError, match="limit leg"),
    ):
        await kraken_client.create_order(request)
    mock_client.create_order.assert_not_called()


class TestResolveCcxtOrderType:
    """Raw-ordertype recovery for fetched ccxt snapshots (#156)."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def _ccxt_order(self, **overrides: Any) -> dict[str, Any]:
        """Build a minimal unified ccxt order dict."""
        base: dict[str, Any] = {
            "id": "OID-fetched-1",
            "clientOrderId": "cid-fetched-1",
            "symbol": "BTC/USD",
            "side": "sell",
            "type": "limit",
            "amount": 0.25,
            "price": 47900.0,
            "status": "open",
            "filled": 0.0,
            "remaining": 0.25,
            "timestamp": 1700000000000,
            "info": {"descr": {"ordertype": "stop-loss-limit"}},
        }
        base.update(overrides)
        return base

    def test_raw_kraken_ordertype_recovers_stop_identity(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """A fetched stop order keeps its stop identity in the snapshot.

        Given: a unified ccxt order whose type was collapsed to 'limit'
            but whose raw info.descr.ordertype says stop-loss-limit,
        When: converted to an ExchangeOrderSnapshot,
        Then: the snapshot type is STOP_LOSS_LIMIT, not LIMIT — fetched
            protective stops stay distinguishable on adoption paths.
        """
        snapshot = kraken_client._convert_ccxt_order(self._ccxt_order())
        assert snapshot.type is ExchangeOrderTypeEnum.STOP_LOSS_LIMIT
        market_collapsed = self._ccxt_order(
            type="market",
            price=None,
            info={"descr": {"ordertype": "stop-loss"}},
        )
        assert (
            kraken_client._convert_ccxt_order(market_collapsed).type
            is ExchangeOrderTypeEnum.STOP_LOSS
        )

    def test_missing_raw_description_falls_back_to_unified_type(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Payloads without info.descr keep the unified-type mapping.

        Given: unified orders with no raw description (or non-dict info),
        When: converted,
        Then: the pre-existing _CCXT_TYPE_MAP fallback applies.
        """
        no_info = self._ccxt_order(info=None)
        assert kraken_client._convert_ccxt_order(no_info).type is ExchangeOrderTypeEnum.LIMIT
        no_descr = self._ccxt_order(info={}, type="market")
        assert kraken_client._convert_ccxt_order(no_descr).type is ExchangeOrderTypeEnum.MARKET

    def test_unknown_raw_ordertype_falls_back_with_warning(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """An unrecognized raw ordertype degrades to the unified type.

        Given: a raw info.descr.ordertype outside the wire enum,
        When: converted,
        Then: the unified type mapping is used instead of raising.
        """
        weird = self._ccxt_order(info={"descr": {"ordertype": "quantum-stop"}})
        assert kraken_client._convert_ccxt_order(weird).type is ExchangeOrderTypeEnum.LIMIT


def _trade_built_candle() -> CandleUpdate:
    """Return a representative trade-built 1m candle for shadow-path tests.

    Returns:
        A ``CandleUpdate`` standing in for a completed 1-minute bucket.
    """
    return CandleUpdate(
        symbol="BTC/USD",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        vwap=1.5,
        trades=1,
        volume=1.0,
        interval_begin=datetime.fromtimestamp(0, UTC),
        interval=1,
    )


@pytest.mark.asyncio
async def test_handle_trade_data_folds_into_trade_built_builder_when_enabled(
    monkeypatch: MonkeyPatch,
) -> None:
    """Enabled shadow folds each parsed spot trade into the trade-built builder.

    Given: A client with trade-built shadow candles enabled,
    When: ``_handle_trade_data`` parses a trade frame,
    Then: The parsed trade is folded into the trade-built builder.
    """
    client = _client()
    client._trade_built_candles_enabled = True
    trade = TradeUpdate(
        symbol="BTC/USD",
        side="buy",
        quantity=0.1,
        price=50000.0,
        ord_type="limit",
        trade_id="1",
        timestamp=datetime.now(UTC),
    )
    monkeypatch.setattr(kr, "parse_kraken_trade_list", lambda data: [trade])
    client._handle_trade_data([{"symbol": "BTC/USD"}])
    assert client._trade_built_candle_builder.update_count == 1


@pytest.mark.asyncio
async def test_handle_trade_data_skips_trade_built_builder_when_disabled(
    monkeypatch: MonkeyPatch,
) -> None:
    """Disabled shadow leaves the trade-built builder untouched (no leak when off).

    Given: A client with trade-built shadow candles disabled (the default),
    When: ``_handle_trade_data`` parses a trade frame,
    Then: The trade-built builder receives no updates.
    """
    client = _client()
    trade = TradeUpdate(
        symbol="BTC/USD",
        side="buy",
        quantity=0.1,
        price=50000.0,
        ord_type="limit",
        trade_id="1",
        timestamp=datetime.now(UTC),
    )
    monkeypatch.setattr(kr, "parse_kraken_trade_list", lambda data: [trade])
    client._handle_trade_data([{"symbol": "BTC/USD"}])
    assert client._trade_built_candle_builder.update_count == 0


@pytest.mark.asyncio
async def test_subscribe_trade_built_candles_rejects_non_1m() -> None:
    """Trade-built shadow only supports 1m candles.

    Given: A Kraken spot client,
    When: ``subscribe_trade_built_candles`` is iterated with a non-1m timeframe,
    Then: A ``ValueError`` is raised.
    """
    client = _client()
    gen = client.subscribe_trade_built_candles(["BTC-USD"], "5m")
    with pytest.raises(ValueError, match="only support 1m"):
        await anext(gen)


@pytest.mark.asyncio
async def test_subscribe_trade_built_candles_drains_queue_and_isolates_native() -> None:
    """Shadow subscription yields trade-built candles without touching native queues.

    Given: A client whose trade-built queue receives a candle shortly after the
        first drain attempt times out,
    When: ``subscribe_trade_built_candles`` is iterated then closed,
    Then: The candle is yielded, ``_trade_built_candles_enabled`` toggles True
        during iteration and False after close, and the native
        ``_candle_queues`` are never created.
    """
    client = _client()
    candle = _trade_built_candle()

    async def _delayed_put() -> None:
        await asyncio.sleep(0.15)
        await client._trade_built_candle_queue.put(candle)

    putter = asyncio.create_task(_delayed_put())
    gen = client.subscribe_trade_built_candles(["BTC-USD"], "1m")
    first = await anext(gen)
    assert first is candle
    assert client._trade_built_candles_enabled is True
    await gen.aclose()
    assert client._trade_built_candles_enabled is False
    assert client._candle_queues == {}
    await putter


@pytest.mark.asyncio
async def test_trade_built_candle_aggregator_enqueues_completed_candles(
    monkeypatch: MonkeyPatch,
) -> None:
    """The aggregator routes grace-completed buckets into the trade-built queue.

    Given: A builder that reports one completed bucket,
    When: ``_trade_built_candle_aggregator`` runs,
    Then: The completed candle is enqueued and the builder sees now minus
        the configured finalization grace.

    Returns:
        None.
    """
    client = _client()
    candle = _trade_built_candle()
    fixed_now = datetime(2026, 6, 20, 12, 1, 15, tzinfo=UTC)
    observed_cutoffs: list[datetime] = []
    monkeypatch.setattr(kr, "datetime", SimpleNamespace(now=lambda _tz: fixed_now))

    def _pop_completed(now_utc: datetime) -> list[CandleUpdate]:
        """Capture the completion cutoff and return one candle.

        Args:
            now_utc: Grace-adjusted completion cutoff.

        Returns:
            Completed candle list.
        """
        observed_cutoffs.append(now_utc)
        return [candle]

    monkeypatch.setattr(client._trade_built_candle_builder, "pop_completed", _pop_completed)
    task = asyncio.create_task(client._trade_built_candle_aggregator())
    try:
        got = await asyncio.wait_for(client._trade_built_candle_queue.get(), timeout=3.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert got is candle
    assert observed_cutoffs == [fixed_now - timedelta(seconds=12)]


@pytest.mark.asyncio
async def test_trade_built_candle_aggregator_survives_tick_failure(
    monkeypatch: MonkeyPatch,
) -> None:
    """A failing aggregator tick is logged and swallowed so the task survives.

    Given: A builder whose ``pop_completed`` raises on the first tick (mirroring
        the 2026-06-23 serviceless-settings RuntimeError) then succeeds,
    When: ``_trade_built_candle_aggregator`` runs across ticks,
    Then: The failure does not kill the unsupervised task; the next tick's candle
        is still enqueued, proving Spot candle production is not silently stalled.

    Returns:
        None.
    """
    client = _client()
    candle = _trade_built_candle()
    calls = {"n": 0}

    def _pop_completed(now_utc: datetime) -> list[CandleUpdate]:
        """Raise on the first tick, then return one completed candle.

        Args:
            now_utc: Grace-adjusted completion cutoff.

        Returns:
            Completed candle list on later ticks.

        Raises:
            RuntimeError: On the first tick to simulate a transient fault.
        """
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated transient aggregator fault")
        return [candle]

    monkeypatch.setattr(client._trade_built_candle_builder, "pop_completed", _pop_completed)
    task = asyncio.create_task(client._trade_built_candle_aggregator())
    try:
        got = await asyncio.wait_for(client._trade_built_candle_queue.get(), timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert got is candle
    assert calls["n"] >= 2


@pytest.mark.asyncio
async def test_subscribe_trade_built_candles_reuses_running_aggregator_task() -> None:
    """A second subscription reuses the already-running aggregator task.

    Given: A client whose trade-built aggregator task is already running,
    When: ``subscribe_trade_built_candles`` is iterated,
    Then: The existing task is reused (not replaced) and is cancelled on close.
    """
    client = _client()

    async def _never() -> None:
        await asyncio.Event().wait()

    running = asyncio.create_task(_never())
    client._trade_built_candle_aggregator_task = running
    candle = _trade_built_candle()
    await client._trade_built_candle_queue.put(candle)
    gen = client.subscribe_trade_built_candles(["BTC-USD"], "1m")
    first = await anext(gen)
    assert first is candle
    assert client._trade_built_candle_aggregator_task is running
    await gen.aclose()
    assert running.cancelled() or running.done()
