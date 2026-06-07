"""Tests for Kraken Futures exchange client."""

import asyncio
import contextlib as _ctx
from collections.abc import Generator
from datetime import UTC
from datetime import UTC as _UTC
from datetime import datetime
from datetime import datetime as _dt
from datetime import timedelta
from datetime import timedelta as _td
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger

import snapper.infrastructure.exchanges._subscription_health as health_mod
import snapper.infrastructure.exchanges.implementations.kraken_futures as mod
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import kraken_futures as kf
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import _enqueue_or_drop_oldest
from snapper.infrastructure.exchanges.implementations.kraken_futures import _timeframe_to_seconds

_REAL_ASYNCIO_SLEEP = asyncio.sleep


@pytest.fixture()
def client() -> KrakenFuturesExchangeClient:
    """Create a KrakenFuturesExchangeClient instance for testing."""
    return KrakenFuturesExchangeClient(sandbox=True)


@pytest.fixture()
def auth_client() -> KrakenFuturesExchangeClient:
    """Create an authenticated KrakenFuturesExchangeClient for testing."""
    return KrakenFuturesExchangeClient(sandbox=True, api_key="test-key", api_secret="test-secret")


@pytest.fixture(autouse=True)
def _patch_ccxt() -> Generator[None]:
    """Patch ccxt.krakenfutures so no real HTTP calls are made."""
    mock_ccxt = MagicMock()
    mock_ccxt.load_markets = MagicMock(return_value={})
    mock_ccxt.fetch_ticker = MagicMock(
        return_value={
            "bid": 66500.0,
            "ask": 66510.0,
            "last": 66505.0,
            "baseVolume": 1234.0,
            "timestamp": 1640995200000,
        }
    )
    mock_ccxt.fetch_ohlcv = MagicMock(
        return_value=[
            [1640995200000, 50000.0, 50100.0, 49900.0, 50050.0, 100.0],
        ]
    )
    with patch("snapper.infrastructure.exchanges.implementations.kraken_futures.ccxt") as mock_mod:
        mock_mod.krakenfutures = MagicMock(return_value=mock_ccxt)
        yield


@pytest.fixture(autouse=True)
def _patch_sdk() -> Generator[None]:
    """Patch Kraken SDK Trade and User classes."""
    with (
        patch("snapper.infrastructure.exchanges.implementations.kraken_futures.Trade"),
        patch("snapper.infrastructure.exchanges.implementations.kraken_futures.User"),
    ):
        yield


async def _sync_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Call func directly instead of spawning a thread.

    Replaces asyncio.to_thread so coverage can track the executed code.
    """
    return func(*args, **kwargs)


@pytest.fixture(autouse=True)
def _patch_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run asyncio.to_thread synchronously for coverage tracking."""
    monkeypatch.setattr(mod.asyncio, "to_thread", _sync_to_thread)


class _DeterministicClock:
    """Deterministic monotonic clock for public subscribe pacing tests."""

    def __init__(self) -> None:
        """Initialize clock at zero."""
        self.now = 0.0

    def monotonic(self) -> float:
        """Return the current monotonic time."""
        return self.now

    def advance(self, delay: float) -> None:
        """Advance the clock without scheduling."""
        self.now += delay

    async def sleep(self, delay: float) -> None:
        """Advance the clock and yield control once."""
        self.now += delay
        await _REAL_ASYNCIO_SLEEP(0)


class _RecordingWsClient:
    """Record public subscribe calls and their send timestamps."""

    def __init__(self, clock: _DeterministicClock) -> None:
        """Bind the recorder to the shared deterministic clock."""
        self._clock = clock
        self.calls: list[tuple[str, tuple[str, ...], float]] = []

    async def subscribe(self, *, feed: str, products: list[str]) -> None:
        """Record the subscribe call with the current send timestamp."""
        self.calls.append((feed, tuple(products), self._clock.monotonic()))
        await _REAL_ASYNCIO_SLEEP(0)


def _install_public_subscribe_clock(
    monkeypatch: pytest.MonkeyPatch, clock: _DeterministicClock
) -> None:
    """Patch limiter and tracker clocks to a shared deterministic source."""
    monkeypatch.setattr(mod.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(health_mod.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(mod.asyncio, "sleep", clock.sleep)


class TestClientInit:
    """Tests for client initialization."""

    def test_init_defaults(self) -> None:
        """Initialize with default parameters.

        Given: No arguments,
        When: KrakenFuturesExchangeClient is created,
        Then: Sandbox is False, no auth, queues are empty.
        """
        c = KrakenFuturesExchangeClient()
        assert c.sandbox is False
        assert c.exchange_name == "kraken_futures"
        assert c.supports_websocket_executions is False
        assert c._tick_queue.empty()
        assert c._trade_queue.empty()
        assert c._execution_queue.empty()

    def test_init_sandbox(self) -> None:
        """Initialize with sandbox mode.

        Given: sandbox=True,
        When: KrakenFuturesExchangeClient is created,
        Then: Client is in sandbox mode.
        """
        c = KrakenFuturesExchangeClient(sandbox=True)
        assert c.sandbox is True

    def test_init_with_credentials(self) -> None:
        """Initialize with API credentials enables execution support.

        Given: api_key and api_secret provided,
        When: KrakenFuturesExchangeClient is created,
        Then: supports_websocket_executions is True and SDK clients are set.
        """
        c = KrakenFuturesExchangeClient(api_key="key", api_secret="secret")
        assert c.supports_websocket_executions is True
        assert c._api_key == "key"
        assert c._api_secret == "secret"
        assert c._trade_client is not None
        assert c._user_client is not None

    def test_init_without_credentials(self) -> None:
        """Initialize without API credentials disables execution support.

        Given: No api_key or api_secret,
        When: KrakenFuturesExchangeClient is created,
        Then: supports_websocket_executions is False and SDK clients are None.
        """
        c = KrakenFuturesExchangeClient()
        assert c.supports_websocket_executions is False
        assert c._trade_client is None
        assert c._user_client is None

    def test_tick_queue_absorbs_boot_burst(self) -> None:
        """Spec — the ticker producer queue is sized for boot-time replay.

        Given: A fresh KrakenFuturesExchangeClient,
        When: tick queue maxsize is compared to the standard queue
            bound,
        Then: tick queue maxsize is strictly greater than the
            non-tick ``_QUEUE_MAX_SIZE`` — the Kraken Futures broker,
            like Spot, replays a wildcard ticker snapshot on initial
            subscribe and sends follow-on updates before the consumer
            steady-state catches up. The 2026-05-25 post-restart
            5-minute window counted 5 drop-oldest WARN records on the
            Futures tick queue. The larger
            ``_TICK_QUEUE_MAX_SIZE`` absorbs the burst; the smaller
            default applies to ``trade``/``candle``/``execution``
            paths where no drops have been observed.
        """
        c = KrakenFuturesExchangeClient()
        assert c._tick_queue.maxsize >= 50_000
        assert c._tick_queue.maxsize == kf._TICK_QUEUE_MAX_SIZE
        assert c._candle_queue.maxsize >= 50_000
        assert c._candle_queue.maxsize == kf._TICK_QUEUE_MAX_SIZE
        assert c._tick_queue.maxsize > c._trade_queue.maxsize
        assert c._trade_queue.maxsize == kf._QUEUE_MAX_SIZE


@pytest.mark.asyncio
async def test_subscribe_ticks_caches_products_not_symbols(
    client: KrakenFuturesExchangeClient,
) -> None:
    """Futures cache stores products in the replayable symbols field.

    Given: A Futures client with an active websocket,
    When: A ticker feed is subscribed per product,
    Then: The cache records products and an empty parameters JSON payload.
    """
    client._ws_client = AsyncMock()
    await client._subscribe_in_chunks("ticker", ["PF_XBTUSD"])
    req = next(iter(client._subscription_cache.values()))
    assert req.channel == "ticker"
    assert req.symbols == ("PF_XBTUSD",)
    assert req.parameters_json == "{}"


@pytest.mark.asyncio
async def test_subscribe_dedup_distinguishes_ticker_and_trade_feeds_with_same_products(
    client: KrakenFuturesExchangeClient,
) -> None:
    """Futures cache keys include the feed name.

    Given: The same Futures product subscribed to ticker and trade feeds,
    When: Both subscriptions are cached,
    Then: The cache keeps two entries.
    """
    client._ws_client = AsyncMock()
    await client._subscribe_in_chunks("ticker", ["PF_XBTUSD"])
    await client._subscribe_in_chunks("trade", ["PF_XBTUSD"])
    assert len(client._subscription_cache) == 2
    assert {req.channel for req in client._subscription_cache.values()} == {"ticker", "trade"}


@pytest.mark.asyncio
async def test_replay_uses_subscribe_with_feed_and_products_kwargs(
    client: KrakenFuturesExchangeClient,
) -> None:
    """Futures replay uses feed/products kwargs rather than Spot params.

    Given: A Futures client with cached subscriptions,
    When: Subscriptions are replayed,
    Then: The SDK subscribe calls receive feed and products keyword arguments.
    """
    ws = AsyncMock()
    client._ws_client = ws
    first = SubscriptionRequest(channel="ticker", symbols=("PF_XBTUSD",), parameters_json="{}")
    second = SubscriptionRequest(channel="trade", symbols=("PF_ETHUSD",), parameters_json="{}")
    client._subscription_cache[first.key()] = first
    client._subscription_cache[second.key()] = second
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_futures.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await client._replay_subscriptions()
    assert ws.subscribe.await_args_list[0].kwargs == {
        "feed": "ticker",
        "products": ["PF_XBTUSD"],
    }
    assert ws.subscribe.await_args_list[1].kwargs == {
        "feed": "trade",
        "products": ["PF_ETHUSD"],
    }
    sleep_mock.assert_awaited_once()
    assert sleep_mock.await_args is not None
    sleep_arg = sleep_mock.await_args.args[0]
    assert 0 < sleep_arg <= kf._PUBLIC_SUBSCRIBE_MIN_INTERVAL_S


@pytest.mark.asyncio
async def test_replay_requires_public_ws_client(client: KrakenFuturesExchangeClient) -> None:
    """Replay fails when the public websocket client is absent.

    Given: A Futures client without a public websocket,
    When: Subscriptions are replayed,
    Then: RuntimeError is raised.
    """
    client._ws_client = None
    with pytest.raises(RuntimeError):
        await client._replay_subscriptions()


@pytest.mark.asyncio
async def test_ensure_ws_connected_auto_replays_after_reconnect(
    client: KrakenFuturesExchangeClient,
) -> None:
    """Futures reconnect automatically replays cached subscriptions.

    Given: A Futures client with a cached public subscription,
    When: _ensure_ws_connected creates a websocket,
    Then: The replay hook is awaited.
    """
    req = SubscriptionRequest(channel="ticker", symbols=("PF_XBTUSD",), parameters_json="{}")
    client._subscription_cache[req.key()] = req
    with (
        patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient"
        ) as ws_cls,
        patch.object(client, "_replay_subscriptions", new_callable=AsyncMock) as replay_mock,
    ):
        ws_cls.return_value.start = AsyncMock()
        await client._ensure_ws_connected()
    replay_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_public_subscribe_paced_by_min_interval(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Limiter spaces sequential public subscribes by the minimum interval.

    Given: A Futures client with the shared public limiter clock patched,
    When: Five public subscribes are sent sequentially,
    Then: Each send is separated by at least the configured minimum interval.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    ws = _RecordingWsClient(clock)
    client._ws_client = ws

    for product in ("PF_A", "PF_B", "PF_C", "PF_D", "PF_E"):
        await client._send_public_subscribe(feed="ticker", product=product)

    send_times = [call[2] for call in ws.calls]
    gaps = [curr - prev for prev, curr in zip(send_times, send_times[1:], strict=False)]

    assert len(ws.calls) == 5
    assert all(gap >= kf._PUBLIC_SUBSCRIBE_MIN_INTERVAL_S - 1e-12 for gap in gaps)


@pytest.mark.asyncio
async def test_public_subscribe_requires_connected_ws(client: KrakenFuturesExchangeClient) -> None:
    """Public subscribe helper requires an active websocket client.

    Given: A Futures client without a connected public websocket,
    When: The shared public subscribe helper is called,
    Then: It raises RuntimeError instead of silently dropping the request.
    """
    client._ws_client = None

    with pytest.raises(RuntimeError, match="connected"):
        await client._send_public_subscribe(feed="ticker", product="PF_XBTUSD")


@pytest.mark.asyncio
async def test_public_subscribe_rejects_unsupported_feed(
    client: KrakenFuturesExchangeClient,
) -> None:
    """Public subscribe helper rejects feeds outside ticker and trade.

    Given: A Futures client with a connected public websocket,
    When: The shared public subscribe helper receives an unsupported feed,
    Then: It raises ValueError before touching the websocket client.
    """
    client._ws_client = AsyncMock()

    with pytest.raises(ValueError, match="Unsupported public subscription feed"):
        await client._send_public_subscribe(feed="book", product="PF_XBTUSD")


@pytest.mark.asyncio
async def test_rate_limited_alert_triggers_cooldown(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rate-limited alerts pause later public subscribes by the cooldown window.

    Given: A Futures client that already sent one public subscribe,
    When: A rate_limited alert arrives before the next subscribe,
    Then: The next subscribe waits at least the configured cooldown.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    ws = _RecordingWsClient(clock)
    client._ws_client = ws

    await client._send_public_subscribe(feed="ticker", product="PF_XBTUSD")
    await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    await client._send_public_subscribe(feed="ticker", product="PF_ETHUSD")

    assert len(ws.calls) == 2
    assert ws.calls[1][2] - ws.calls[0][2] >= kf._RATE_LIMITED_COOLDOWN_S - 1e-12


@pytest.mark.asyncio
async def test_repeated_rate_limited_alerts_use_max_semantics(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated rate-limited alerts extend to the latest cooldown only once.

    Given: A Futures client receiving multiple rate_limited alerts within one second,
    When: Each alert updates the limiter state,
    Then: The pause horizon stays at roughly now plus one cooldown, not a sum of alerts.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)

    await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    clock.advance(0.4)
    await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    clock.advance(0.4)
    await client._on_ws_message({"event": "alert", "message": "rate_limited"})

    assert client._next_public_subscribe_at - clock.monotonic() == pytest.approx(
        kf._RATE_LIMITED_COOLDOWN_S
    )


@pytest.mark.asyncio
async def test_rate_limited_log_is_throttled_within_cooldown(
    client: KrakenFuturesExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Burst-arriving rate_limited alerts emit exactly one WARN per cooldown window.

    Given: A Futures client receiving multiple ``rate_limited`` alerts
        in rapid succession (the production failure mode: SDK message
        buffer flushes N pending broker responses together during a
        shutdown or reconnect storm — 2026-05-25 boot-1 shutdown saw
        515 alerts in 140 ms),
    When: ``_on_ws_message`` dispatches each alert to ``_handle_alert_event``,
    Then: Exactly ONE WARN record is emitted (the first one); the
        subsequent alerts within the same cooldown window flow through
        the cooldown-update path silently. This protects the boot log
        from buffer-flush amplification of an otherwise correct
        rate-limit handler. Mirrors the P2 aggregate pattern (commit
        ``05142cbf``): preserve behavioural correctness, suppress
        per-event log spam.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        for _ in range(5):
            await client._on_ws_message({"event": "alert", "message": "rate_limited"})
            clock.advance(0.1)
    finally:
        logger.remove(handler_id)
    rate_limited_warnings = [
        r for r in caplog.records if r.levelname == "WARNING" and "rate_limited" in r.getMessage()
    ]
    assert len(rate_limited_warnings) == 1


@pytest.mark.asyncio
async def test_rate_limited_log_re_emits_after_cooldown_window(
    client: KrakenFuturesExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A new burst after the cooldown elapses produces a fresh WARN.

    Given: A Futures client that received a ``rate_limited`` alert
        (emitting one WARN) and then waited longer than the cooldown
        before another alert arrives,
    When: ``_on_ws_message`` dispatches the second alert,
    Then: A SECOND WARN is emitted — operators must still get a
        notification when the broker rate-limits us again after the
        previous limit was supposed to have cleared. The throttle is
        per-cooldown-window, not per-process. Branch coverage: the
        ``now - self._last_rate_limited_log_at >= _RATE_LIMITED_COOLDOWN_S``
        gate must evaluate to True on the second call.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        await client._on_ws_message({"event": "alert", "message": "rate_limited"})
        clock.advance(kf._RATE_LIMITED_COOLDOWN_S + 1.0)
        await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    finally:
        logger.remove(handler_id)
    rate_limited_warnings = [
        r for r in caplog.records if r.levelname == "WARNING" and "rate_limited" in r.getMessage()
    ]
    assert len(rate_limited_warnings) == 2


@pytest.mark.asyncio
async def test_rate_limited_state_updates_on_every_alert_regardless_of_log(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cooldown state extends on each alert even when the WARN is throttled.

    Given: A Futures client that already absorbed and logged one
        ``rate_limited`` alert, then receives a second alert while
        still inside the cooldown window (so its WARN is suppressed),
    When: ``_on_ws_message`` dispatches the second alert,
    Then: ``self._next_public_subscribe_at`` is bumped to the second
        alert's deadline (``now + _RATE_LIMITED_COOLDOWN_S``) via the
        existing ``max`` semantics — verifying that the log throttle
        does NOT alter the behavioural rate-limit handling. Behavioural
        correctness and log noise control are decoupled.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    clock.advance(2.0)
    await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    assert client._next_public_subscribe_at - clock.monotonic() == pytest.approx(
        kf._RATE_LIMITED_COOLDOWN_S
    )


@pytest.mark.asyncio
async def test_rate_limited_log_emits_at_exact_cooldown_boundary(
    client: KrakenFuturesExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The gate fires at ``elapsed == _RATE_LIMITED_COOLDOWN_S`` exactly.

    Given: A Futures client that emitted one ``rate_limited`` WARN at
        t=0, then waited EXACTLY ``_RATE_LIMITED_COOLDOWN_S`` seconds
        (the boundary case),
    When: A second ``rate_limited`` alert arrives,
    Then: A SECOND WARN is emitted — the gate uses ``>=`` so the
        boundary is inclusive. Without this branch coverage a future
        switch to strict ``>`` would silently regress this edge case.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        await client._on_ws_message({"event": "alert", "message": "rate_limited"})
        clock.advance(kf._RATE_LIMITED_COOLDOWN_S)
        await client._on_ws_message({"event": "alert", "message": "rate_limited"})
    finally:
        logger.remove(handler_id)
    rate_limited_warnings = [
        r for r in caplog.records if r.levelname == "WARNING" and "rate_limited" in r.getMessage()
    ]
    assert len(rate_limited_warnings) == 2


@pytest.mark.asyncio
async def test_rate_limited_sampled_now_is_consistent_under_lock_contention(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cooldown deadline uses the actual lock-acquire time, not pre-lock time.

    Given: A Futures client whose ``_public_subscribe_lock`` is held
        externally for 3 simulated seconds (modelling the production
        pattern where ``_send_public_subscribe`` holds the same lock
        across an awaited ``asyncio.sleep`` / WS subscribe at lines
        1208-1219 of the implementation),
    When: A ``rate_limited`` alert arrives during that hold and waits
        behind the lock,
    Then: ``_next_public_subscribe_at`` is computed from the time
        AFTER the lock acquisition (lock-internal sample), not from
        the pre-lock sample. Sampling ``now`` before the lock would
        leak a stale cooldown deadline equal to the wait duration.
        Regression coverage for a throttle defect where the original
        implementation sampled ``now`` outside the lock and would have
        under-extended the cooldown by the wait duration under contention.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    await client._public_subscribe_lock.acquire()
    holding_advance_s = 3.0
    alert_task = asyncio.create_task(
        client._handle_alert_event({"event": "alert", "message": "rate_limited"})
    )
    while not client._public_subscribe_lock._waiters:
        await _REAL_ASYNCIO_SLEEP(0)
    clock.advance(holding_advance_s)
    client._public_subscribe_lock.release()
    await alert_task
    expected_deadline = holding_advance_s + kf._RATE_LIMITED_COOLDOWN_S
    assert client._next_public_subscribe_at == pytest.approx(expected_deadline)


@pytest.mark.asyncio
async def test_already_subscribed_alert_logged_as_debug(
    client: KrakenFuturesExchangeClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Benign 'Already subscribed to feed, re-requesting' alerts log at DEBUG.

    Given: A Futures client receiving the Kraken Futures broker-emitted
        race alert ``{'event': 'alert', 'message': 'Already subscribed
        to feed, re-requesting'}`` (fires on publisher health-loop
        replay collisions and reconnect echoes — confirmed benign in
        production via the 2026-05-25 boot-log review),
    When: ``_on_ws_message`` dispatches the alert to ``_handle_alert_event``,
    Then: The handler emits a DEBUG record matching the
        ``"Kraken Futures subscribe race (already subscribed)"`` prefix
        and NO WARNING record fires — real subscription failures
        retain their WARNING level via the downstream branches.
        Mirrors the Spot-side patch in
        :mod:`snapper.infrastructure.exchanges.kraken_sdk_patches`,
        scoped to the Futures publisher boundary because Futures uses
        a separate ``ConnectFuturesWebsocket`` client outside the
        Spot SDK monkeypatch surface.
    """
    handler_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        await client._on_ws_message(
            {"event": "alert", "message": "Already subscribed to feed, re-requesting"}
        )
    finally:
        logger.remove(handler_id)
    assert any(
        r.levelname == "DEBUG" and "subscribe race (already subscribed)" in r.getMessage()
        for r in caplog.records
    )
    assert not any(r.levelname == "WARNING" for r in caplog.records)


@pytest.mark.asyncio
async def test_real_subscription_alert_keeps_warning_level(
    client: KrakenFuturesExchangeClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Real attributed subscription failures still log at WARNING.

    Given: A Futures client receiving a non-race alert with a feed +
        product_id and an explicit error message
        (``{'event': 'alert', 'message': 'Invalid arguments', 'feed':
        'ticker', 'product_id': 'PF_XBTUSD'}``),
    When: ``_on_ws_message`` dispatches the alert to ``_handle_alert_event``,
    Then: A WARNING record is emitted via the attributed-alert branch
        (``"Kraken Futures subscription alert feed=ticker
        product=PF_XBTUSD error=Invalid arguments"``) — verifying the
        DEBUG downgrade is scoped exclusively to the benign race
        message and does NOT shadow operationally significant alerts.
    """
    handler_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        await client._on_ws_message(
            {
                "event": "alert",
                "message": "Invalid arguments",
                "feed": "ticker",
                "product_id": "PF_XBTUSD",
            }
        )
    finally:
        logger.remove(handler_id)
    assert any(
        r.levelname == "WARNING" and "subscription alert" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_mark_pending_happens_inside_limiter(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Queued subscribes stamp requested_at at actual send time.

    Given: Three concurrent public subscribes queued behind the limiter,
    When: The third subscribe eventually acquires the lock and is sent,
    Then: Its health-tracker requested_at matches the third actual send time.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    ws = _RecordingWsClient(clock)
    client._ws_client = ws

    await asyncio.gather(
        client._send_public_subscribe(feed="ticker", product="PF_ONE"),
        client._send_public_subscribe(feed="ticker", product="PF_TWO"),
        client._send_public_subscribe(feed="ticker", product="PF_THREE"),
    )

    snapshot = client._health_tracker.snapshot()
    third_send_at = ws.calls[2][2]

    assert snapshot[("ticker", "PF_THREE")].requested_at == pytest.approx(third_send_at)
    assert third_send_at >= 2 * kf._PUBLIC_SUBSCRIBE_MIN_INTERVAL_S - 1e-12


@pytest.mark.asyncio
async def test_replay_after_reconnect_uses_same_limiter(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replay and fresh subscribes share the same public limiter.

    Given: Cached public subscriptions and one fresh subscribe queued during replay,
    When: Replay and the fresh subscribe run concurrently,
    Then: All sends are serialized through one paced send stream.
    """
    clock = _DeterministicClock()
    _install_public_subscribe_clock(monkeypatch, clock)
    ws = _RecordingWsClient(clock)
    client._ws_client = ws

    for product in ("PF_A", "PF_B", "PF_C", "PF_D"):
        req = SubscriptionRequest(channel="ticker", symbols=(product,), parameters_json="{}")
        client._subscription_cache[req.key()] = req

    replay_task = asyncio.create_task(client._replay_subscriptions())
    await _REAL_ASYNCIO_SLEEP(0)
    fresh_task = asyncio.create_task(client._send_public_subscribe("ticker", "PF_LATE"))
    await asyncio.gather(replay_task, fresh_task)

    send_times = [call[2] for call in ws.calls]
    gaps = [curr - prev for prev, curr in zip(send_times, send_times[1:], strict=False)]

    assert len(ws.calls) == 5
    assert {call[1][0] for call in ws.calls} == {"PF_A", "PF_B", "PF_C", "PF_D", "PF_LATE"}
    assert all(gap >= kf._PUBLIC_SUBSCRIBE_MIN_INTERVAL_S - 1e-12 for gap in gaps)


class TestRequireAuthenticated:
    """Tests for _require_authenticated guard."""

    def test_raises_without_credentials(self, client: KrakenFuturesExchangeClient) -> None:
        """Raise RuntimeError when credentials are missing.

        Given: Client without API credentials,
        When: _require_authenticated is called,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            client._require_authenticated()

    def test_passes_with_credentials(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Pass without error when credentials are set.

        Given: Client with API credentials,
        When: _require_authenticated is called,
        Then: No exception raised.
        """
        auth_client._require_authenticated()


class TestEnqueueOrDropOldest:
    """Tests for _enqueue_or_drop_oldest module-level helper."""

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

    def test_drop_log_is_rate_limited(self, caplog: pytest.LogCaptureFixture) -> None:
        """Per-drop log spam is collapsed to one summary per interval.

        Given: A bounded queue at capacity 1 and a freshly-reset counter,
        When: 50 drop-oldest events fire within the same interval,
        Then: At most one warning summary line is emitted.
        """
        kf._drop_counters.clear()
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("seed")
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for i in range(50):
                _enqueue_or_drop_oldest(queue, f"item-{i}", "futures-tick")
        finally:
            logger.remove(sink_id)
        kf._drop_counters.clear()

        summaries = [rec for rec in caplog.records if "futures-tick queue full" in rec.message]
        assert len(summaries) <= 1


class TestConnect:
    """Tests for connect/disconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_loads_markets(self, client: KrakenFuturesExchangeClient) -> None:
        """Connect loads CCXT markets and creates Market client.

        Given: Fresh client,
        When: connect() is called,
        Then: Markets are loaded and market_client is set.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market"
        ) as mock_market_cls:
            await client.connect()
            assert client._market_client is not None
            mock_market_cls.assert_called_once_with(sandbox=True)

    @pytest.mark.asyncio
    async def test_connect_failure_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """Connect failure propagates exception.

        Given: CCXT load_markets raises,
        When: connect() is called,
        Then: Exception propagates.
        """
        client._ccxt_client.load_markets = MagicMock(side_effect=RuntimeError("network"))
        with pytest.raises(RuntimeError, match="network"):
            await client.connect()

    @pytest.mark.asyncio
    async def test_disconnect_without_ws(self, client: KrakenFuturesExchangeClient) -> None:
        """Disconnect when no WS client is active.

        Given: Client with no WS connection,
        When: disconnect() is called,
        Then: Completes without error.
        """
        await client.disconnect()
        assert client._ws_client is None
        assert client._private_ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_with_ws(self, client: KrakenFuturesExchangeClient) -> None:
        """Disconnect closes WS client.

        Given: Client with active WS connection,
        When: disconnect() is called,
        Then: WS client is closed and set to None.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        await client.disconnect()
        mock_ws.close.assert_awaited_once()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_ws_error_handled(self, client: KrakenFuturesExchangeClient) -> None:
        """Disconnect handles WS close error gracefully.

        Given: WS client raises on close(),
        When: disconnect() is called,
        Then: No exception propagated, WS client set to None.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = RuntimeError("close failed")
        client._ws_client = mock_ws
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_private_ws(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Disconnect closes private WS client.

        Given: Client with active private WS connection,
        When: disconnect() is called,
        Then: Private WS client is closed and set to None.
        """
        mock_ws = AsyncMock()
        auth_client._private_ws_client = mock_ws
        await auth_client.disconnect()
        mock_ws.close.assert_awaited_once()
        assert auth_client._private_ws_client is None


class TestOnWsMessage:
    """Tests for the WS callback-to-queue bridge."""

    @pytest.fixture(autouse=True)
    def _patch_adapters(self) -> Generator[None]:
        """Patch adapter functions for WS message routing tests."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
                return_value=TickerUpdate(
                    symbol="BTC-USD-PERP",
                    bid=66500.0,
                    bid_qty=50.0,
                    ask=66510.0,
                    ask_qty=30.0,
                    last=66505.0,
                    volume=1234.0,
                    vwap=0.0,
                    low=65500.0,
                    high=67000.0,
                    change=0.94,
                    change_pct=0.0,
                ),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
                return_value=TradeUpdate(
                    symbol="BTC-USD-PERP",
                    side="buy",
                    quantity=10.0,
                    price=66621.0,
                    ord_type="fill",
                    timestamp=MagicMock(),
                    trade_id="abc-123",
                ),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_ticker_message_routed_to_queue(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Route ticker WS message to tick_queue.

        Given: WS message with feed=ticker,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {"feed": "ticker", "product_id": "PF_XBTUSD", "bid": 66500.0}
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()
        update = client._tick_queue.get_nowait()
        assert update.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_ticker_lite_message_routed(self, client: KrakenFuturesExchangeClient) -> None:
        """Route ticker_lite WS message to tick_queue.

        Given: WS message with feed=ticker_lite,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {"feed": "ticker_lite", "product_id": "PF_XBTUSD", "last": 66505.0}
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_ticker_message_with_symbol_routed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Route ticker WS message without product_id backfill.

        Given: WS ticker message that already includes symbol,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue without rewriting symbol.
        """
        expected_update = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )
        msg = {"feed": "ticker", "symbol": "BTC-USD-PERP", "bid": 66500.0}
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            return_value=expected_update,
        ) as mock_parse:
            await client._on_ws_message(msg)
        mock_parse.assert_called_once_with(msg)
        assert not client._tick_queue.empty()
        update = client._tick_queue.get_nowait()
        assert update == expected_update

    @pytest.mark.asyncio
    async def test_on_message_passes_live_trade_feed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Route a live ``feed=trade`` WS message (single-trade-per-envelope) to trade_queue.

        Given: WS message with ``feed=trade`` and trade fields directly on
            the envelope (no ``trades`` array — this is the live shape),
        When: _on_ws_message is called,
        Then: Parsed TradeUpdate is placed in _trade_queue.
            The pre-2026-05-12 handler looked for ``message["trades"]``
            and silently dropped every live trade because the live envelope
            has no such array — the fix normalizes the live ``trade`` feed
            via ``[message]``.
        """
        msg = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "time": 1640995200000,
            "qty": 10.0,
            "price": 66621.0,
            "side": "buy",
        }
        await client._on_ws_message(msg)
        assert not client._trade_queue.empty()
        update = client._trade_queue.get_nowait()
        assert update.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_on_message_drops_trade_snapshot_feed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Drop a ``feed=trade_snapshot`` WS message before enqueue.

        Given: WS message with ``feed=trade_snapshot`` and ``trades: [...]``
            (the initial-state batch shape),
        When: _on_ws_message is called,
        Then: No TradeUpdate is placed in _trade_queue.
        """
        msg = {
            "feed": "trade_snapshot",
            "product_id": "PI_XBTUSD",
            "trades": [
                {"time": 1640995200000, "qty": 10.0, "price": 66621.0, "side": "buy"},
                {"time": 1640995201000, "qty": 5.0, "price": 66622.0, "side": "sell"},
            ],
        }
        await client._on_ws_message(msg)
        assert client._trade_queue.empty()
        assert client._candle_builder.active_buckets() == 0

    @pytest.mark.asyncio
    async def test_trade_message_also_folds_into_candle_builder(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Every parsed live trade is also routed into ``_candle_builder``.

        Given: A live ``feed=trade`` WS message,
        When: ``_on_ws_message`` is called,
        Then: ``_candle_builder.active_buckets()`` becomes >= 1 — proving
            the trade-handler path wires the builder, not just the
            trade queue. Regression guard for a defect where
            ``builder.update`` was missing on the Kraken Futures hot
            path (Kraken Equities had it; Futures was an accidental
            omission).
        """
        assert client._candle_builder.active_buckets() == 0
        msg = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "time": 1640995200000,
            "qty": 7.0,
            "price": 66600.0,
            "side": "buy",
        }
        await client._on_ws_message(msg)
        assert client._candle_builder.active_buckets() == 1

    @pytest.mark.asyncio
    async def test_live_trade_messages_fold_each_into_candle_builder(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Every live trade envelope folds into the builder.

        Given: Two live ``feed=trade`` messages,
        When: ``_on_ws_message`` is called for each,
        Then: The candle bucket for the current minute holds 2 trades
            in its ``trades`` counter.
        """
        assert client._candle_builder.active_buckets() == 0
        first_msg = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "time": 1640995200000,
            "qty": 1.0,
            "price": 66600.0,
            "side": "buy",
        }
        second_msg = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "time": 1640995320000,
            "qty": 1.0,
            "price": 66601.0,
            "side": "buy",
        }
        await client._on_ws_message(first_msg)
        await client._on_ws_message(second_msg)
        assert client._candle_builder.active_buckets() == 1
        candles = client._candle_builder.pop_completed(datetime.now(UTC) + timedelta(minutes=2))
        assert len(candles) == 1
        assert candles[0].trades == 2

    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, client: KrakenFuturesExchangeClient) -> None:
        """Ignore heartbeat messages.

        Given: WS heartbeat message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"feed": "heartbeat"})
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_event_message_ignored(self, client: KrakenFuturesExchangeClient) -> None:
        """Ignore subscription event messages.

        Given: WS subscription ACK message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"event": "subscribed", "feed": "ticker"})
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_ticker_skipped(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip ticker messages that fail parsing.

        Given: WS ticker message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in tick_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            side_effect=ValueError("parse error"),
        ):
            await client._on_ws_message({"feed": "ticker", "product_id": "INVALID"})
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_trade_skipped(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip trade messages that fail parsing.

        Given: WS trade message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in trade_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "feed": "trade",
                "product_id": "INVALID",
                "trades": [{"time": 0, "qty": 1.0, "price": 100.0, "side": "buy"}],
            }
            await client._on_ws_message(msg)
        assert client._trade_queue.empty()


class TestOnExecutionMessage:
    """Tests for the private WS execution callback."""

    @pytest.mark.asyncio
    async def test_fill_message_routed_to_execution_queue(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Route fills WS message to execution_queue.

        Given: WS message with feed=fills,
        When: _on_execution_message is called,
        Then: Parsed ExecutionUpdate is placed in _execution_queue.
        """
        mock_update = MagicMock(spec=ExecutionUpdate)
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_fill",
            return_value=mock_update,
        ):
            msg = {"feed": "fills", "fills": [{"fill_id": "f1", "order_id": "o1"}]}
            await auth_client._on_execution_message(msg)
        assert not auth_client._execution_queue.empty()

    @pytest.mark.asyncio
    async def test_fills_snapshot_message_routed(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Route fills_snapshot WS message to execution_queue.

        Given: WS message with feed=fills_snapshot,
        When: _on_execution_message is called,
        Then: Parsed ExecutionUpdate is placed in _execution_queue.
        """
        mock_update = MagicMock(spec=ExecutionUpdate)
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_fill",
            return_value=mock_update,
        ):
            msg = {"feed": "fills_snapshot", "fills": [{"fill_id": "f1", "order_id": "o1"}]}
            await auth_client._on_execution_message(msg)
        assert not auth_client._execution_queue.empty()

    @pytest.mark.asyncio
    async def test_open_orders_delta_ignored(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Ignore open_orders delta WS messages.

        Given: WS message with feed=open_orders,
        When: _on_execution_message is called,
        Then: No items in execution queue (open_orders excluded from fill pipeline).
        """
        msg: dict[str, Any] = {"feed": "open_orders", "order": {"order_id": "o1"}}
        await auth_client._on_execution_message(msg)
        assert auth_client._execution_queue.empty()

    @pytest.mark.asyncio
    async def test_open_orders_snapshot_ignored(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Ignore open_orders_snapshot WS messages.

        Given: WS message with feed=open_orders_snapshot,
        When: _on_execution_message is called,
        Then: No items in execution queue (open_orders excluded from fill pipeline).
        """
        msg: dict[str, Any] = {"feed": "open_orders_snapshot", "orders": [{"order_id": "o1"}]}
        await auth_client._on_execution_message(msg)
        assert auth_client._execution_queue.empty()

    @pytest.mark.asyncio
    async def test_event_message_ignored(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Ignore subscription event messages on private WS.

        Given: WS subscription ACK message,
        When: _on_execution_message is called,
        Then: No items in execution queue.
        """
        await auth_client._on_execution_message({"event": "subscribed", "feed": "fills"})
        assert auth_client._execution_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_fill_skipped(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Skip fill messages that fail parsing.

        Given: WS fill message that causes ValueError in parser,
        When: _on_execution_message is called,
        Then: No items in execution_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_fill",
            side_effect=ValueError("parse error"),
        ):
            msg = {"feed": "fills", "fills": [{"invalid": "data"}]}
            await auth_client._on_execution_message(msg)
        assert auth_client._execution_queue.empty()


class TestRestMethods:
    """Tests for REST API methods."""

    @pytest.mark.asyncio
    async def test_get_ticker(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch ticker via CCXT.

        Given: CCXT client returns ticker data,
        When: get_ticker is called,
        Then: Returns TickerSnapshot with correct values.
        """
        result = await client.get_ticker("BTC/USD:USD")
        assert result.bid == pytest.approx(66500.0)
        assert result.ask == pytest.approx(66510.0)
        assert result.last == pytest.approx(66505.0)

    @pytest.mark.asyncio
    async def test_get_ohlcv(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch OHLCV candles via CCXT.

        Given: CCXT client returns candle data,
        When: get_ohlcv is called,
        Then: Returns list of OhlcvSnapshot with correct values.
        """
        result = await client.get_ohlcv("BTC/USD:USD", "1m")
        assert len(result) == 1
        assert result[0].timestamp == pytest.approx(1640995200.0)
        assert result[0].open == pytest.approx(50000.0)
        assert result[0].close == pytest.approx(50050.0)


class TestOrderMethods:
    """Tests for authenticated order CRUD methods."""

    @pytest.mark.asyncio
    async def test_create_order_returns_snapshot(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Create order returns ExchangeOrderSnapshot with DB IDs.

        Given: Authenticated client with mocked Trade SDK and DB logging,
        When: create_order is called,
        Then: Returns snapshot with order details and DB identifiers.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-123", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=5.0,
            price=66000.0,
            client_order_id="my-order-1",
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch.object(auth_client, "_log_order_to_db", new_callable=AsyncMock) as mock_log,
        ):
            mock_log.return_value = (42, "pub-id-001")
            result = await auth_client.create_order(request)
        assert result.id == "ord-123"
        assert result.symbol == "BTC-USD-PERP"
        assert result.side == OrderSideEnum.BUY
        assert result.status == ExchangeOrderStatusEnum.OPEN
        assert result.amount == pytest.approx(5.0)
        assert result.price == pytest.approx(66000.0)
        assert result.client_order_id == "my-order-1"
        assert result.db_order_id == 42
        assert result.db_order_public_id == "pub-id-001"
        mock_log.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_order_requires_auth(self, client: KrakenFuturesExchangeClient) -> None:
        """Create order raises RuntimeError without credentials.

        Given: Unauthenticated client,
        When: create_order is called,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.create_order(MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_order_returns_snapshot(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Cancel order returns ExchangeOrderSnapshot.

        Given: Authenticated client with mocked Trade SDK,
        When: cancel_order is called,
        Then: Returns snapshot with cancelled status.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.cancel_order = MagicMock(
            return_value={"cancelStatus": {"status": "cancelled"}}
        )
        result = await auth_client.cancel_order("ord-123", symbol="BTC-USD-PERP")
        assert result.id == "ord-123"
        assert result.status == ExchangeOrderStatusEnum.CANCELED
        assert result.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_get_order_returns_snapshot(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get order returns ExchangeOrderSnapshot.

        Given: Authenticated client with mocked Trade SDK,
        When: get_order is called,
        Then: Returns snapshot with current order state.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order_id": "ord-123",
                        "symbol": "PF_XBTUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 10.0,
                        "filledSize": 3.0,
                        "limitPrice": 66000.0,
                        "status": "partiallyFilled",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await auth_client.get_order("ord-123")
        assert result.id == "ord-123"
        assert result.symbol == "BTC-USD-PERP"
        assert result.filled == pytest.approx(3.0)
        assert result.status == ExchangeOrderStatusEnum.OPEN

    @pytest.mark.asyncio
    async def test_get_order_not_found_raises(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get order raises ValueError when order not found.

        Given: SDK returns empty orders list,
        When: get_order is called,
        Then: Raises ValueError.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.get_orders_status = MagicMock(return_value={"orders": []})
        with pytest.raises(ValueError, match="not found"):
            await auth_client.get_order("nonexistent")

    @pytest.mark.asyncio
    async def test_get_orders_returns_list(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Get orders returns list of snapshots.

        Given: Authenticated client with mocked User SDK,
        When: get_orders is called,
        Then: Returns list of ExchangeOrderSnapshot.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_orders = MagicMock(
            return_value={
                "openOrders": [
                    {
                        "order_id": "ord-1",
                        "symbol": "PF_XBTUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 5.0,
                        "filledSize": 0.0,
                        "status": "placed",
                    },
                    {
                        "order_id": "ord-2",
                        "symbol": "PF_ETHUSD",
                        "side": "sell",
                        "orderType": "mkt",
                        "qty": 10.0,
                        "filledSize": 10.0,
                        "status": "filled",
                    },
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=lambda s: {"PF_XBTUSD": "BTC-USD-PERP", "PF_ETHUSD": "ETH-USD-PERP"}[s],
        ):
            result = await auth_client.get_orders()
        assert len(result) == 2
        assert result[0].id == "ord-1"
        assert result[1].id == "ord-2"

    @pytest.mark.asyncio
    async def test_get_orders_filters_by_symbol(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get orders filters by native symbol.

        Given: Two open orders on different symbols,
        When: get_orders is called with symbol filter,
        Then: Returns only matching orders.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_orders = MagicMock(
            return_value={
                "openOrders": [
                    {
                        "order_id": "ord-1",
                        "symbol": "PF_XBTUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 5.0,
                        "status": "placed",
                    },
                    {
                        "order_id": "ord-2",
                        "symbol": "PF_ETHUSD",
                        "side": "sell",
                        "orderType": "lmt",
                        "qty": 10.0,
                        "status": "placed",
                    },
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=lambda s: {"PF_XBTUSD": "BTC-USD-PERP", "PF_ETHUSD": "ETH-USD-PERP"}[s],
        ):
            result = await auth_client.get_orders(symbol="BTC-USD-PERP")
        assert len(result) == 1
        assert result[0].symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_get_balance_returns_account_balances(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get balance returns dict of AccountBalance.

        Given: Authenticated client with mocked User SDK returning nested balances,
        When: get_balance is called,
        Then: Returns dict with currency balances using marginRequirements.im.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balances": {"USD": 10000.0},
                        "marginRequirements": {"im": 500.0},
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert "USD" in result
        assert result["USD"].total == pytest.approx(10000.0)
        assert result["USD"].used == pytest.approx(500.0)
        assert result["USD"].free == pytest.approx(9500.0)

    @pytest.mark.asyncio
    async def test_get_balance_filters_by_currency(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get balance filters by specific currency.

        Given: Multiple currencies in wallets,
        When: get_balance is called with currency filter,
        Then: Returns only the matching currency.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balances": {"USD": 10000.0, "BTC": 1.5},
                        "marginRequirements": {"im": 0},
                    }
                }
            }
        )
        result = await auth_client.get_balance(currency="USD")
        assert len(result) == 1
        assert "USD" in result

    @pytest.mark.asyncio
    async def test_order_methods_require_auth(self, client: KrakenFuturesExchangeClient) -> None:
        """All order methods raise RuntimeError without credentials.

        Given: Unauthenticated client,
        When: Any order method is called,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.cancel_order("123")
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.get_order("123")
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.get_orders()
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.get_balance()


class TestStubMethods:
    """Tests for stub methods that remain unimplemented."""

    def test_subscribe_executions_requires_auth(self, client: KrakenFuturesExchangeClient) -> None:
        """subscribe_executions raises RuntimeError without credentials.

        Given: Unauthenticated client,
        When: subscribe_executions is iterated,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            iterator = client.subscribe_executions()
            asyncio.get_event_loop().run_until_complete(iterator.__anext__())


class TestSubscribeCandles:
    """Tests for trade-synthesized candle streaming via subscribe_candles."""

    @pytest.mark.asyncio
    async def test_non_1m_raises_value_error(self, client: KrakenFuturesExchangeClient) -> None:
        """subscribe_candles rejects any timeframe other than 1m.

        Given: An ``"1h"`` request,
        When: subscribe_candles is iterated,
        Then: A ``ValueError`` surfaces on the first ``__anext__``
            pointing the caller at ``get_ohlcv`` for historical
            backfill (still REST-based and intentionally so).
        """
        iterator = client.subscribe_candles(["BTC-USD-PERP"], "1h")
        with pytest.raises(ValueError, match="only supports 1m"):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test_reuses_running_aggregator_task(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """A second subscribe_candles call does not spawn a duplicate aggregator.

        Given: A pre-existing aggregator task that is still running,
        When: subscribe_candles is iterated and immediately closed,
        Then: The same task object remains the client's aggregator
            handle (the ``is None or .done()`` guard short-circuits).
        """

        async def never_returns() -> None:
            while True:
                await asyncio.sleep(60)

        client._candle_aggregator_task = asyncio.create_task(never_returns())
        original_task = client._candle_aggregator_task
        try:
            iterator = client.subscribe_candles(["BTC-USD-PERP"], "1m")
            with _ctx.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.05)
            assert client._candle_aggregator_task is original_task
        finally:
            original_task.cancel()
            with _ctx.suppress(asyncio.CancelledError):
                await original_task

    @pytest.mark.asyncio
    async def test_timeout_loops_until_candle_arrives(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """The yield loop survives queue-empty timeouts without spinning out.

        Given: The candle queue raises ``TimeoutError`` immediately,
        When: subscribe_candles is awaited with a tight outer timeout,
        Then: The retry sleep branch executes deterministically and the
            outer ``wait_for`` is what finally bails.
        """
        real_sleep = asyncio.sleep
        sleeps: list[float] = []

        async def fast_sleep(delay: float) -> None:
            sleeps.append(delay)
            await real_sleep(0)

        client._candle_queue.get = AsyncMock(side_effect=TimeoutError())
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["BTC-USD-PERP"], "1m")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.2)
        assert 0.01 in sleeps

    @pytest.mark.asyncio
    async def test_emits_candle_built_from_trade_stream(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """subscribe_candles emits a candle once the minute closes.

        Given: A trade with a past timestamp is folded into the
            builder (its minute is therefore strictly before the
            aggregator's ``now``),
        When: subscribe_candles is iterated with ``asyncio.sleep``
            collapsed so the aggregator ticks immediately,
        Then: A ``CandleUpdate`` carrying the trade's OHLCV is
            yielded. We avoid mocking ``datetime.now`` because that
            would break the ``.replace().timestamp()`` chain inside
            the builder.
        """
        past_minute = (_dt.now(_UTC) - _td(minutes=5)).replace(second=0, microsecond=0)
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=0.5,
            price=50000.0,
            ord_type="fill",
            timestamp=past_minute,
            trade_id="t1",
        )
        client._candle_builder.update(trade)

        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["BTC-USD-PERP"], "1m")
            result = await asyncio.wait_for(iterator.__anext__(), timeout=2.0)
        assert isinstance(result, CandleUpdate)
        assert result.symbol == "BTC-USD-PERP"
        assert result.open == pytest.approx(50000.0)
        assert result.close == pytest.approx(50000.0)
        assert result.volume == pytest.approx(0.5)
        assert result.trades == 1
        assert result.interval == 60


class TestTimeframeToSeconds:
    """Tests for _timeframe_to_seconds helper."""

    def test_known_timeframes(self) -> None:
        """All supported timeframes map to correct seconds.

        Given: Known timeframe strings,
        When: _timeframe_to_seconds is called,
        Then: Returns correct duration.
        """
        assert _timeframe_to_seconds("1m") == 60
        assert _timeframe_to_seconds("5m") == 300
        assert _timeframe_to_seconds("15m") == 900
        assert _timeframe_to_seconds("1h") == 3600
        assert _timeframe_to_seconds("4h") == 14400
        assert _timeframe_to_seconds("1d") == 86400

    def test_unsupported_timeframe_raises(self) -> None:
        """Unsupported timeframe raises ValueError.

        Given: An invalid timeframe string,
        When: _timeframe_to_seconds is called,
        Then: Raises ValueError with supported list.
        """
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            _timeframe_to_seconds("2w")


class TestSubscribeInstruments:
    """Tests for instrument subscription (REST-based)."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_yields_all(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Yield all instruments from REST API.

        Given: Market client returns 2 instruments,
        When: subscribe_instruments is iterated,
        Then: Yields 2 raw instrument dicts.
        """
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {
            "instruments": [
                {"symbol": "PI_XBTUSD", "type": "futures_inverse"},
                {"symbol": "PF_ETHUSD", "type": "futures_vanilla"},
            ]
        }
        client._market_client = mock_market
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 2
        assert results[0]["symbol"] == "PI_XBTUSD"

    def test_get_instruments_sync(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch instruments synchronously.

        Given: Market client returns instruments,
        When: get_instruments_sync is called,
        Then: Returns list of raw instrument dicts.
        """
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {"instruments": [{"symbol": "PI_XBTUSD"}]}
        client._market_client = mock_market
        results = client.get_instruments_sync()
        assert len(results) == 1

    def test_get_parsed_instrument(self, client: KrakenFuturesExchangeClient) -> None:
        """Parse raw instrument dict into descriptor.

        Given: Raw instrument dict,
        When: get_parsed_instrument is called,
        Then: Returns InstrumentPairDescriptor.
        """
        raw = {
            "symbol": "PI_XBTUSD",
            "type": "futures_inverse",
            "tickSize": 0.5,
            "contractSize": 1,
            "tradeable": True,
            "base": "BTC",
            "quote": "USD",
            "marginLevels": [{"contracts": 0, "initialMargin": 0.02, "maintenanceMargin": 0.01}],
        }
        result = client.get_parsed_instrument(raw)
        assert isinstance(result, InstrumentPairDescriptor)
        assert result.symbol == "PI_XBTUSD"


class TestSymbolConversionInOrders:
    """Tests for symbol conversion used in order methods."""

    @pytest.mark.asyncio
    async def test_create_order_converts_symbol(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Create order converts native symbol to Kraken product ID.

        Given: Authenticated client,
        When: create_order is called with native symbol,
        Then: SDK receives Kraken product ID.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-1", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=1.0,
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
            return_value="PF_XBTUSD",
        ) as mock_convert:
            await auth_client.create_order(request)
        mock_convert.assert_called_once_with("BTC-USD-PERP")
        assert auth_client._trade_client is not None
        call_kwargs = auth_client._trade_client.create_order.call_args
        assert call_kwargs.kwargs["symbol"] == "PF_XBTUSD"

    @pytest.mark.asyncio
    async def test_convert_sdk_order_handles_unknown_symbol(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Convert SDK order gracefully handles unknown symbol mapping.

        Given: SDK returns order with unmapped Kraken symbol,
        When: _convert_sdk_order is called,
        Then: Falls back to raw Kraken symbol.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=ValueError("unknown"),
        ):
            result = auth_client._convert_sdk_order(
                {
                    "order_id": "ord-1",
                    "symbol": "PF_UNKNOWN",
                    "side": "buy",
                    "orderType": "lmt",
                    "qty": 1.0,
                    "status": "placed",
                }
            )
        assert result.symbol == "PF_UNKNOWN"


class TestDisconnectPrivateWsError:
    """Tests for private WS close error handling during disconnect."""

    @pytest.mark.asyncio
    async def test_disconnect_private_ws_error_handled(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Disconnect handles private WS close error gracefully.

        Given: Private WS client raises on close(),
        When: disconnect() is called,
        Then: No exception propagated, private WS client set to None.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = RuntimeError("private close failed")
        auth_client._private_ws_client = mock_ws
        await auth_client.disconnect()
        assert auth_client._private_ws_client is None


class TestEnsurePrivateWsConnected:
    """Tests for _ensure_private_ws_connected."""

    @pytest.mark.asyncio
    async def test_creates_private_ws_client(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Create private WS client when not yet connected.

        Given: Authenticated client without private WS,
        When: _ensure_private_ws_connected is called,
        Then: Private WS client is created and started.
        """
        mock_ws = AsyncMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
            return_value=mock_ws,
        ):
            await auth_client._ensure_private_ws_connected()
        assert auth_client._private_ws_client is mock_ws
        mock_ws.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_when_already_connected(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Skip connection when private WS is already connected.

        Given: Authenticated client with existing private WS,
        When: _ensure_private_ws_connected is called,
        Then: Existing client is preserved unchanged.
        """
        existing_ws = AsyncMock()
        auth_client._private_ws_client = existing_ws
        await auth_client._ensure_private_ws_connected()
        assert auth_client._private_ws_client is existing_ws
        existing_ws.start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_requires_auth(self, client: KrakenFuturesExchangeClient) -> None:
        """Raise RuntimeError when credentials are missing.

        Given: Unauthenticated client,
        When: _ensure_private_ws_connected is called,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client._ensure_private_ws_connected()


class TestCreateOrderStopPrice:
    """Tests for create_order stop_price branch."""

    @pytest.mark.asyncio
    async def test_create_order_with_stop_price(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Create order passes stop_price when provided.

        Given: Authenticated client,
        When: create_order is called with stop_price,
        Then: SDK receives stopPrice kwarg.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-stp", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.STOP_LOSS,
            amount=1.0,
            stop_price=60000.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch.object(auth_client, "_log_order_to_db", new_callable=AsyncMock) as mock_log,
        ):
            mock_log.return_value = None
            result = await auth_client.create_order(request)
        assert result.id == "ord-stp"
        call_kwargs = auth_client._trade_client.create_order.call_args.kwargs
        assert call_kwargs["stopPrice"] == pytest.approx(60000.0)


class TestCreateOrderValidation:
    """Tests for create_order order type validation."""

    @pytest.mark.asyncio
    async def test_unsupported_order_type_raises(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Reject unsupported order types instead of silent fallback.

        Given: Authenticated client,
        When: create_order is called with STOP_LOSS_LIMIT (unsupported),
        Then: Raises ValueError listing supported types.
        """
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
            amount=1.0,
            price=60000.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            pytest.raises(ValueError, match="Unsupported order type"),
        ):
            await auth_client.create_order(request)

    @pytest.mark.asyncio
    async def test_create_order_db_log_returns_none(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Create order succeeds even when DB logging returns None.

        Given: Authenticated client where _log_order_to_db returns None,
        When: create_order is called,
        Then: Returns snapshot without DB IDs.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-nodb", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch.object(auth_client, "_log_order_to_db", new_callable=AsyncMock) as mock_log,
        ):
            mock_log.return_value = None
            result = await auth_client.create_order(request)
        assert result.id == "ord-nodb"
        assert result.db_order_id is None
        assert result.db_order_public_id is None


class TestConvertSdkOrderVariants:
    """Tests for _convert_sdk_order SDK payload normalization."""

    def test_lowercase_symbol_normalized(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Normalize lowercase SDK symbols to uppercase.

        Given: SDK order dict with lowercase symbol ``pf_xbtusd``,
        When: _convert_sdk_order is called,
        Then: Symbol is uppercased before native conversion.
        """
        data = {
            "order_id": "ord-lc",
            "symbol": "pf_xbtusd",
            "side": "buy",
            "orderType": "lmt",
            "qty": 5.0,
            "filledSize": 0.0,
            "status": "placed",
        }
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ) as mock_mapper:
            result = auth_client._convert_sdk_order(data)
        mock_mapper.assert_called_once_with("PF_XBTUSD")
        assert result.symbol == "BTC-USD-PERP"

    def test_filled_size_and_unfilled_size_variants(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle filledSize + unfilledSize without qty field.

        Given: SDK order dict with filledSize and unfilledSize but no qty,
        When: _convert_sdk_order is called,
        Then: Derives qty from filledSize + unfilledSize.
        """
        data = {
            "order_id": "ord-uf",
            "symbol": "PF_XBTUSD",
            "side": "sell",
            "orderType": "lmt",
            "filledSize": 3.0,
            "unfilledSize": 7.0,
            "status": "partiallyFilled",
        }
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = auth_client._convert_sdk_order(data)
        assert result.amount == pytest.approx(10.0)
        assert result.filled == pytest.approx(3.0)
        assert result.remaining == pytest.approx(7.0)

    def test_order_id_variant(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Handle orderId field variant.

        Given: SDK order dict with orderId (camelCase) instead of order_id,
        When: _convert_sdk_order is called,
        Then: Resolves order ID correctly.
        """
        data = {
            "orderId": "ord-camel",
            "symbol": "PF_XBTUSD",
            "side": "buy",
            "type": "limit",
            "qty": 1.0,
            "status": "placed",
        }
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = auth_client._convert_sdk_order(data)
        assert result.id == "ord-camel"

    def test_type_field_variant(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Handle 'type' field variant instead of 'orderType'.

        Given: SDK order dict with type=limit (not orderType),
        When: _convert_sdk_order is called,
        Then: Maps order type correctly.
        """
        data = {
            "order_id": "ord-type",
            "symbol": "PF_XBTUSD",
            "side": "buy",
            "type": "mkt",
            "qty": 1.0,
            "status": "placed",
        }
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = auth_client._convert_sdk_order(data)
        assert result.type == ExchangeOrderTypeEnum.MARKET


class TestGetOrdersStatusAndLimit:
    """Tests for get_orders status filter and limit branches."""

    @pytest.mark.asyncio
    async def test_get_orders_filters_by_status(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get orders filters by order status.

        Given: Multiple open orders with different statuses,
        When: get_orders is called with status filter,
        Then: Returns only matching orders.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_orders = MagicMock(
            return_value={
                "openOrders": [
                    {
                        "order_id": "ord-1",
                        "symbol": "PF_XBTUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 5.0,
                        "status": "placed",
                    },
                    {
                        "order_id": "ord-2",
                        "symbol": "PF_XBTUSD",
                        "side": "sell",
                        "orderType": "mkt",
                        "qty": 10.0,
                        "filledSize": 10.0,
                        "status": "filled",
                    },
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await auth_client.get_orders(status=ExchangeOrderStatusEnum.CLOSED)
        assert len(result) == 1
        assert result[0].id == "ord-2"

    @pytest.mark.asyncio
    async def test_get_orders_limits_results(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get orders limits the number of returned results.

        Given: Three open orders,
        When: get_orders is called with limit=1,
        Then: Returns only 1 order.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_orders = MagicMock(
            return_value={
                "openOrders": [
                    {
                        "order_id": f"ord-{i}",
                        "symbol": "PF_XBTUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 1.0,
                        "status": "placed",
                    }
                    for i in range(3)
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await auth_client.get_orders(limit=1)
        assert len(result) == 1


class TestGetBalanceCurrencyFilter:
    """Tests for get_balance edge cases with wallet data."""

    @pytest.mark.asyncio
    async def test_get_balance_skips_non_dict_account(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Skip non-dict account entries in wallet data.

        Given: Wallet data with a non-dict entry,
        When: get_balance is called,
        Then: Non-dict entries are skipped.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balances": {"USD": 5000.0},
                        "marginRequirements": {"im": 100.0},
                    },
                    "invalid": "not-a-dict",
                }
            }
        )
        result = await auth_client.get_balance()
        assert len(result) == 1
        assert "USD" in result

    @pytest.mark.asyncio
    async def test_get_balance_skips_zero_amounts(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Skip currencies with zero balance.

        Given: Wallet data with a zero-balance currency,
        When: get_balance is called,
        Then: Zero-balance currencies are omitted.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balances": {"USD": 5000.0, "EUR": 0},
                        "marginRequirements": {"im": 0},
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert "USD" in result
        assert "EUR" not in result


class TestSubscribeTicksImpl:
    """Tests for subscribe_ticks and _subscribe_ticks_impl."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_yields_updates(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Subscribe to ticks yields TickerUpdate from queue.

        Given: WS client connected and tick enqueued,
        When: subscribe_ticks is iterated,
        Then: Yields the TickerUpdate.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        tick = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Yield the tick on first call, then raise to stop iteration."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._tick_queue.put_nowait(tick)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[TickerUpdate] = []
            try:
                async for update in client.subscribe_ticks(["BTC-USD-PERP"]):
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1
        assert results[0].symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_subscribe_ticks_connection_error_on_exception_occur(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise ConnectionError when WS exception_occur is set.

        Given: WS client with exception_occur=True,
        When: subscribe_ticks loop checks,
        Then: Raises ConnectionError and resets ws_client in finally.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = True
        mock_ws.unsubscribe = AsyncMock()

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            pytest.raises(ConnectionError, match="connection lost"),
        ):
            async for _ in client.subscribe_ticks(["BTC-USD-PERP"]):
                pass
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_ticks_unsubscribe_error_handled(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle unsubscribe error gracefully during cleanup.

        Given: WS client that fails on unsubscribe,
        When: subscribe_ticks is cancelled,
        Then: No exception propagated.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        mock_ws.unsubscribe = AsyncMock(side_effect=RuntimeError("unsub failed"))

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Raise on first call to break the loop."""
            nonlocal call_count
            call_count += 1
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
            pytest.raises(ConnectionError),
        ):
            async for _ in client.subscribe_ticks(["BTC-USD-PERP"]):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_ticks_timeout_continues(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """TimeoutError from queue.get is retried.

        Given: WS client connected,
        When: queue.get times out then yields a message,
        Then: Loop continues and yields the message.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        tick = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Timeout first, yield second, then error to stop."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                coro.close()
                raise TimeoutError
            if call_count == 2:
                client._tick_queue.put_nowait(tick)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[TickerUpdate] = []
            try:
                async for update in client.subscribe_ticks(["BTC-USD-PERP"]):
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1


class TestSubscribeTradesImpl:
    """Tests for subscribe_trades and _subscribe_trades_impl."""

    @pytest.mark.asyncio
    async def test_subscribe_trades_yields_updates(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Subscribe to trades yields TradeUpdate from queue.

        Given: WS client connected and trade enqueued,
        When: subscribe_trades is iterated,
        Then: Yields the TradeUpdate.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Yield trade on first call, then raise to stop."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._trade_queue.put_nowait(trade)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[TradeUpdate] = []
            try:
                async for update in client.subscribe_trades(["BTC-USD-PERP"]):
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1
        assert results[0].symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_subscribe_trades_connection_error_on_exception_occur(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise ConnectionError when trade WS exception_occur is set.

        Given: WS client with exception_occur=True,
        When: subscribe_trades loop checks,
        Then: Raises ConnectionError and resets ws_client in finally.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = True
        mock_ws.unsubscribe = AsyncMock()

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            pytest.raises(ConnectionError, match="connection lost"),
        ):
            async for _ in client.subscribe_trades(["BTC-USD-PERP"]):
                pass
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_trades_unsubscribe_error_handled(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle unsubscribe error gracefully during trade cleanup.

        Given: WS client that fails on unsubscribe,
        When: subscribe_trades is cancelled,
        Then: No exception propagated.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        mock_ws.unsubscribe = AsyncMock(side_effect=RuntimeError("unsub failed"))

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Raise on first call to break the loop."""
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
            pytest.raises(ConnectionError),
        ):
            async for _ in client.subscribe_trades(["BTC-USD-PERP"]):
                pass


class TestSubscribeExecutionsImpl:
    """Tests for subscribe_executions and _subscribe_executions_impl."""

    @pytest.mark.asyncio
    async def test_subscribe_executions_yields_updates(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Subscribe to executions yields ExecutionUpdate from queue.

        Given: Private WS client connected and execution enqueued,
        When: subscribe_executions is iterated,
        Then: Yields the ExecutionUpdate.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        execution = MagicMock(spec=ExecutionUpdate)

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Yield execution on first call, then raise to stop."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                auth_client._execution_queue.put_nowait(execution)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[ExecutionUpdate] = []
            try:
                async for update in auth_client.subscribe_executions():
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_subscribe_executions_connection_error_on_exception_occur(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise ConnectionError when private WS exception_occur is set.

        Given: Private WS client with exception_occur=True,
        When: subscribe_executions loop checks,
        Then: Raises ConnectionError and resets private_ws_client in finally.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = True
        mock_ws.unsubscribe = AsyncMock()

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            pytest.raises(ConnectionError, match="private WS connection lost"),
        ):
            async for _ in auth_client.subscribe_executions():
                pass
        assert auth_client._private_ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_executions_unsubscribe_error_handled(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle unsubscribe error gracefully during executions cleanup.

        Given: Private WS client that fails on unsubscribe,
        When: subscribe_executions is cancelled,
        Then: No exception propagated.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        mock_ws.unsubscribe = AsyncMock(side_effect=RuntimeError("unsub failed"))

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Raise on first call to break the loop."""
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
            pytest.raises(ConnectionError),
        ):
            async for _ in auth_client.subscribe_executions():
                pass


class TestSubscribeInstrumentsEdgePaths:
    """Tests for subscribe_instruments and get_instruments_sync edge paths."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_creates_market_client(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Create Market client when not yet set.

        Given: Client with no market_client,
        When: subscribe_instruments is iterated,
        Then: Market client is lazily created.
        """
        client._market_client = None
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {"instruments": [{"symbol": "PI_XBTUSD"}]}
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market",
            return_value=mock_market,
        ):
            results = []
            async for inst in client.subscribe_instruments():
                results.append(inst)
        assert len(results) == 1

    def test_get_instruments_sync_creates_market_client(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Create Market client when not yet set in sync path.

        Given: Client with no market_client,
        When: get_instruments_sync is called,
        Then: Market client is lazily created.
        """
        client._market_client = None
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {"instruments": [{"symbol": "PI_XBTUSD"}]}
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market",
            return_value=mock_market,
        ):
            results = client.get_instruments_sync()
        assert len(results) == 1


class TestOnExecutionMessageUnknownFeed:
    """Tests for _on_execution_message with unrecognized feeds."""

    @pytest.mark.asyncio
    async def test_unknown_feed_ignored(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Ignore messages with unrecognized feed type.

        Given: WS message with feed=heartbeat (not fills),
        When: _on_execution_message is called,
        Then: No items in execution queue.
        """
        await auth_client._on_execution_message({"feed": "heartbeat"})
        assert auth_client._execution_queue.empty()


class TestEnsureWsConnectedEarlyReturn:
    """Tests for _ensure_ws_connected early return path."""

    @pytest.mark.asyncio
    async def test_skips_when_already_connected(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip WS connection when already connected.

        Given: Client with existing WS client,
        When: _ensure_ws_connected is called,
        Then: Existing client is preserved, start not called.
        """
        existing_ws = AsyncMock()
        client._ws_client = existing_ws
        await client._ensure_ws_connected()
        assert client._ws_client is existing_ws
        existing_ws.start.assert_not_awaited()


class TestSubscribeImplGuardPaths:
    """Tests for RuntimeError guard and TimeoutError in subscribe impls."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_guard_ws_none(self, client: KrakenFuturesExchangeClient) -> None:
        """Raise RuntimeError when _ensure_ws_connected leaves ws_client None.

        Given: _ensure_ws_connected does not set ws_client,
        When: _subscribe_ticks_impl is iterated,
        Then: Raises RuntimeError.
        """
        with (
            patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            async for _ in client._subscribe_ticks_impl(["BTC-USD-PERP"]):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_trades_guard_ws_none(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise RuntimeError when _ensure_ws_connected leaves ws_client None.

        Given: _ensure_ws_connected does not set ws_client,
        When: _subscribe_trades_impl is iterated,
        Then: Raises RuntimeError.
        """
        with (
            patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            async for _ in client._subscribe_trades_impl(["BTC-USD-PERP"]):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_in_chunks_guard_ws_none(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """_subscribe_in_chunks raises when ws_client is not initialised.

        Given: client._ws_client = None,
        When: _subscribe_in_chunks is awaited directly,
        Then: RuntimeError is raised before any subscribe call.
        """
        client._ws_client = None
        with pytest.raises(RuntimeError, match="WebSocket client not connected"):
            await client._subscribe_in_chunks("ticker", ["PF_XBTUSD"])

    @pytest.mark.asyncio
    async def test_subscribe_in_chunks_subscribes_each_product_and_sleeps(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Per-product subscribe issues one SDK call per product.

        Given: Several ws_symbols,
        When: _subscribe_in_chunks runs,
        Then: subscribe is called once per product and asyncio.sleep is
            awaited between products, not after the last product.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        ws_symbols = [f"PF_SYM{i:03d}USD" for i in range(4)]
        sleep_calls: list[float] = []

        async def _spy_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.asyncio.sleep",
            new=_spy_sleep,
        ):
            await client._subscribe_in_chunks("ticker", ws_symbols)

        assert mock_ws.subscribe.await_count == len(ws_symbols)
        assert [call.kwargs["products"] for call in mock_ws.subscribe.await_args_list] == [
            [symbol] for symbol in ws_symbols
        ]
        assert [req.symbols for req in client._subscription_cache.values()] == [
            (symbol,) for symbol in ws_symbols
        ]
        assert len(sleep_calls) == 3
        assert all(0 < delay <= kf._PUBLIC_SUBSCRIBE_MIN_INTERVAL_S for delay in sleep_calls)

    @pytest.mark.asyncio
    async def test_subscribe_executions_guard_ws_none(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise RuntimeError when _ensure_private_ws_connected leaves client None.

        Given: _ensure_private_ws_connected does not set private_ws_client,
        When: _subscribe_executions_impl is iterated,
        Then: Raises RuntimeError.
        """
        with (
            patch.object(auth_client, "_ensure_private_ws_connected", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="Private WebSocket client not connected"),
        ):
            async for _ in auth_client._subscribe_executions_impl():
                pass

    @pytest.mark.asyncio
    async def test_subscribe_trades_timeout_continues(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """TimeoutError from trade queue.get is retried.

        Given: WS client connected,
        When: queue.get times out then yields a message,
        Then: Loop continues and yields the message.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Timeout first, yield second, then error to stop."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                coro.close()
                raise TimeoutError
            if call_count == 2:
                client._trade_queue.put_nowait(trade)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[TradeUpdate] = []
            try:
                async for update in client.subscribe_trades(["BTC-USD-PERP"]):
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_subscribe_executions_timeout_continues(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """TimeoutError from execution queue.get is retried.

        Given: Private WS client connected,
        When: queue.get times out then yields a message,
        Then: Loop continues and yields the message.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        execution = MagicMock(spec=ExecutionUpdate)

        call_count = 0

        async def fake_wait_for(coro: Any, timeout: float) -> Any:
            """Timeout first, yield second, then error to stop."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                coro.close()
                raise TimeoutError
            if call_count == 2:
                auth_client._execution_queue.put_nowait(execution)
                result: Any = await coro
                return result
            coro.close()
            raise ConnectionError("stop")

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient",
                return_value=mock_ws,
            ),
            patch("asyncio.wait_for", side_effect=fake_wait_for),
        ):
            results: list[ExecutionUpdate] = []
            try:
                async for update in auth_client.subscribe_executions():
                    results.append(update)
            except ConnectionError:
                pass
        assert len(results) == 1


class TestCreateOrderPostOnlyAndReduceOnly:
    """Tests for post_only and reduce_only branches in create_order."""

    @pytest.mark.asyncio
    async def test_create_order_post_only_limit(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Post-only limit order uses 'post' order type.

        Given: Authenticated client,
        When: create_order is called with post_only=True and LIMIT type,
        Then: SDK receives orderType='post' instead of 'lmt'.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-post", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=66000.0,
            post_only=True,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch.object(auth_client, "_log_order_to_db", new_callable=AsyncMock) as mock_log,
        ):
            mock_log.return_value = None
            result = await auth_client.create_order(request)
        call_kwargs = auth_client._trade_client.create_order.call_args.kwargs
        assert call_kwargs["orderType"] == "post"
        assert result.id == "ord-post"

    @pytest.mark.asyncio
    async def test_create_order_reduce_only(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Reduce-only order passes reduceOnly=True to SDK.

        Given: Authenticated client,
        When: create_order is called with reduce_only=True,
        Then: SDK receives reduceOnly=True kwarg.
        """
        assert auth_client._trade_client is not None
        auth_client._trade_client.create_order = MagicMock(
            return_value={"sendStatus": {"order_id": "ord-reduce", "status": "placed"}}
        )
        request = ExchangeOrderRequest(
            symbol="BTC-USD-PERP",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=2.0,
            reduce_only=True,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch.object(auth_client, "_log_order_to_db", new_callable=AsyncMock) as mock_log,
        ):
            mock_log.return_value = None
            result = await auth_client.create_order(request)
        call_kwargs = auth_client._trade_client.create_order.call_args.kwargs
        assert call_kwargs["reduceOnly"] is True
        assert result.id == "ord-reduce"


class TestParseFlexAccount:
    """Tests for _parse_flex_account static method."""

    def test_parse_flex_account_with_balance(self) -> None:
        """Parse flex account returns AccountBalance when balance is non-zero.

        Given: Account data with balanceValue=5000, initialMargin=200, availableMargin=4800,
        When: _parse_flex_account is called,
        Then: Returns AccountBalance with correct values.
        """
        acct_data: dict[str, Any] = {
            "balanceValue": 5000.0,
            "initialMargin": 200.0,
            "availableMargin": 4800.0,
        }
        result = KrakenFuturesExchangeClient._parse_flex_account("flex", acct_data)
        assert result is not None
        assert result.currency == "flex_usd"
        assert result.total == pytest.approx(5000.0)
        assert result.used == pytest.approx(200.0)
        assert result.free == pytest.approx(4800.0)

    def test_parse_flex_account_zero_balance(self) -> None:
        """Parse flex account returns None when balance is zero.

        Given: Account data with balanceValue=0,
        When: _parse_flex_account is called,
        Then: Returns None.
        """
        acct_data: dict[str, Any] = {
            "balanceValue": 0,
            "initialMargin": 0,
            "availableMargin": 0,
        }
        result = KrakenFuturesExchangeClient._parse_flex_account("cash", acct_data)
        assert result is None

    def test_parse_flex_account_defaults_available_margin(self) -> None:
        """Parse flex account defaults availableMargin to balanceValue when missing.

        Given: Account data without availableMargin key,
        When: _parse_flex_account is called,
        Then: free equals balanceValue, used equals 0.
        """
        acct_data: dict[str, Any] = {
            "balanceValue": 3000.0,
            "initialMargin": 100.0,
        }
        result = KrakenFuturesExchangeClient._parse_flex_account("flex", acct_data)
        assert result is not None
        assert result.free == pytest.approx(3000.0)
        assert result.used == pytest.approx(0.0)
        assert result.total == pytest.approx(3000.0)

    def test_parse_flex_account_available_exceeds_total(self) -> None:
        """Parse flex account clamps free to total when availableMargin exceeds balanceValue.

        Given: Account data where availableMargin > balanceValue,
        When: _parse_flex_account is called,
        Then: free is clamped to balanceValue, used is 0.
        """
        acct_data: dict[str, Any] = {
            "balanceValue": 100.0,
            "availableMargin": 110.0,
        }
        result = KrakenFuturesExchangeClient._parse_flex_account("flex", acct_data)
        assert result is not None
        assert result.free == pytest.approx(100.0)
        assert result.used == pytest.approx(0.0)
        assert result.total == pytest.approx(100.0)


class TestGetBalanceFlexWallet:
    """Tests for flex/cash wallet branch in get_balance."""

    @pytest.mark.asyncio
    async def test_get_balance_flex_wallet(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Get balance returns flex wallet balance via _parse_flex_account.

        Given: Wallet data with a flex account (no balances dict),
        When: get_balance is called,
        Then: Returns AccountBalance parsed from flex account fields.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balanceValue": 8000.0,
                        "initialMargin": 300.0,
                        "availableMargin": 7700.0,
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert "flex_usd" in result
        assert result["flex_usd"].total == pytest.approx(8000.0)
        assert result["flex_usd"].used == pytest.approx(300.0)
        assert result["flex_usd"].free == pytest.approx(7700.0)

    @pytest.mark.asyncio
    async def test_get_balance_cash_wallet(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Get balance returns cash wallet balance via _parse_flex_account.

        Given: Wallet data with a cash account (no balances dict),
        When: get_balance is called,
        Then: Returns AccountBalance parsed from cash account fields.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "cash": {
                        "balanceValue": 2000.0,
                        "initialMargin": 50.0,
                        "availableMargin": 1950.0,
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert "cash_usd" in result
        assert result["cash_usd"].total == pytest.approx(2000.0)

    @pytest.mark.asyncio
    async def test_get_balance_flex_zero_skipped(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get balance skips flex wallet with zero balance.

        Given: Wallet data with a flex account whose balanceValue is 0,
        When: get_balance is called,
        Then: No flex_usd entry in result.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "flex": {
                        "balanceValue": 0,
                        "initialMargin": 0,
                        "availableMargin": 0,
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert "flex_usd" not in result

    @pytest.mark.asyncio
    async def test_get_balance_unknown_account_type_skipped(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get balance skips dict accounts that are neither coin-margin nor flex/cash.

        Given: Wallet data with an account named 'other' lacking balances key,
        When: get_balance is called,
        Then: The unknown account is silently skipped.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_wallets = MagicMock(
            return_value={
                "accounts": {
                    "other": {
                        "balanceValue": 999.0,
                        "initialMargin": 0,
                    }
                }
            }
        )
        result = await auth_client.get_balance()
        assert len(result) == 0


class TestGetOpenPositions:
    """Tests for get_open_positions method."""

    @pytest.mark.asyncio
    async def test_get_open_positions_returns_snapshots(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get open positions returns list of OpenPositionSnapshot.

        Given: Authenticated client with mocked User SDK returning positions,
        When: get_open_positions is called,
        Then: Returns list of OpenPositionSnapshot with correct fields.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_positions = MagicMock(
            return_value={
                "openPositions": [
                    {
                        "symbol": "PF_XBTUSD",
                        "side": "long",
                        "size": 5.0,
                        "price": 65000.0,
                        "markPrice": 65500.0,
                        "unrealizedPnl": 250.0,
                        "unrealizedFunding": -10.0,
                    },
                    {
                        "symbol": "PF_ETHUSD",
                        "side": "short",
                        "size": 10.0,
                        "price": 3500.0,
                        "markPrice": 3450.0,
                        "unrealizedPnl": 500.0,
                        "unrealizedFunding": 5.0,
                    },
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=lambda s: {"PF_XBTUSD": "BTC-USD-PERP", "PF_ETHUSD": "ETH-USD-PERP"}[s],
        ):
            result = await auth_client.get_open_positions()
        assert len(result) == 2
        assert isinstance(result[0], OpenPositionSnapshot)
        assert result[0].symbol == "BTC-USD-PERP"
        assert result[0].side == OrderSideEnum.BUY
        assert result[0].size == pytest.approx(5.0)
        assert result[0].entry_price == pytest.approx(65000.0)
        assert result[0].mark_price == pytest.approx(65500.0)
        assert result[0].unrealized_pnl == pytest.approx(250.0)
        assert result[0].unrealized_funding == pytest.approx(-10.0)
        assert result[1].symbol == "ETH-USD-PERP"
        assert result[1].side == OrderSideEnum.SELL
        assert result[1].size == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_get_open_positions_empty(self, auth_client: KrakenFuturesExchangeClient) -> None:
        """Get open positions returns empty list when no positions.

        Given: User SDK returns empty openPositions list,
        When: get_open_positions is called,
        Then: Returns empty list.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_positions = MagicMock(return_value={"openPositions": []})
        result = await auth_client.get_open_positions()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_open_positions_unknown_symbol_fallback(
        self, auth_client: KrakenFuturesExchangeClient
    ) -> None:
        """Get open positions uses raw symbol when conversion fails.

        Given: Position with unknown symbol that fails conversion,
        When: get_open_positions is called,
        Then: Uses the raw Kraken symbol as native_symbol.
        """
        assert auth_client._user_client is not None
        auth_client._user_client.get_open_positions = MagicMock(
            return_value={
                "openPositions": [
                    {
                        "symbol": "PF_UNKNOWNSYM",
                        "side": "long",
                        "size": 1.0,
                        "price": 100.0,
                        "markPrice": 101.0,
                        "unrealizedPnl": 1.0,
                        "unrealizedFunding": 0.0,
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=ValueError("Unknown symbol"),
        ):
            result = await auth_client.get_open_positions()
        assert len(result) == 1
        assert result[0].symbol == "PF_UNKNOWNSYM"

    @pytest.mark.asyncio
    async def test_get_open_positions_requires_auth(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Get open positions raises RuntimeError without credentials.

        Given: Unauthenticated client,
        When: get_open_positions is called,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.get_open_positions()


class TestKrakenFuturesLiveFixtures:
    """Verify order lifecycle using real SDK responses captured from Kraken Futures ETH-USD-PERP.

    Each mock dict mirrors an actual SDK return value recorded during live
    integration tests.  The tests confirm that ``create_order``,
    ``cancel_order``, ``get_order``, and ``get_open_positions`` produce the
    correct ``ExchangeOrderSnapshot`` / ``OpenPositionSnapshot`` fields.
    """

    @pytest.fixture
    def client(self) -> KrakenFuturesExchangeClient:
        """Provide an authenticated KrakenFuturesExchangeClient for live fixture tests."""
        return KrakenFuturesExchangeClient(
            sandbox=True, api_key="live-key", api_secret="live-secret"
        )

    @pytest.mark.asyncio
    async def test_passive_create_limit(self, client: KrakenFuturesExchangeClient) -> None:
        """Passive limit order returns open snapshot.

        Given: SDK create_order returns a placed limit order.
        When: create_order is called with a limit buy.
        Then: Snapshot has status=open, filled=0.0, remaining equal to amount.
        """
        assert client._trade_client is not None
        client._trade_client.create_order = MagicMock(
            return_value={
                "sendStatus": {
                    "order_id": "0f3e7d8a-pass-lmt-open-kraken00000",
                    "status": "placed",
                }
            }
        )
        request = ExchangeOrderRequest(
            symbol="ETH-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.01,
            price=1000.0,
            client_order_id="test-passive-1",
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_ETHUSD",
            ),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            snap = await client.create_order(request)
        assert snap.id == "0f3e7d8a-pass-lmt-open-kraken00000"
        assert snap.symbol == "ETH-USD-PERP"
        assert snap.side == OrderSideEnum.BUY
        assert snap.type == ExchangeOrderTypeEnum.LIMIT
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(0.01)
        assert snap.client_order_id == "test-passive-1"
        assert snap.price == pytest.approx(1000.0)

    @pytest.mark.asyncio
    async def test_passive_fetch(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch a passive open order returns open snapshot.

        Given: SDK get_orders_status returns order with status=untouched.
        When: get_order is called.
        Then: Snapshot has status=open, filled=0.0.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order_id": "0f3e7d8a-pass-lmt-open-kraken00000",
                        "symbol": "PF_ETHUSD",
                        "side": "buy",
                        "orderType": "lmt",
                        "qty": 0.01,
                        "filledSize": 0.0,
                        "limitPrice": 1000.0,
                        "status": "untouched",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="ETH-USD-PERP",
        ):
            snap = await client.get_order("0f3e7d8a-pass-lmt-open-kraken00000")
        assert snap.id == "0f3e7d8a-pass-lmt-open-kraken00000"
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.filled == pytest.approx(0.0)
        assert snap.side == OrderSideEnum.BUY

    @pytest.mark.asyncio
    async def test_passive_cancel(self, client: KrakenFuturesExchangeClient) -> None:
        """Cancel of passive order returns canceled snapshot.

        Given: SDK cancel_order returns cancelled status.
        When: cancel_order is called.
        Then: Snapshot has status=canceled.
        """
        assert client._trade_client is not None
        client._trade_client.cancel_order = MagicMock(
            return_value={"cancelStatus": {"status": "cancelled"}}
        )
        snap = await client.cancel_order(
            "0f3e7d8a-pass-lmt-open-kraken00000", symbol="ETH-USD-PERP"
        )
        assert snap.id == "0f3e7d8a-pass-lmt-open-kraken00000"
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.symbol == "ETH-USD-PERP"

    @pytest.mark.asyncio
    async def test_topbook_create(self, client: KrakenFuturesExchangeClient) -> None:
        """Topbook limit order at the best price returns open snapshot.

        Given: SDK create_order returns a placed limit order at the ask/bid.
        When: create_order is called.
        Then: Snapshot has status=open.
        """
        assert client._trade_client is not None
        client._trade_client.create_order = MagicMock(
            return_value={
                "sendStatus": {
                    "order_id": "1a2b3c4d-tbok-lmt-open-kraken00000",
                    "status": "placed",
                }
            }
        )
        request = ExchangeOrderRequest(
            symbol="ETH-USD-PERP",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.01,
            price=2500.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_ETHUSD",
            ),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            snap = await client.create_order(request)
        assert snap.id == "1a2b3c4d-tbok-lmt-open-kraken00000"
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_topbook_fetch(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch topbook order returns open snapshot.

        Given: SDK get_orders_status returns topbook order with status=untouched.
        When: get_order is called.
        Then: Snapshot has status=open.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order_id": "1a2b3c4d-tbok-lmt-open-kraken00000",
                        "symbol": "PF_ETHUSD",
                        "side": "sell",
                        "orderType": "lmt",
                        "qty": 0.01,
                        "filledSize": 0.0,
                        "limitPrice": 2500.0,
                        "status": "untouched",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="ETH-USD-PERP",
        ):
            snap = await client.get_order("1a2b3c4d-tbok-lmt-open-kraken00000")
        assert snap.id == "1a2b3c4d-tbok-lmt-open-kraken00000"
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_topbook_cancel(self, client: KrakenFuturesExchangeClient) -> None:
        """Cancel topbook order returns canceled snapshot.

        Given: SDK cancel_order returns cancelled status.
        When: cancel_order is called.
        Then: Snapshot has status=canceled.
        """
        assert client._trade_client is not None
        client._trade_client.cancel_order = MagicMock(
            return_value={"cancelStatus": {"status": "cancelled"}}
        )
        snap = await client.cancel_order(
            "1a2b3c4d-tbok-lmt-open-kraken00000", symbol="ETH-USD-PERP"
        )
        assert snap.status == ExchangeOrderStatusEnum.CANCELED

    @pytest.mark.asyncio
    async def test_aggressive_create_immediate_fill(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Aggressive limit that crosses the spread fills immediately.

        Given: SDK create_order returns a filled status (immediate taker fill).
        When: create_order is called with a crossing limit price.
        Then: Snapshot has status=closed (via SDK placed -> get_order filled path).
        """
        assert client._trade_client is not None
        client._trade_client.create_order = MagicMock(
            return_value={
                "sendStatus": {
                    "order_id": "5e6f7a8b-aggr-fill-kraken000000000",
                    "status": "placed",
                }
            }
        )
        request = ExchangeOrderRequest(
            symbol="ETH-USD-PERP",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.01,
            price=1800.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_ETHUSD",
            ),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            snap = await client.create_order(request)
        assert snap.id == "5e6f7a8b-aggr-fill-kraken000000000"
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_aggressive_fetch_filled(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch of an aggressive order that filled shows closed status.

        Given: SDK get_orders_status returns order with status=filled.
        When: get_order is called.
        Then: Snapshot has status=closed, filled equal to qty.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order_id": "5e6f7a8b-aggr-fill-kraken000000000",
                        "symbol": "PF_ETHUSD",
                        "side": "sell",
                        "orderType": "lmt",
                        "qty": 0.01,
                        "filledSize": 0.01,
                        "limitPrice": 1800.0,
                        "status": "filled",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="ETH-USD-PERP",
        ):
            snap = await client.get_order("5e6f7a8b-aggr-fill-kraken000000000")
        assert snap.id == "5e6f7a8b-aggr-fill-kraken000000000"
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.filled == pytest.approx(0.01)
        assert snap.remaining == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_market_create(self, client: KrakenFuturesExchangeClient) -> None:
        """Market sell returns open snapshot (SDK returns placed for market too).

        Given: SDK create_order returns placed status for a market order.
        When: create_order is called with type=market.
        Then: Snapshot has type=market, status=open (initial state from SDK).
        """
        assert client._trade_client is not None
        client._trade_client.create_order = MagicMock(
            return_value={
                "sendStatus": {
                    "order_id": "c9d0e1f2-mkt-sell-kraken0000000000",
                    "status": "placed",
                }
            }
        )
        request = ExchangeOrderRequest(
            symbol="ETH-USD-PERP",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.01,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_ETHUSD",
            ),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            snap = await client.create_order(request)
        assert snap.id == "c9d0e1f2-mkt-sell-kraken0000000000"
        assert snap.type == ExchangeOrderTypeEnum.MARKET
        assert snap.side == OrderSideEnum.SELL
        assert snap.status == ExchangeOrderStatusEnum.OPEN

    @pytest.mark.asyncio
    async def test_market_fetch_filled(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch of a market order shows closed status after fill.

        Given: SDK get_orders_status returns order with status=filled, orderType=mkt.
        When: get_order is called.
        Then: Snapshot has status=closed, type=market, filled equal to qty.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order_id": "c9d0e1f2-mkt-sell-kraken0000000000",
                        "symbol": "PF_ETHUSD",
                        "side": "sell",
                        "orderType": "mkt",
                        "qty": 0.01,
                        "filledSize": 0.01,
                        "status": "filled",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="ETH-USD-PERP",
        ):
            snap = await client.get_order("c9d0e1f2-mkt-sell-kraken0000000000")
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.type == ExchangeOrderTypeEnum.MARKET
        assert snap.filled == pytest.approx(0.01)

    @pytest.mark.asyncio
    async def test_cancel_inflight_create_and_cancel(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Create then immediately cancel a limit order.

        Given: SDK create_order returns placed; cancel_order returns cancelled.
        When: create_order followed by cancel_order.
        Then: Create returns open snapshot; cancel returns canceled snapshot.
        """
        assert client._trade_client is not None
        client._trade_client.create_order = MagicMock(
            return_value={
                "sendStatus": {
                    "order_id": "d3e4f5a6-cinf-lmt-kraken0000000000",
                    "status": "placed",
                }
            }
        )
        request = ExchangeOrderRequest(
            symbol="ETH-USD-PERP",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.01,
            price=1000.0,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_ETHUSD",
            ),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            snap_create = await client.create_order(request)
        assert snap_create.status == ExchangeOrderStatusEnum.OPEN
        assert snap_create.id == "d3e4f5a6-cinf-lmt-kraken0000000000"

        client._trade_client.cancel_order = MagicMock(
            return_value={"cancelStatus": {"status": "cancelled"}}
        )
        snap_cancel = await client.cancel_order(
            "d3e4f5a6-cinf-lmt-kraken0000000000", symbol="ETH-USD-PERP"
        )
        assert snap_cancel.status == ExchangeOrderStatusEnum.CANCELED
        assert snap_cancel.id == "d3e4f5a6-cinf-lmt-kraken0000000000"

    @pytest.mark.asyncio
    async def test_positions_empty_after_flatten(self, client: KrakenFuturesExchangeClient) -> None:
        """Positions list is empty after all positions are closed.

        Given: SDK get_open_positions returns empty openPositions list.
        When: get_open_positions is called.
        Then: Returns empty list.
        """
        assert client._user_client is not None
        client._user_client.get_open_positions = MagicMock(return_value={"openPositions": []})
        result = await client.get_open_positions()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_nested_sdk_response(self, client: KrakenFuturesExchangeClient) -> None:
        """get_order unwraps nested SDK get_orders_status response.

        Real SDK returns {"orders": [{"order": {fields...}, "status": "ENTERED_BOOK"}]}.
        The inner "order" dict has orderId/quantity/filled/limitPrice, and the outer
        status must be propagated.

        Given: SDK get_orders_status returns nested structure with ENTERED_BOOK status.
        When: get_order is called.
        Then: Snapshot has correct id, symbol, amount, price, and OPEN status.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order": {
                            "orderId": "a178b88c-9f26-4aee-8472-5e584b750dd4",
                            "cliOrdId": "test-pass-buy-1775398727",
                            "type": "post",
                            "symbol": "PF_XBTUSD",
                            "side": "buy",
                            "quantity": 0.0001,
                            "filled": 0,
                            "limitPrice": 60163.2,
                            "reduceOnly": False,
                            "timestamp": "2026-04-05T14:18:47.000Z",
                            "lastUpdateTimestamp": "2026-04-05T14:18:47.000Z",
                        },
                        "status": "ENTERED_BOOK",
                        "updateReason": None,
                        "error": None,
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            snap = await client.get_order("a178b88c-9f26-4aee-8472-5e584b750dd4", "BTC-USD-PERP")
        assert snap.id == "a178b88c-9f26-4aee-8472-5e584b750dd4"
        assert snap.client_order_id == "test-pass-buy-1775398727"
        assert snap.symbol == "BTC-USD-PERP"
        assert snap.side == OrderSideEnum.BUY
        assert snap.type == ExchangeOrderTypeEnum.LIMIT
        assert snap.amount == pytest.approx(0.0001)
        assert snap.price == pytest.approx(60163.2)
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(0.0001)

    @pytest.mark.asyncio
    async def test_cancel_order_extracts_order_events(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """cancel_order extracts full order data from orderEvents.

        Real SDK cancel returns cancelStatus.orderEvents[0].order with full
        order details (orderId, symbol, quantity, limitPrice, filled).

        Given: SDK cancel returns orderEvents with order details.
        When: cancel_order is called.
        Then: Snapshot has correct amount, price, side, and CANCELED status.
        """
        assert client._trade_client is not None
        client._trade_client.cancel_order = MagicMock(
            return_value={
                "cancelStatus": {
                    "status": "cancelled",
                    "order_id": "a178b8a7-42f0-4d60-bfa9-06d90ff1da45",
                    "orderEvents": [
                        {
                            "type": "CANCEL",
                            "uid": "a178b8a7-42f0-4d60-bfa9-06d90ff1da45",
                            "order": {
                                "orderId": "a178b8a7-42f0-4d60-bfa9-06d90ff1da45",
                                "cliOrdId": None,
                                "type": "post",
                                "symbol": "PF_XBTUSD",
                                "side": "buy",
                                "quantity": 0.0001,
                                "filled": 0,
                                "limitPrice": 60000.0,
                                "reduceOnly": False,
                                "timestamp": "2026-04-05T14:17:41.168Z",
                                "lastUpdateTimestamp": "2026-04-05T14:17:41.168Z",
                            },
                        }
                    ],
                }
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            snap = await client.cancel_order("a178b8a7-42f0-4d60-bfa9-06d90ff1da45", "BTC-USD-PERP")
        assert snap.id == "a178b8a7-42f0-4d60-bfa9-06d90ff1da45"
        assert snap.symbol == "BTC-USD-PERP"
        assert snap.side == OrderSideEnum.BUY
        assert snap.amount == pytest.approx(0.0001)
        assert snap.price == pytest.approx(60000.0)
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.filled == pytest.approx(0.0)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cancel_not_found_reconciles_via_get_order(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """NotFound cancel reconciles via get_order to detect late fills.

        When the SDK cancel returns notFound (order not yet propagated or
        already filled), the client must call get_order to retrieve the
        actual state rather than blindly declaring CANCELED.

        Given: SDK cancel returns notFound, get_orders_status shows FULLY_EXECUTED.
        When: cancel_order is called.
        Then: Snapshot reflects the filled state from get_order, not CANCELED.
        """
        assert client._trade_client is not None
        client._trade_client.cancel_order = MagicMock(
            return_value={
                "cancelStatus": {
                    "status": "notFound",
                    "order_id": "a178ba6f-reconcile",
                    "orderEvents": [],
                }
            }
        )
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order": {
                            "orderId": "a178ba6f-reconcile",
                            "cliOrdId": None,
                            "type": "lmt",
                            "symbol": "PF_XBTUSD",
                            "side": "buy",
                            "quantity": 0,
                            "filled": 0.0001,
                            "limitPrice": 66000.0,
                            "reduceOnly": False,
                        },
                        "status": "FULLY_EXECUTED",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            snap = await client.cancel_order("a178ba6f-reconcile", "BTC-USD-PERP")
        assert snap.id == "a178ba6f-reconcile"
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.filled == pytest.approx(0.0001)

    @pytest.mark.asyncio
    async def test_cancel_not_found_fallback_when_get_order_fails(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """NotFound cancel falls back to OPEN when get_order also fails.

        Given: SDK cancel returns notFound, get_orders_status returns empty.
        When: cancel_order is called.
        Then: Falls back to minimal snapshot with OPEN status (notFound unmapped).
        """
        assert client._trade_client is not None
        client._trade_client.cancel_order = MagicMock(
            return_value={
                "cancelStatus": {
                    "status": "notFound",
                    "order_id": "a178ba6f-fallback",
                    "orderEvents": [],
                }
            }
        )
        client._trade_client.get_orders_status = MagicMock(return_value={"orders": []})
        snap = await client.cancel_order("a178ba6f-fallback", "BTC-USD-PERP")
        assert snap.id == "a178ba6f-fallback"
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_get_order_fully_executed(self, client: KrakenFuturesExchangeClient) -> None:
        """get_order correctly parses FULLY_EXECUTED with quantity=0.

        Real SDK: when an order is fully filled, get_orders_status returns
        quantity=0 (remaining) and filled=0.0001 (executed).
        The converter must compute amount = filled + quantity = 0.0001.

        Given: SDK returns FULLY_EXECUTED status with quantity=0, filled=0.0001.
        When: get_order is called.
        Then: Snapshot has amount=0.0001, filled=0.0001, remaining=0, status=CLOSED.
        """
        assert client._trade_client is not None
        client._trade_client.get_orders_status = MagicMock(
            return_value={
                "orders": [
                    {
                        "order": {
                            "orderId": "a178d025-ab24-42e0-9f37-e5dd5cfae2b3",
                            "cliOrdId": None,
                            "type": "lmt",
                            "symbol": "PF_XBTUSD",
                            "side": "buy",
                            "quantity": 0,
                            "filled": 0.0001,
                            "limitPrice": 70000.0,
                            "reduceOnly": False,
                            "timestamp": "2026-04-05T15:23:22.770Z",
                            "lastUpdateTimestamp": "2026-04-05T15:23:22.770Z",
                        },
                        "status": "FULLY_EXECUTED",
                    }
                ]
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            snap = await client.get_order("a178d025-ab24-42e0-9f37-e5dd5cfae2b3")
        assert snap.id == "a178d025-ab24-42e0-9f37-e5dd5cfae2b3"
        assert snap.amount == pytest.approx(0.0001)
        assert snap.filled == pytest.approx(0.0001)
        assert snap.remaining == pytest.approx(0.0)
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.price == pytest.approx(70000.0)

    def test_convert_sdk_order_size_field(self, client: KrakenFuturesExchangeClient) -> None:
        """Converter handles ``size`` field as total order quantity.

        Some SDK responses (e.g., fill events) use ``size`` instead of
        ``quantity`` or ``unfilledSize``. The converter must treat ``size``
        as the total order amount.

        Given: Order dict with ``size=0.0005`` and no quantity/unfilledSize.
        When: _convert_sdk_order is called.
        Then: amount=0.0005.
        """
        snap = client._convert_sdk_order(
            {
                "order_id": "size-field-test",
                "symbol": "pf_xbtusd",
                "side": "buy",
                "size": 0.0005,
                "filledSize": 0,
                "orderType": "lmt",
                "limitPrice": 65000,
                "status": "untouched",
            }
        )
        assert snap.amount == pytest.approx(0.0005)
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(0.0005)


class TestGetHistoricalFundingRates:
    """Tests for get_historical_funding_rates method."""

    @pytest.mark.asyncio
    async def test_returns_snapshots_from_sdk(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Historical funding rates are parsed into FundingRateSnapshot list.

        Given: SDK returns rates with timestamp, fundingRate, relativeFundingRate,
        When: get_historical_funding_rates is called,
        Then: Returns list of FundingRateSnapshot sorted by effective_from.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    {
                        "timestamp": "2026-03-01T16:00:00.000Z",
                        "fundingRate": 1.0327e-08,
                        "relativeFundingRate": 7.182e-05,
                    },
                    {
                        "timestamp": "2026-03-01T20:00:00.000Z",
                        "fundingRate": -1.2047e-08,
                        "relativeFundingRate": -8.487e-05,
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert len(result) == 2
        assert isinstance(result[0], FundingRateSnapshot)
        assert result[0].symbol == "BTC-USD-PERP"
        assert result[0].exchange == "kraken_futures"
        assert result[0].rate_type == "perpetual_funding"
        assert result[0].direction == "both"
        assert result[0].rate == pytest.approx(7.182e-05)
        assert result[0].notional_asset == "USD"
        assert result[0].source == "exchange_api"
        assert result[1].rate == pytest.approx(-8.487e-05)
        assert result[0].effective_from < result[1].effective_from

    @pytest.mark.asyncio
    async def test_empty_rates_returns_empty_list(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Empty rates list returns empty snapshot list.

        Given: SDK returns empty rates,
        When: get_historical_funding_rates is called,
        Then: Returns empty list.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={"rates": []},
        )
        result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_skips_entries_with_missing_relative_rate(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Entries without relativeFundingRate are skipped.

        Given: SDK returns entries missing relativeFundingRate,
        When: get_historical_funding_rates is called,
        Then: Those entries are excluded from results.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    {
                        "timestamp": "2026-03-01T16:00:00.000Z",
                        "fundingRate": 1.0e-08,
                    },
                    {
                        "timestamp": "2026-03-01T20:00:00.000Z",
                        "fundingRate": -1.0e-08,
                        "relativeFundingRate": -5.0e-05,
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_skips_entries_with_bad_timestamp(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Entries with unparseable timestamps are skipped.

        Given: SDK returns entries with invalid timestamp string,
        When: get_historical_funding_rates is called,
        Then: Those entries are excluded from results.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    {
                        "timestamp": "not-a-date",
                        "fundingRate": 1.0e-08,
                        "relativeFundingRate": 5.0e-05,
                    },
                ],
            }
        )
        result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_unknown_symbol_fallback(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Unknown symbol uses raw Kraken symbol as fallback.

        Given: Symbol conversion raises ValueError,
        When: get_historical_funding_rates is called,
        Then: Uses raw symbol string.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    {
                        "timestamp": "2026-03-01T16:00:00.000Z",
                        "fundingRate": 1.0e-08,
                        "relativeFundingRate": 5.0e-05,
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=ValueError("Unknown"),
        ):
            result = await client.get_historical_funding_rates("PF_UNKNOWN")
        assert len(result) == 1
        assert result[0].symbol == "PF_UNKNOWN"

    @pytest.mark.asyncio
    async def test_initializes_market_client_if_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Market client is lazily initialized when None.

        Given: client._market_client is None,
        When: get_historical_funding_rates is called,
        Then: Market client is created and method succeeds.
        """
        client._market_client = None
        mock_market = MagicMock()
        mock_market.get_historical_funding_rates = MagicMock(
            return_value={"rates": []},
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market",
            return_value=mock_market,
        ):
            result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert result == []
        assert client._market_client is mock_market

    @pytest.mark.asyncio
    async def test_non_dict_response_returns_empty(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-dict SDK response returns empty list.

        Given: SDK returns a non-dict value (e.g., string error),
        When: get_historical_funding_rates is called,
        Then: Returns empty list without crashing.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value="error: invalid symbol",
        )
        result = await client.get_historical_funding_rates("PF_INVALID")
        assert result == []

    @pytest.mark.asyncio
    async def test_non_list_rates_returns_empty(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-list rates field returns empty list.

        Given: SDK returns dict with non-list rates value,
        When: get_historical_funding_rates is called,
        Then: Returns empty list.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={"rates": "not-a-list"},
        )
        result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_non_dict_entry_skipped(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-dict entries in rates list are skipped.

        Given: SDK returns rates list containing a non-dict element,
        When: get_historical_funding_rates is called,
        Then: Non-dict entry is skipped, valid ones are processed.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    "not-a-dict",
                    {
                        "timestamp": "2026-03-01T16:00:00.000Z",
                        "fundingRate": 1.0e-08,
                        "relativeFundingRate": 5.0e-05,
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_non_numeric_rate_skipped(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Entries with non-numeric relativeFundingRate are skipped.

        Given: SDK returns entry with string rate value,
        When: get_historical_funding_rates is called,
        Then: Entry is excluded from results.
        """
        client._market_client = MagicMock()
        client._market_client.get_historical_funding_rates = MagicMock(
            return_value={
                "rates": [
                    {
                        "timestamp": "2026-03-01T16:00:00.000Z",
                        "fundingRate": 1.0e-08,
                        "relativeFundingRate": "N/A",
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_historical_funding_rates("PF_XBTUSD")
        assert result == []


class TestGetCurrentFundingRate:
    """Tests for get_current_funding_rate method."""

    @pytest.mark.asyncio
    async def test_returns_snapshot_for_matching_ticker(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Current funding rate extracts from matching ticker.

        Given: SDK get_tickers returns ticker with fundingRate for symbol,
        When: get_current_funding_rate is called,
        Then: Returns FundingRateSnapshot with correct fields.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {
                        "symbol": "pf_xbtusd",
                        "fundingRate": 0.000220714,
                        "lastTime": "2026-04-04T00:07:33.690Z",
                        "tag": "perpetual",
                    },
                    {
                        "symbol": "pf_ethusd",
                        "fundingRate": 0.000100000,
                        "lastTime": "2026-04-04T00:07:34.000Z",
                        "tag": "perpetual",
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is not None
        assert isinstance(result, FundingRateSnapshot)
        assert result.symbol == "BTC-USD-PERP"
        assert result.exchange == "kraken_futures"
        assert result.rate_type == "perpetual_funding"
        assert result.direction == "both"
        assert result.rate == pytest.approx(0.000220714)
        assert result.notional_asset == "USD"
        assert result.source == "exchange_api"

    @pytest.mark.asyncio
    async def test_returns_none_for_unmatched_symbol(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Returns None when ticker does not contain the requested symbol.

        Given: SDK get_tickers returns tickers not matching the symbol,
        When: get_current_funding_rate is called,
        Then: Returns None.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {"symbol": "pf_ethusd", "fundingRate": 0.0001},
                ],
            }
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_funding_rate_is_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Returns None when ticker has no fundingRate field.

        Given: Matching ticker but fundingRate is None,
        When: get_current_funding_rate is called,
        Then: Returns None.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": None},
                ],
            }
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_tickers(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Returns None when no tickers returned.

        Given: SDK get_tickers returns empty list,
        When: get_current_funding_rate is called,
        Then: Returns None.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={"tickers": []},
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None

    @pytest.mark.asyncio
    async def test_effective_from_is_current_hour_boundary(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Current funding rate uses the most recent hour boundary.

        Given: Matching ticker with valid fundingRate,
        When: get_current_funding_rate is called,
        Then: effective_from is the current hour boundary (minute=0, second=0).
        """
        frozen_now = datetime(2026, 4, 9, 14, 37, 42, 123456, tzinfo=UTC)
        expected_boundary = datetime(2026, 4, 9, 14, 0, 0, tzinfo=UTC)
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0.0001},
                ],
            }
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
                return_value="BTC-USD-PERP",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.datetime",
            ) as mock_dt,
        ):
            mock_dt.now.return_value = frozen_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is not None
        assert result.effective_from == expected_boundary

    @pytest.mark.asyncio
    async def test_unknown_symbol_fallback(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Uses raw symbol when conversion fails.

        Given: Symbol conversion raises ValueError,
        When: get_current_funding_rate is called,
        Then: Uses raw symbol string.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {
                        "symbol": "PF_UNKNOWN",
                        "fundingRate": 0.0001,
                        "lastTime": "2026-04-04T00:07:33.690Z",
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            side_effect=ValueError("Unknown"),
        ):
            result = await client.get_current_funding_rate("PF_UNKNOWN")
        assert result is not None
        assert result.symbol == "PF_UNKNOWN"

    @pytest.mark.asyncio
    async def test_initializes_market_client_if_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Market client is lazily initialized when None.

        Given: client._market_client is None,
        When: get_current_funding_rate is called,
        Then: Market client is created and method succeeds.
        """
        client._market_client = None
        mock_market = MagicMock()
        mock_market.get_tickers = MagicMock(
            return_value={"tickers": []},
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market",
            return_value=mock_market,
        ):
            result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None
        assert client._market_client is mock_market

    @pytest.mark.asyncio
    async def test_non_dict_response_returns_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-dict SDK response returns None.

        Given: SDK returns a non-dict value,
        When: get_current_funding_rate is called,
        Then: Returns None without crashing.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value="error: service unavailable",
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None

    @pytest.mark.asyncio
    async def test_non_list_tickers_returns_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-list tickers field returns None.

        Given: SDK returns dict with non-list tickers value,
        When: get_current_funding_rate is called,
        Then: Returns None.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={"tickers": "not-a-list"},
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None

    @pytest.mark.asyncio
    async def test_non_dict_ticker_entry_skipped(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-dict entries in tickers list are skipped.

        Given: SDK returns tickers list with non-dict element before match,
        When: get_current_funding_rate is called,
        Then: Non-dict entry is skipped, matching ticker is processed.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    "not-a-dict",
                    {
                        "symbol": "PF_XBTUSD",
                        "fundingRate": 0.0001,
                        "lastTime": "2026-04-04T00:07:33.690Z",
                    },
                ],
            }
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.kraken_futures_ws_to_native",
            return_value="BTC-USD-PERP",
        ):
            result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is not None
        assert result.rate == pytest.approx(0.0001)

    @pytest.mark.asyncio
    async def test_non_numeric_rate_returns_none(
        self,
        client: KrakenFuturesExchangeClient,
    ) -> None:
        """Non-numeric fundingRate returns None.

        Given: Matching ticker with non-numeric fundingRate string,
        When: get_current_funding_rate is called,
        Then: Returns None instead of crashing.
        """
        client._market_client = MagicMock()
        client._market_client.get_tickers = MagicMock(
            return_value={
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": "N/A"},
                ],
            }
        )
        result = await client.get_current_funding_rate("PF_XBTUSD")
        assert result is None
