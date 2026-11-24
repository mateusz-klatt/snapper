"""Tests for Kraken ledger reads on the executor's private client."""

import asyncio
import time
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import ccxt
import pytest

from snapper.infrastructure.exchanges.errors import CircuitBreakerOpenError
from snapper.infrastructure.exchanges.implementations import kraken as kr
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken import PrivateRestBusyError
from snapper.infrastructure.exchanges.implementations.kraken import _optional_position_text
from snapper.infrastructure.exchanges.implementations.kraken import _parse_native_position_entry
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPermissionMissingError
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget


class _Ccxt:
    """Stand-in for the one ccxt client owned by a Kraken exchange client."""

    def __init__(self, ledgers: object, trade_balance: object) -> None:
        self.nonce = self._nonce
        self.ledgers_body = ledgers
        self.trade_body = trade_balance
        self.ledger_params: list[dict[str, object]] = []
        self.trade_params: list[dict[str, object]] = []
        self.constructor_kwargs: dict[str, object] = {}
        self.proxies: dict[str, str] = {}

    def _nonce(self) -> int:
        return 11

    async def privatePostLedgers(self, params: dict[str, object]) -> object:
        self.ledger_params.append(params)
        if isinstance(self.ledgers_body, Exception):
            raise self.ledgers_body
        return self.ledgers_body

    async def privatePostTradeBalance(self, params: dict[str, object]) -> object:
        self.trade_params.append(params)
        if isinstance(self.trade_body, Exception):
            raise self.trade_body
        return self.trade_body


class _Retry:
    """Record the private call and invoke it the way _with_retry does."""

    def __init__(self, client: _Ccxt) -> None:
        self.client = client
        self.functions: list[object] = []
        self.kwargs: list[dict[str, object]] = []

    async def __call__(self, func: object, *args: object, **kwargs: object) -> object:
        self.functions.append(func)
        self.kwargs.append(kwargs)
        if not callable(func):
            raise TypeError("private call is not callable")
        return await func(*args)


def _client(ledgers: object, trade_balance: object) -> tuple[KrakenExchangeClient, _Ccxt, _Retry]:
    """Build a client that reuses one ccxt instance and records private calls."""
    ccxt_client = _Ccxt(ledgers, trade_balance)
    main_client = _Ccxt(ledgers, trade_balance)
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    client.api_key = "key"
    client.api_secret = "secret"
    client._ccxt_client = main_client
    client._ledger_ccxt_client = ccxt_client
    retry = _Retry(ccxt_client)
    client._with_retry = retry
    return client, ccxt_client, retry


def _real_ledger_client(ledgers: object) -> KrakenExchangeClient:
    """Build a real retry client with an isolated CCXT ledger endpoint double."""
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    client._ledger_ccxt_client = _Ccxt(ledgers, _trade_body())
    return client


async def _until_private_rest_waiter(client: KrakenExchangeClient) -> None:
    """Yield until one private call is queued, tolerating local REST-slot backoff."""
    while client._private_rest_waiters < 1:
        await asyncio.sleep(0.01)


def _ledger_body() -> dict[str, object]:
    """Return one valid one-row Ledgers envelope."""
    return {
        "error": [],
        "result": {
            "ledger": {
                "LID1": {
                    "refid": "RID",
                    "time": "1700000000.0",
                    "type": "margin",
                    "subtype": "",
                    "aclass": "currency",
                    "asset": "ZEUR",
                    "amount": "1.25",
                    "fee": "0.01",
                    "balance": "9.00",
                }
            },
            "count": 1,
        },
    }


def _trade_body() -> dict[str, object]:
    """Return one valid TradeBalance envelope."""
    return {
        "error": [],
        "result": {"eb": "1", "tb": "1", "m": "0", "n": "1", "c": "0", "v": "0", "ml": "2"},
    }


def _info(**overrides: object) -> dict[str, object]:
    """Return one unclosed Kraken position payload."""
    info: dict[str, object] = {
        "pair": "XXBTZEUR",
        "type": "sell",
        "vol": "0.01000000",
        "vol_closed": "0.00000000",
        "cost": "500.00000",
        "value": "510.00000",
        "net": "-10.00000",
        "fee": "0.13000",
        "rollovertm": "1785585600",
    }
    info.update(overrides)
    return info


def test_ledger_client_mirrors_constructor_nonce_and_proxy_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lazy client differs only by timeout and has identical egress metadata.

    Given a CCXT constructor double and a connected main-client nonce,
    When the ledger client is built twice,
    Then one cached client receives all main options, a 10-second timeout, the
        shared nonce, and the same proxy-target configuration without load_markets.
    """
    constructed: list[_Ccxt] = []

    def build(kwargs: dict[str, object]) -> _Ccxt:
        client = _Ccxt(_ledger_body(), _trade_body())
        client.constructor_kwargs = dict(kwargs)
        constructed.append(client)
        return client

    monkeypatch.setattr(kr.ccxt, "kraken", build)
    client = KrakenExchangeClient(
        api_key="key",
        api_secret="secret",
        enable_rate_limit=False,
    )
    try:
        main = client._ccxt_client
        main.nonce = kr._kraken_time_nonce
        ledger = client._get_ledger_ccxt_client()
        assert client._get_ledger_ccxt_client() is ledger
        assert len(constructed) == 2
        assert main.constructor_kwargs["timeout"] == 30000
        assert ledger.constructor_kwargs == {
            **main.constructor_kwargs,
            "timeout": kr._LEDGER_READ_TIMEOUT_MS,
        }
        assert ledger.constructor_kwargs["timeout"] == 10000
        assert ledger.nonce is main.nonce
        with patch.object(kr.time, "time", return_value=1700000000.25):
            assert ledger.nonce() == main.nonce() == 170000000025000000
        main_target = kr.ccxt_proxy_target(main)
        ledger_target = kr.ccxt_proxy_target(ledger)
        assert main_target.client is main
        assert ledger_target.client is ledger
        assert (
            ledger_target.session_attr,
            ledger_target.proxy_attr,
            ledger_target.proxy_attr_shape,
        ) == (
            main_target.session_attr,
            main_target.proxy_attr,
            main_target.proxy_attr_shape,
        )
        assert ledger.proxies == main.proxies
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_ledger_reads_use_the_dedicated_private_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ledger reads use only the dedicated client and its own egress target.

    Given separate main and ledger CCXT instances,
    When Ledgers and TradeBalance are read,
    Then both calls use the ledger instance and leave the main client untouched.
    """
    client, ccxt_client, retry = _client(_ledger_body(), _trade_body())
    nonce = ccxt_client.nonce

    def _reject_user(*args: object, **kwargs: object) -> object:
        raise AssertionError(args or kwargs)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.kraken.User",
        _reject_user,
    )
    page = await client.read_venue_ledger_page(10, 20, 5)
    balance = await client.read_trade_balance()
    assert page.count == 1
    assert page.entries[0].venue_ledger_id == "LID1"
    assert balance.eb == "1"
    assert getattr(retry.functions[0], "__self__", None) is ccxt_client
    assert getattr(retry.functions[1], "__self__", None) is ccxt_client
    assert getattr(retry.functions[0], "__func__", None) is _Ccxt.privatePostLedgers
    assert getattr(retry.functions[1], "__func__", None) is _Ccxt.privatePostTradeBalance
    assert ccxt_client.ledger_params == [{"start": 10, "end": 20, "ofs": 5}]
    assert ccxt_client.trade_params == [{}]
    assert client._ledger_ccxt_client is ccxt_client
    assert client._ccxt_client.ledger_params == []
    assert client._ccxt_client.trade_params == []
    assert ccxt_client.nonce is nonce
    assert ccxt_client.nonce() == 11
    ledger_kwargs, trade_kwargs = retry.kwargs
    assert ledger_kwargs["egress_kind"] == "private_idempotent_read"
    assert ledger_kwargs["egress_operation"] == "privatePostLedgers"
    assert ledger_kwargs["retry_network_errors"] is False
    assert ledger_kwargs["retry_rate_limit_errors"] is False
    assert trade_kwargs["retry_rate_limit_errors"] is False
    assert trade_kwargs["egress_operation"] == "privatePostTradeBalance"
    assert isinstance(ledger_kwargs["egress_target"], RestProxyTarget)
    target = ledger_kwargs["egress_target"]
    assert isinstance(target, RestProxyTarget)
    assert target.client is ccxt_client


@pytest.mark.asyncio
async def test_ledger_reads_require_credentials_on_the_same_client() -> None:
    """Missing credentials fail before a private call.

    Given a client with no key and a client with a key but no secret,
    When a ledger page is requested,
    Then both raise the ledger credential error and do not call ccxt.
    """
    missing_key, ccxt_client, retry = _client(_ledger_body(), _trade_body())
    missing_key.api_key = None
    missing_secret, _, _ = _client(_ledger_body(), _trade_body())
    missing_secret.api_secret = None
    with pytest.raises(RuntimeError, match="API credentials required for ledger"):
        await missing_key.read_venue_ledger_page(1, 2, 0)
    with pytest.raises(RuntimeError, match="API credentials required for ledger"):
        await missing_secret.read_trade_balance()
    assert ccxt_client.ledger_params == []
    assert retry.functions == []


@pytest.mark.asyncio
async def test_ledger_read_skips_held_lock_and_trading_still_waits() -> None:
    """A skipped ledger read neither invokes CCXT nor leaks its skip context.

    Given a held private lock and a ledger read that skips atomically,
    When a trading invocation follows,
    Then it waits for the lock normally and executes after release.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    ledger_client = _Ccxt(_ledger_body(), _trade_body())
    client._ledger_ccxt_client = ledger_client
    endpoint = AsyncMock()
    trading_calls: list[str] = []

    async def trading() -> str:
        trading_calls.append("trading")
        return "filled"

    await client._private_rest_lock.acquire()
    released = False
    trading_task: asyncio.Task[str] | None = None
    try:
        with (
            patch.object(ledger_client, "privatePostLedgers", endpoint),
            pytest.raises(PrivateRestBusyError),
        ):
            await client.read_venue_ledger_page(1, 2, 0)
        endpoint.assert_not_awaited()
        assert kr._BUDGETED_LEDGER_READ.get() is False
        assert client._circuit_failures == 0
        trading_task = asyncio.create_task(client._with_retry(trading))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert client._private_rest_waiters == 1
        assert not trading_task.done()
        assert trading_calls == []
        client._private_rest_lock.release()
        released = True
        assert await trading_task == "filled"
        assert trading_calls == ["trading"]
    finally:
        if not released:
            client._private_rest_lock.release()
        if trading_task is not None:
            await trading_task
        await asyncio.sleep(0)
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_ledger_read_skips_an_unlocked_lock_with_a_waiter() -> None:
    """A released lock with a queued trader is still busy for ledger reads.

    Given a trader queued on a lock that has just been released but not resumed,
    When a ledger read attempts atomic acquisition,
    Then it raises PrivateRestBusyError without invoking the endpoint.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    ledger_client = _Ccxt(_ledger_body(), _trade_body())
    client._ledger_ccxt_client = ledger_client
    endpoint = AsyncMock()

    async def waiting_trade() -> str:
        return "waited"

    await client._private_rest_lock.acquire()
    released = False
    waiter = asyncio.create_task(client._run_private_rest(waiting_trade, operation="waiting_trade"))
    try:
        await asyncio.sleep(0)
        assert client._private_rest_waiters == 1
        client._private_rest_lock.release()
        released = True
        assert not client._private_rest_lock.locked()
        assert client.private_rest_busy()
        with (
            patch.object(ledger_client, "privatePostLedgers", endpoint),
            pytest.raises(PrivateRestBusyError),
        ):
            await client.read_venue_ledger_page(1, 2, 0)
        endpoint.assert_not_awaited()
        assert kr._BUDGETED_LEDGER_READ.get() is False
        assert await waiter == "waited"
    finally:
        if not released:
            client._private_rest_lock.release()
        await waiter
        await asyncio.sleep(0)
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_permission_denied_becomes_ledger_permission_missing() -> None:
    """Permission failures become one typed error and other failures propagate.

    Given a PermissionDenied error, a permission message, a typed permission error,
        and an unrelated runtime error,
    When each is raised by the existing private method,
    Then permission cases raise KrakenLedgerPermissionMissingError and the other error
        stays a RuntimeError.
    """
    denied, _, _ = _client(ccxt.PermissionDenied("nope"), _trade_body())
    with pytest.raises(KrakenLedgerPermissionMissingError):
        await denied.read_venue_ledger_page(1, 2, 0)
    message, _, _ = _client(RuntimeError("EAPI:Invalid permissions"), _trade_body())
    with pytest.raises(KrakenLedgerPermissionMissingError, match="EAPI:Invalid permissions"):
        await message.read_venue_ledger_page(1, 2, 0)
    typed = KrakenLedgerPermissionMissingError("denied")
    explicit, _, _ = _client(typed, _trade_body())
    with pytest.raises(KrakenLedgerPermissionMissingError) as caught:
        await explicit.read_venue_ledger_page(1, 2, 0)
    assert caught.value is typed
    other, _, _ = _client(_ledger_body(), RuntimeError("network"))
    with pytest.raises(RuntimeError, match="network"):
        await other.read_trade_balance()
    envelope, _, _ = _client({"error": ["Invalid permissions"], "result": {}}, _trade_body())
    with pytest.raises(KrakenLedgerPermissionMissingError, match="Invalid permissions"):
        await envelope.read_venue_ledger_page(1, 2, 0)


@pytest.mark.asyncio
async def test_malformed_ledger_envelope_raises_value_error() -> None:
    """A malformed private envelope is a parse error.

    Given a Ledgers body without a ledger object,
    When the existing client reads it,
    Then ValueError is raised.
    """
    client, _, _ = _client({"error": [], "result": {}}, _trade_body())
    with pytest.raises(ValueError, match="malformed"):
        await client.read_venue_ledger_page(1, 2, 0)


def test_position_evidence_fields_are_retained_without_partial_close() -> None:
    """Keep vol_closed, fee, and rollovertm and still refuse a partial close.

    Given an unclosed row, a row with blank evidence, and a partially closed row,
    When each row is parsed,
    Then evidence is retained only when the venue sent it and the partial close raises.
    """
    observed_at = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
    parsed = _parse_native_position_entry(
        {"symbol": "BTC/EUR", "info": _info()},
        observed_at,
    )
    assert parsed.vol_closed == 0.0
    assert parsed.fee == "0.13000"
    assert parsed.rollovertm == "1785585600"
    blank = _parse_native_position_entry(
        {"symbol": "BTC/EUR", "info": _info(fee="  ", rollovertm=None)},
        observed_at,
    )
    assert blank.vol_closed == 0.0
    assert blank.fee is None
    assert blank.rollovertm is None
    assert _optional_position_text({"fee": "0.13000"}, "fee") == "0.13000"
    assert _optional_position_text({"fee": "  "}, "fee") is None
    assert _optional_position_text({"fee": 1}, "fee") == "1"
    assert _optional_position_text({}, "fee") is None
    with pytest.raises(ValueError, match="not faithfully interpretable"):
        _parse_native_position_entry(
            {"symbol": "BTC/EUR", "info": _info(vol_closed="0.00200000")},
            observed_at,
        )


@pytest.mark.asyncio
async def test_disconnect_closes_main_and_dedicated_ledger_sessions() -> None:
    """Disconnect closes both CCXT sessions without touching their identities.

    Given existing main and lazily built ledger clients with open sessions,
    When disconnect runs,
    Then both session close methods and the normal worker shutdown are called.
    """
    client, ledger, _ = _client(_ledger_body(), _trade_body())
    main = client._ccxt_client
    main.session = MagicMock()
    ledger.session = MagicMock()
    close_ws = AsyncMock()
    shutdown = MagicMock()
    with (
        patch.object(client, "_close_ws_client", close_ws),
        patch.object(client, "_shutdown_rest_pool", shutdown),
    ):
        await client.disconnect()
    close_ws.assert_awaited_once()
    main.session.close.assert_called_once_with()
    ledger.session.close.assert_called_once_with()
    shutdown.assert_called_once_with()
    assert client._ccxt_client is main
    assert client._ledger_ccxt_client is ledger


@pytest.mark.asyncio
async def test_six_ledger_timeouts_do_not_open_or_delay_trading_circuit() -> None:
    """Six observation timeouts leave trading circuit state untouched.

    Given six consecutive CCXT RequestTimeout failures from ledger reads,
    When a classified private trading call follows,
    Then no observation failure is recorded, the breaker stays closed, and trading runs.
    """
    client = _real_ledger_client(ccxt.RequestTimeout("ledger timeout"))

    async def trading() -> str:
        return "trading-ok"

    try:
        with patch.object(
            client,
            "_record_unretried_network_failure",
            wraps=client._record_unretried_network_failure,
        ) as record:
            for _ in range(6):
                with pytest.raises(TimeoutError, match="ledger timeout"):
                    await client.read_venue_ledger_page(1, 2, 0)
            record.assert_not_called()
        assert client._circuit_failures == 0
        assert client._circuit_open_until == 0.0
        assert kr._BUDGETED_LEDGER_READ.get() is False
        result = await client._with_retry(
            trading,
            retry_network_errors=False,
            egress_kind="private_mutation",
            egress_operation="test_trading_after_ledger_timeouts",
            egress_target=kr.ccxt_proxy_target(client._ccxt_client),
        )
        assert result == "trading-ok"
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_successful_ledger_read_preserves_trading_failure_count() -> None:
    """A successful observation cannot mask an existing trading outage.

    Given three trading circuit failures,
    When one ledger read succeeds,
    Then the failure count remains three.
    """
    client = _real_ledger_client(_ledger_body())
    client._circuit_failures = 3
    try:
        page = await client.read_venue_ledger_page(1, 2, 0)
        assert page.count == 1
        assert client._circuit_failures == 3
        assert client._circuit_open_until == 0.0
        assert kr._BUDGETED_LEDGER_READ.get() is False
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_trading_network_failure_still_trips_circuit_breaker() -> None:
    """The budgeted-read exemption never changes trading failure accounting.

    Given a classified trading call one network failure from the threshold,
    When it fails without a retry,
    Then the trading breaker opens as before.
    """
    client = _real_ledger_client(_ledger_body())
    client._circuit_failures = client._max_failures - 1

    async def trading_failure() -> None:
        raise ccxt.NetworkError("trading offline")

    try:
        with pytest.raises(ccxt.NetworkError, match="trading offline"):
            await client._with_retry(
                trading_failure,
                retry_network_errors=False,
                egress_kind="private_mutation",
                egress_operation="test_trading_network_failure",
                egress_target=kr.ccxt_proxy_target(client._ccxt_client),
            )
        assert client._circuit_failures == client._max_failures
        assert client._circuit_open_until > time.time()
        assert kr._BUDGETED_LEDGER_READ.get() is False
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("starting_failures", "trading_fails", "expected_failures"),
    [(0, True, 1), (2, False, 0)],
)
async def test_budgeted_marker_does_not_leak_into_concurrent_trading_call(
    starting_failures: int,
    trading_fails: bool,
    expected_failures: int,
) -> None:
    """A trading call in another task keeps normal circuit accounting.

    Given a ledger read in flight in one task, holding the private lock,
    When a classified trading call in a second task queues behind it and then
        fails with a network error or succeeds,
    Then the trading failure is counted or the count is reset as usual, because
        the budgeted-read marker is scoped to the ledger task only.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    ledger_client = _Ccxt(_ledger_body(), _trade_body())
    client._ledger_ccxt_client = ledger_client
    client._circuit_failures = starting_failures
    ledger_started = asyncio.Event()
    release_ledger = asyncio.Event()

    async def blocking_ledgers(params: dict[str, object]) -> object:
        ledger_client.ledger_params.append(params)
        ledger_started.set()
        await release_ledger.wait()
        return ledger_client.ledgers_body

    async def trading() -> str:
        if trading_fails:
            raise ccxt.RequestTimeout("trading timeout")
        return "trading-ok"

    async def run_trading() -> str | None:
        try:
            result: str = await client._with_retry(
                trading,
                retry_network_errors=False,
                egress_kind="private_mutation",
                egress_operation="test_concurrent_trading",
                egress_target=kr.ccxt_proxy_target(client._ccxt_client),
            )
        except ccxt.RequestTimeout:
            return None
        return result

    try:
        with patch.object(ledger_client, "privatePostLedgers", blocking_ledgers):
            ledger_task = asyncio.create_task(client.read_venue_ledger_page(1, 2, 0))
            await asyncio.wait_for(ledger_started.wait(), timeout=5)
            trading_task = asyncio.create_task(run_trading())
            await asyncio.wait_for(_until_private_rest_waiter(client), timeout=5)
            release_ledger.set()
            page = await asyncio.wait_for(ledger_task, timeout=5)
            trading_result = await asyncio.wait_for(trading_task, timeout=5)
        assert page.count == 1
        assert ledger_client.ledger_params == [{"start": 1, "end": 2, "ofs": 0}]
        assert trading_result == (None if trading_fails else "trading-ok")
        assert client._circuit_failures == expected_failures
        assert kr._BUDGETED_LEDGER_READ.get() is False
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_open_trading_circuit_refuses_ledger_read_and_resets_marker() -> None:
    """An existing trading-open breaker still refuses reads before CCXT entry.

    Given an open trading circuit,
    When a ledger read is attempted and refused,
    Then no endpoint call occurs, trading state is preserved, and the marker resets.
    """
    client = _real_ledger_client(_ledger_body())
    client._circuit_failures = 3
    client._circuit_open_until = time.time() + 60.0
    try:
        with pytest.raises(CircuitBreakerOpenError, match="Circuit breaker open"):
            await client.read_venue_ledger_page(1, 2, 0)
        ledger = client._ledger_ccxt_client
        assert ledger is not None
        assert ledger.ledger_params == []
        assert client._circuit_failures == 3
        assert kr._BUDGETED_LEDGER_READ.get() is False
    finally:
        client._shutdown_rest_pool()


@pytest.mark.asyncio
async def test_budgeted_rate_limit_and_permission_errors_do_not_retry() -> None:
    """Budgeted private calls never hide extra counter spending.

    Given the real retry wrapper and a private endpoint rejecting a read,
    When ledger or TradeBalance observation calls it,
    Then only one attempt occurs for rate limits, missing permission, and network errors.
    """
    for error in (
        ccxt.RateLimitExceeded("slow down"),
        ccxt.PermissionDenied("permission missing"),
        ccxt.NetworkError("disconnected"),
        ccxt.RequestTimeout("timed out"),
        PrivateRestBusyError("busy"),
    ):
        client, _, _ = _client(_ledger_body(), _trade_body())
        client._circuit_open_until = 0.0
        client._circuit_failures = 0
        client._max_failures = 5
        client._with_retry = KrakenExchangeClient._with_retry.__get__(client)
        invoke = AsyncMock(side_effect=error)
        with (
            patch.object(client, "_invoke_with_retry_slot", invoke),
            patch.object(client, "_record_unretried_network_failure") as record,
        ):
            expected = type(error)
            if isinstance(error, ccxt.PermissionDenied):
                expected = KrakenLedgerPermissionMissingError
            if isinstance(error, ccxt.RequestTimeout):
                expected = TimeoutError
            with pytest.raises(expected):
                await client.read_venue_ledger_page(10, 20, 0)
        invoke.assert_awaited_once()
        assert client._circuit_failures == 0
        assert kr._BUDGETED_LEDGER_READ.get() is False
        record.assert_not_called()


@pytest.mark.parametrize("value", [1785585600, 1785585600.5, "1785585600"])
def test_numeric_position_evidence_is_retained(value: int | float | str) -> None:
    """Numeric venue evidence survives serialization.

    Given a numeric or textual rollover timestamp,
    When a position is parsed,
    Then its exact rendered value is retained without changing position arithmetic.
    """
    parsed = _parse_native_position_entry(
        {"symbol": "BTC/EUR", "info": _info(rollovertm=value)},
        datetime(2026, 11, 23, 18, 0, tzinfo=UTC),
    )
    assert parsed.rollovertm == str(value)


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), None])
def test_invalid_numeric_position_evidence_is_absent(value: object) -> None:
    """Invalid evidence does not masquerade as a venue timestamp.

    Given a boolean, nonfinite number, or absent timestamp,
    When the optional field is read,
    Then no evidence string is invented.
    """
    assert _optional_position_text({"rollovertm": value}, "rollovertm") is None
