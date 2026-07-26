"""Tests for test_live_orders script."""

import json
from collections.abc import Awaitable
from collections.abc import Callable
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.test_live_orders import EXCHANGE_RUNNERS
from scripts.test_live_orders import emit
from scripts.test_live_orders import get_settings
from scripts.test_live_orders import main
from scripts.test_live_orders import run_kraken_futures
from scripts.test_live_orders import run_kraken_spot
from scripts.test_live_orders import run_walutomat
from scripts.test_live_orders import safe_get_ticker
from scripts.test_live_orders import snapshot_to_dict
from snapper.core.ids import is_uuid7
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot

LiveRunner = Callable[[SimpleNamespace, list[str] | None], Awaitable[None]]


def _make_settings(**overrides: str) -> SimpleNamespace:
    """Create mock settings with optional API keys.

    Args:
        **overrides: Key-value pairs to override defaults.

    Returns:
        SimpleNamespace with exchange credentials.
    """
    defaults = {
        "kraken_api_key": "",
        "kraken_api_secret": "",
        "kraken_futures_api_key": "",
        "kraken_futures_api_secret": "",
        "walutomat_api_key": "",
        "walutomat_private_key": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_ticker(symbol: str, bid: float, ask: float) -> TickerSnapshot:
    """Create a deterministic ticker snapshot.

    Args:
        symbol: Native exchange symbol.
        bid: Best bid price.
        ask: Best ask price.

    Returns:
        TickerSnapshot with midpoint as last price.
    """
    return TickerSnapshot(symbol=symbol, bid=bid, ask=ask, last=(bid + ask) / 2, timestamp=1.0)


def _make_order_snapshot(
    order_id: str,
    symbol: str,
    side: OrderSideEnum,
    amount: float,
    price: float,
    status: ExchangeOrderStatusEnum = ExchangeOrderStatusEnum.OPEN,
) -> ExchangeOrderSnapshot:
    """Create an order snapshot for runner client mocks.

    Args:
        order_id: Exchange order identifier.
        symbol: Native exchange symbol.
        side: Order side.
        amount: Order amount.
        price: Limit price.
        status: Snapshot status.

    Returns:
        ExchangeOrderSnapshot with coherent fill quantities.
    """
    filled = amount if status == ExchangeOrderStatusEnum.CLOSED else 0.0
    remaining = 0.0 if status == ExchangeOrderStatusEnum.CLOSED else amount
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id=f"client-{order_id}",
        symbol=symbol,
        side=side,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=amount,
        price=price,
        status=status,
        filled=filled,
        remaining=remaining,
        timestamp=1.0,
    )


def _make_exchange_client(
    ticker: TickerSnapshot,
    create_snaps: list[ExchangeOrderSnapshot],
    get_snaps: list[ExchangeOrderSnapshot],
    cancel_snaps: list[ExchangeOrderSnapshot],
) -> MagicMock:
    """Create an async exchange client double.

    Args:
        ticker: Ticker returned by get_ticker.
        create_snaps: Snapshots returned by create_order in call order.
        get_snaps: Snapshots returned by get_order in call order.
        cancel_snaps: Snapshots returned by cancel_order in call order.

    Returns:
        MagicMock implementing the async client context manager protocol.
    """
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_ticker = AsyncMock(return_value=ticker)
    client.create_order = AsyncMock(side_effect=create_snaps)
    client.get_order = AsyncMock(side_effect=get_snaps)
    client.cancel_order = AsyncMock(side_effect=cancel_snaps)
    return client


def _json_lines(capsys: pytest.CaptureFixture[str]) -> list[JsonObject]:
    """Parse JSON fixture lines emitted to stdout.

    Args:
        capsys: Pytest capture fixture.

    Returns:
        List of parsed JSON objects.
    """
    output = capsys.readouterr().out.strip()
    return [cast(JsonObject, json.loads(line)) for line in output.splitlines() if line.strip()]


def _line_data(line: JsonObject) -> dict[str, JsonValue]:
    """Return the structured data payload from an emitted fixture.

    Args:
        line: Parsed fixture line.

    Returns:
        Nested fixture data object.
    """
    data = line["data"]
    assert isinstance(data, dict)
    return data


def _created_requests(client: MagicMock) -> list[ExchangeOrderRequest]:
    """Return all ExchangeOrderRequest objects passed to create_order.

    Args:
        client: Mock exchange client.

    Returns:
        Create order requests in await order.
    """
    requests: list[ExchangeOrderRequest] = []
    for call in client.create_order.await_args_list:
        request = call.args[0]
        assert isinstance(request, ExchangeOrderRequest)
        requests.append(request)
    return requests


def _assert_ids_unique_and_well_formed(client: MagicMock, expected_count: int) -> None:
    """Assert every submit in a run carried a distinct, uuid7-shaped id.

    Correlation ids are asserted by shape and distinctness rather than
    by literal value. A repeated id is the collision that turns into a
    venue duplicate-id refusal, which the submit path does not classify
    as ambiguous and which therefore becomes a false REJECTED plus a
    sweep-exempting durable row for a possibly-live order.

    Args:
        client: Mock exchange client the runner submitted through.
        expected_count: Number of submits the run should have made.
    """
    ids = [request.client_order_id for request in _created_requests(client)]
    assert len(ids) == expected_count
    assert all(is_uuid7(value) for value in ids)
    assert len(set(ids)) == expected_count


async def _run_with_client(
    runner: LiveRunner,
    settings: SimpleNamespace,
    client_patch_target: str,
    client: MagicMock,
    scenarios: list[str] | None,
) -> MagicMock:
    """Run a live-order runner with a patched exchange client.

    Args:
        runner: Runner coroutine under test.
        settings: Credentials namespace for the runner.
        client_patch_target: Import path to the exchange client constructor.
        client: Mock client returned by the constructor.
        scenarios: Scenario filter passed to the runner.

    Returns:
        Mock constructor created by patch.
    """
    with (
        patch(client_patch_target, return_value=client) as constructor,
        patch("scripts.test_live_orders.asyncio.sleep", new_callable=AsyncMock),
    ):
        await runner(settings, scenarios)
    return constructor


def test_snapshot_to_dict_converts_enums() -> None:
    """Verify snapshot_to_dict converts enums to strings.

    Given: ExchangeOrderSnapshot with enum fields.
    When: snapshot_to_dict is called.
    Then: Enum values are converted to their string representations.
    """
    snap = ExchangeOrderSnapshot(
        id="test-id",
        client_order_id="cli-1",
        symbol="BTC-EUR",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=0.001,
        price=50000.0,
        status=ExchangeOrderStatusEnum.OPEN,
        filled=0.0,
        remaining=0.001,
        timestamp=1234567890.0,
    )
    result = snapshot_to_dict(snap)
    assert result["side"] == "buy"
    assert result["type"] == "limit"
    assert result["status"] == "open"
    assert result["id"] == "test-id"
    assert result["amount"] == 0.001


def test_emit_outputs_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify emit prints structured JSON line.

    Given: Exchange, scenario, step, and data parameters.
    When: emit is called.
    Then: Stdout contains valid JSON with all fields.
    """
    emit("kraken", "passive_buy", "create", {"id": "abc"})
    output = capsys.readouterr().out.strip()
    parsed = json.loads(output)
    assert parsed["exchange"] == "kraken"
    assert parsed["scenario"] == "passive_buy"
    assert parsed["step"] == "create"
    assert parsed["data"] == {"id": "abc"}


def test_exchange_runners_has_all_exchanges() -> None:
    """Verify EXCHANGE_RUNNERS maps all live exchanges.

    Given: The EXCHANGE_RUNNERS constant.
    When: Keys are examined.
    Then: walutomat, kraken, kraken_futures all present.
    """
    assert set(EXCHANGE_RUNNERS.keys()) == {"walutomat", "kraken", "kraken_futures"}


@pytest.mark.asyncio
async def test_safe_get_ticker_uses_client_directly() -> None:
    """Verify safe_get_ticker returns client ticker when no error.

    Given: Client whose get_ticker succeeds.
    When: safe_get_ticker is called.
    Then: Returns the client's ticker directly.
    """
    expected = TickerSnapshot(
        symbol="BTC-EUR", bid=50000.0, ask=50001.0, last=50000.5, timestamp=1.0
    )
    client = MagicMock()
    client.get_ticker = AsyncMock(return_value=expected)
    result = await safe_get_ticker(client, "BTC-EUR")
    assert result.bid == 50000.0
    assert result.ask == 50001.0


@pytest.mark.asyncio
async def test_safe_get_ticker_fallback_on_type_error() -> None:
    """Verify safe_get_ticker falls back to CCXT on TypeError.

    Given: Client whose get_ticker raises TypeError (e.g., None timestamp).
    When: safe_get_ticker is called.
    Then: Falls back to CCXT fetch_ticker via asyncio.to_thread.
    """
    client = MagicMock()
    client.get_ticker = AsyncMock(side_effect=TypeError("NoneType / float"))
    client._ccxt_client = MagicMock()
    with patch(
        "scripts.test_live_orders.asyncio.to_thread",
        new_callable=AsyncMock,
        return_value={"bid": 100.0, "ask": 101.0, "last": 100.5, "timestamp": 1000},
    ) as run_in_thread:
        result = await safe_get_ticker(client, "BTC-EUR")
    assert result.bid == 100.0
    assert result.ask == 101.0
    assert result.symbol == "BTC-EUR"
    run_in_thread.assert_awaited_once_with(client._ccxt_client.fetch_ticker, "BTC/EUR")


@pytest.mark.asyncio
async def test_safe_get_ticker_fallback_uses_zero_for_missing_timestamp() -> None:
    """Verify safe_get_ticker handles missing CCXT timestamps.

    Given: Client fallback data without a timestamp.
    When: safe_get_ticker falls back to fetch_ticker.
    Then: The returned ticker has timestamp zero.
    """
    client = MagicMock()
    client.get_ticker = AsyncMock(side_effect=TypeError("NoneType / float"))
    client._ccxt_client = MagicMock()
    with patch(
        "scripts.test_live_orders.asyncio.to_thread",
        new_callable=AsyncMock,
        return_value={"bid": 100.0, "ask": 101.0, "last": 100.5},
    ) as run_in_thread:
        result = await safe_get_ticker(client, "BTC-EUR")
    assert result.timestamp == 0.0
    run_in_thread.assert_awaited_once_with(client._ccxt_client.fetch_ticker, "BTC/EUR")


@pytest.mark.asyncio
async def test_safe_get_ticker_reraises_type_error_without_ccxt_client() -> None:
    """Verify safe_get_ticker reraises when fallback is unavailable.

    Given: Client whose get_ticker raises TypeError and has no CCXT client.
    When: safe_get_ticker is called.
    Then: The original TypeError is raised.
    """
    client = MagicMock()
    client.get_ticker = AsyncMock(side_effect=TypeError("NoneType / float"))
    client._ccxt_client = None
    with pytest.raises(TypeError, match="NoneType / float"):
        await safe_get_ticker(client, "BTC-EUR")


@pytest.mark.asyncio
async def test_get_settings_decrypts_live_wallet_credentials() -> None:
    """Verify get_settings reads and decrypts live wallet credentials.

    Given: Repository rows for paper, Kraken, Futures, Walutomat, and unknown exchanges.
    When: get_settings loads credentials.
    Then: Live credentials are mapped onto the expected settings attributes.
    """
    rows: list[dict[str, str]] = [
        {
            "credential_type": "paper",
            "exchange": "kraken",
            "encrypted_payload": "paper",
        },
        {
            "credential_type": "live",
            "exchange": "kraken",
            "encrypted_payload": "kraken",
        },
        {
            "credential_type": "live",
            "exchange": "kraken_futures",
            "encrypted_payload": "futures",
        },
        {
            "credential_type": "live",
            "exchange": "walutomat",
            "encrypted_payload": "walutomat",
        },
        {
            "credential_type": "live",
            "exchange": "unknown",
            "encrypted_payload": "unknown",
        },
    ]
    repository = MagicMock()
    repository.list_active_wallet_credentials = AsyncMock(return_value=rows)
    encryption = MagicMock()
    encryption.decrypt = MagicMock(
        side_effect=[
            json.dumps({"api_key": "kraken-key", "api_secret": "kraken-secret"}),
            json.dumps({"api_key": "futures-key", "api_secret": "futures-secret"}),
            json.dumps({"api_key": "wal-key", "private_key_pem": "wal-private"}),
            json.dumps({"api_key": "unused"}),
        ]
    )
    with (
        patch(
            "scripts.test_live_orders.get_bootstrap_settings",
            return_value=SimpleNamespace(db_url="sqlite:///test.db"),
        ),
        patch("scripts.test_live_orders.get_repository", return_value=repository),
        patch("scripts.test_live_orders.get_encryption_service", return_value=encryption),
    ):
        settings = await get_settings()
    assert settings.kraken_api_key == "kraken-key"
    assert settings.kraken_api_secret == "kraken-secret"
    assert settings.kraken_futures_api_key == "futures-key"
    assert settings.kraken_futures_api_secret == "futures-secret"
    assert settings.walutomat_api_key == "wal-key"
    assert settings.walutomat_private_key == "wal-private"
    repository.list_active_wallet_credentials.assert_awaited_once()
    assert encryption.decrypt.call_count == 4


@pytest.mark.asyncio
async def test_run_walutomat_skips_without_api_key() -> None:
    """Verify run_walutomat does nothing when no API key configured.

    Given: Settings with empty walutomat_api_key.
    When: run_walutomat is called.
    Then: Returns without error (no orders placed).
    """
    settings = _make_settings()
    await run_walutomat(settings)


@pytest.mark.asyncio
async def test_run_kraken_spot_skips_without_api_key() -> None:
    """Verify run_kraken_spot does nothing when no API keys configured.

    Given: Settings with empty kraken_api_key.
    When: run_kraken_spot is called.
    Then: Returns without error.
    """
    settings = _make_settings()
    await run_kraken_spot(settings)


@pytest.mark.asyncio
async def test_run_kraken_futures_skips_without_api_key() -> None:
    """Verify run_kraken_futures does nothing when no API keys configured.

    Given: Settings with empty kraken_futures_api_key.
    When: run_kraken_futures is called.
    Then: Returns without error.
    """
    settings = _make_settings()
    await run_kraken_futures(settings)


@pytest.mark.asyncio
async def test_run_walutomat_passive_buy(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify run_walutomat creates, fetches, and cancels passive buy.

    Given: Mock Walutomat client with ticker and order operations.
    When: run_walutomat is called for passive_buy scenario.
    Then: Three JSON lines are emitted (create, fetch, cancel).
    """
    mock_snap = ExchangeOrderSnapshot(
        id="test-wal-001",
        client_order_id="cli-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=4.0,
        status=ExchangeOrderStatusEnum.PENDING,
        filled=0.0,
        remaining=1.0,
        timestamp=1.0,
    )
    mock_client = AsyncMock()
    mock_client.get_ticker = AsyncMock(
        return_value=TickerSnapshot(symbol="EUR-PLN", bid=4.28, ask=4.29, last=4.285, timestamp=1.0)
    )
    mock_client.create_order = AsyncMock(return_value=mock_snap)
    mock_client.get_order = AsyncMock(return_value=mock_snap)
    mock_client.cancel_order = AsyncMock(return_value=mock_snap)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    settings = _make_settings(walutomat_api_key="key", walutomat_private_key="pk")
    with (
        patch("scripts.test_live_orders.WalutomatExchangeClient", return_value=mock_client),
        patch("scripts.test_live_orders.asyncio.sleep", new_callable=AsyncMock),
    ):
        await run_walutomat(settings, ["passive_buy"])

    output = capsys.readouterr().out.strip()
    lines = [json.loads(line) for line in output.split("\n") if line.strip()]
    assert len(lines) == 3
    assert lines[0]["step"] == "create"
    assert lines[1]["step"] == "fetch"
    assert lines[2]["step"] == "cancel"


@pytest.mark.parametrize(
    (
        "scenario",
        "expected_side",
        "expected_price",
        "expected_steps",
        "expected_get_count",
        "expected_cancel_count",
    ),
    [
        (
            "passive_buy",
            OrderSideEnum.BUY,
            3.99,
            ("create", "fetch", "cancel"),
            1,
            1,
        ),
        (
            "passive_sell",
            OrderSideEnum.SELL,
            4.515,
            ("create", "fetch", "cancel"),
            1,
            1,
        ),
        (
            "aggressive_buy",
            OrderSideEnum.BUY,
            4.343,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "aggressive_sell",
            OrderSideEnum.SELL,
            4.158,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "cancel_inflight",
            OrderSideEnum.BUY,
            3.99,
            ("create", "cancel"),
            0,
            1,
        ),
    ],
)
@pytest.mark.asyncio
async def test_run_walutomat_scenario_places_expected_order(
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    expected_side: OrderSideEnum,
    expected_price: float,
    expected_steps: tuple[str, ...],
    expected_get_count: int,
    expected_cancel_count: int,
) -> None:
    """Verify each Walutomat scenario uses the expected client calls.

    Given: A mocked Walutomat client and one selected scenario.
    When: run_walutomat executes the scenario.
    Then: The request, subsequent calls, and emitted fixtures match the scenario.

    The correlation id is asserted by SHAPE, not by literal value.
    These scenarios used to label the id with the scenario name and the
    wall-clock second, and the tests pinned the resulting string against
    a frozen clock — which is precisely what made the collision
    invisible: two concurrent runs of the same scenario within one
    second mint the same id, and a venue duplicate-id refusal is a plain
    ExchangeError that the submit path does not wrap as ambiguous, so it
    becomes a false REJECTED plus a sweep-exempting order_rejected row
    for a possibly-live order. A uuid7 has no such collision mode, and
    there is no literal left to pin. Uniqueness across a whole run is
    pinned separately by the all-scenarios test.
    """
    symbol = "EUR-PLN"
    amount = 1.0
    created = _make_order_snapshot(
        f"{scenario}-created", symbol, expected_side, amount, expected_price
    )
    fetched_status = (
        ExchangeOrderStatusEnum.CLOSED
        if scenario.startswith("aggressive")
        else ExchangeOrderStatusEnum.OPEN
    )
    get_snaps = [
        _make_order_snapshot(
            f"{scenario}-fetched",
            symbol,
            expected_side,
            amount,
            expected_price,
            fetched_status,
        )
    ] * expected_get_count
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            expected_side,
            amount,
            expected_price,
            ExchangeOrderStatusEnum.CANCELED,
        )
    ] * expected_cancel_count
    client = _make_exchange_client(
        _make_ticker(symbol, 4.2, 4.3),
        [created],
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(walutomat_api_key="key", walutomat_private_key="private")

    constructor = await _run_with_client(
        run_walutomat,
        settings,
        "scripts.test_live_orders.WalutomatExchangeClient",
        client,
        [scenario],
    )

    constructor.assert_called_once_with(api_key="key", private_key_data="private")
    client.get_ticker.assert_awaited_once_with(symbol)
    request = _created_requests(client)[0]
    assert request.symbol == symbol
    assert request.side == expected_side
    assert request.type == ExchangeOrderTypeEnum.LIMIT
    assert request.amount == amount
    assert request.price == expected_price
    assert is_uuid7(request.client_order_id)
    assert client.get_order.await_count == expected_get_count
    assert client.cancel_order.await_count == expected_cancel_count
    if expected_get_count:
        client.get_order.assert_awaited_once_with(created.id)
    if expected_cancel_count:
        client.cancel_order.assert_awaited_once_with(created.id)

    lines = _json_lines(capsys)
    assert [line["step"] for line in lines] == list(expected_steps)
    assert all(line["exchange"] == "walutomat" for line in lines)
    assert all(line["scenario"] == scenario for line in lines)
    create_data = _line_data(lines[0])
    assert create_data["id"] == created.id
    assert create_data["side"] == expected_side.value
    assert create_data["type"] == ExchangeOrderTypeEnum.LIMIT.value


@pytest.mark.asyncio
async def test_run_walutomat_defaults_to_all_scenarios(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify Walutomat runs every scenario when no filter is provided.

    Given: A mocked Walutomat client with enough snapshots for all scenarios.
    When: run_walutomat is called without a scenario filter.
    Then: All creates, fetches, cancels, and unfilled aggressive cancels are emitted.
    """
    symbol = "EUR-PLN"
    scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    sides = {
        "passive_buy": OrderSideEnum.BUY,
        "passive_sell": OrderSideEnum.SELL,
        "aggressive_buy": OrderSideEnum.BUY,
        "aggressive_sell": OrderSideEnum.SELL,
        "cancel_inflight": OrderSideEnum.BUY,
    }
    prices = {
        "passive_buy": 3.99,
        "passive_sell": 4.515,
        "aggressive_buy": 4.343,
        "aggressive_sell": 4.158,
        "cancel_inflight": 3.99,
    }
    create_snaps = [
        _make_order_snapshot(f"{scenario}-created", symbol, sides[scenario], 1.0, prices[scenario])
        for scenario in scenarios
    ]
    get_snaps = [
        _make_order_snapshot(f"{scenario}-fetched", symbol, sides[scenario], 1.0, prices[scenario])
        for scenario in scenarios
        if scenario != "cancel_inflight"
    ]
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            sides[scenario],
            1.0,
            prices[scenario],
            ExchangeOrderStatusEnum.CANCELED,
        )
        for scenario in scenarios
    ]
    client = _make_exchange_client(
        _make_ticker(symbol, 4.2, 4.3),
        create_snaps,
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(walutomat_api_key="key", walutomat_private_key="private")

    await _run_with_client(
        run_walutomat,
        settings,
        "scripts.test_live_orders.WalutomatExchangeClient",
        client,
        None,
    )

    assert client.create_order.await_count == 5
    assert client.get_order.await_count == 4
    assert client.cancel_order.await_count == 5
    _assert_ids_unique_and_well_formed(client, 5)
    lines = _json_lines(capsys)
    assert len(lines) == 14
    assert [line["scenario"] for line in lines if line["step"] == "create"] == scenarios
    assert ("aggressive_buy", "cancel_unfilled") in [
        (line["scenario"], line["step"]) for line in lines
    ]
    assert ("aggressive_sell", "cancel_unfilled") in [
        (line["scenario"], line["step"]) for line in lines
    ]


@pytest.mark.parametrize(
    (
        "scenario",
        "expected_side",
        "expected_price",
        "expected_post_only",
        "expected_steps",
        "expected_get_count",
        "expected_cancel_count",
    ),
    [
        (
            "passive_buy",
            OrderSideEnum.BUY,
            45000.0,
            True,
            ("create", "fetch", "cancel"),
            1,
            1,
        ),
        (
            "passive_sell",
            OrderSideEnum.SELL,
            55110.0,
            True,
            ("create", "fetch", "cancel"),
            1,
            1,
        ),
        (
            "aggressive_buy",
            OrderSideEnum.BUY,
            50350.5,
            False,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "aggressive_sell",
            OrderSideEnum.SELL,
            49750.0,
            False,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "cancel_inflight",
            OrderSideEnum.BUY,
            45000.0,
            False,
            ("create", "cancel"),
            0,
            1,
        ),
    ],
)
@pytest.mark.asyncio
async def test_run_kraken_spot_scenario_places_expected_order(
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    expected_side: OrderSideEnum,
    expected_price: float,
    expected_post_only: bool,
    expected_steps: tuple[str, ...],
    expected_get_count: int,
    expected_cancel_count: int,
) -> None:
    """Verify each Kraken Spot scenario uses the expected client calls.

    Given: A mocked Kraken Spot client and one selected scenario.
    When: run_kraken_spot executes the scenario.
    Then: The request, subsequent calls, and emitted fixtures match the scenario.

    The client_order_id assertion matters twice over: these five
    scenarios used to submit with NO correlation id at all against a
    real Kraken account, the one place either venue-boundary defect
    reached a live venue. It is asserted by SHAPE — see the Walutomat
    sibling for why a literal, scenario-and-clock id was itself a
    collision hazard in a real-money script.
    """
    symbol = "BTC-EUR"
    amount = 0.0001
    created = _make_order_snapshot(
        f"{scenario}-created", symbol, expected_side, amount, expected_price
    )
    get_snaps = [
        _make_order_snapshot(f"{scenario}-fetched", symbol, expected_side, amount, expected_price)
    ] * expected_get_count
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            expected_side,
            amount,
            expected_price,
            ExchangeOrderStatusEnum.CANCELED,
        )
    ] * expected_cancel_count
    client = _make_exchange_client(
        _make_ticker(symbol, 50000.0, 50100.0),
        [created],
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(kraken_api_key="key", kraken_api_secret="secret")

    constructor = await _run_with_client(
        run_kraken_spot,
        settings,
        "scripts.test_live_orders.KrakenExchangeClient",
        client,
        [scenario],
    )

    constructor.assert_called_once_with(api_key="key", api_secret="secret")
    client.get_ticker.assert_awaited_once_with(symbol)
    request = _created_requests(client)[0]
    assert request.symbol == symbol
    assert request.side == expected_side
    assert request.type == ExchangeOrderTypeEnum.LIMIT
    assert request.amount == amount
    assert request.price == expected_price
    assert request.post_only is expected_post_only
    assert is_uuid7(request.client_order_id)
    assert client.get_order.await_count == expected_get_count
    assert client.cancel_order.await_count == expected_cancel_count
    assert [call.args for call in client.get_order.await_args_list] == [
        (created.id, symbol)
    ] * expected_get_count
    assert [call.args for call in client.cancel_order.await_args_list] == [
        (created.id, symbol)
    ] * expected_cancel_count

    lines = _json_lines(capsys)
    assert [line["step"] for line in lines] == list(expected_steps)
    assert all(line["exchange"] == "kraken" for line in lines)
    assert all(line["scenario"] == scenario for line in lines)
    create_data = _line_data(lines[0])
    assert create_data["id"] == created.id
    assert create_data["side"] == expected_side.value
    assert create_data["type"] == ExchangeOrderTypeEnum.LIMIT.value


@pytest.mark.asyncio
async def test_run_kraken_spot_defaults_to_all_scenarios(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify Kraken Spot runs every scenario when no filter is provided.

    Given: A mocked Kraken Spot client with enough snapshots for all scenarios.
    When: run_kraken_spot is called without a scenario filter.
    Then: All expected creates, fetches, and cancels are emitted.
    """
    symbol = "BTC-EUR"
    scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    sides = {
        "passive_buy": OrderSideEnum.BUY,
        "passive_sell": OrderSideEnum.SELL,
        "aggressive_buy": OrderSideEnum.BUY,
        "aggressive_sell": OrderSideEnum.SELL,
        "cancel_inflight": OrderSideEnum.BUY,
    }
    prices = {
        "passive_buy": 45000.0,
        "passive_sell": 55110.0,
        "aggressive_buy": 50350.5,
        "aggressive_sell": 49750.0,
        "cancel_inflight": 45000.0,
    }
    create_snaps = [
        _make_order_snapshot(
            f"{scenario}-created", symbol, sides[scenario], 0.0001, prices[scenario]
        )
        for scenario in scenarios
    ]
    get_snaps = [
        _make_order_snapshot(
            f"{scenario}-fetched", symbol, sides[scenario], 0.0001, prices[scenario]
        )
        for scenario in scenarios
        if scenario != "cancel_inflight"
    ]
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            sides[scenario],
            0.0001,
            prices[scenario],
            ExchangeOrderStatusEnum.CANCELED,
        )
        for scenario in ("passive_buy", "passive_sell", "cancel_inflight")
    ]
    client = _make_exchange_client(
        _make_ticker(symbol, 50000.0, 50100.0),
        create_snaps,
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(kraken_api_key="key", kraken_api_secret="secret")

    await _run_with_client(
        run_kraken_spot,
        settings,
        "scripts.test_live_orders.KrakenExchangeClient",
        client,
        None,
    )

    assert client.create_order.await_count == 5
    assert client.get_order.await_count == 4
    assert client.cancel_order.await_count == 3
    _assert_ids_unique_and_well_formed(client, 5)
    lines = _json_lines(capsys)
    assert len(lines) == 12
    assert [line["scenario"] for line in lines if line["step"] == "create"] == scenarios


@pytest.mark.parametrize(
    (
        "scenario",
        "expected_side",
        "expected_price",
        "expected_post_only",
        "expected_steps",
        "expected_get_count",
        "expected_cancel_count",
    ),
    [
        (
            "passive_buy",
            OrderSideEnum.BUY,
            45000.0,
            True,
            ("create", "fetch", "cancel", "fetch_after_cancel"),
            2,
            1,
        ),
        (
            "passive_sell",
            OrderSideEnum.SELL,
            55110.0,
            True,
            ("create", "fetch", "cancel"),
            1,
            1,
        ),
        (
            "aggressive_buy",
            OrderSideEnum.BUY,
            50350.5,
            False,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "aggressive_sell",
            OrderSideEnum.SELL,
            49750.0,
            False,
            ("create", "fetch"),
            1,
            0,
        ),
        (
            "cancel_inflight",
            OrderSideEnum.BUY,
            45000.0,
            False,
            ("create", "cancel"),
            0,
            1,
        ),
    ],
)
@pytest.mark.asyncio
async def test_run_kraken_futures_scenario_places_expected_order(
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    expected_side: OrderSideEnum,
    expected_price: float,
    expected_post_only: bool,
    expected_steps: tuple[str, ...],
    expected_get_count: int,
    expected_cancel_count: int,
) -> None:
    """Verify each Kraken Futures scenario uses the expected client calls.

    Given: A mocked Kraken Futures client and one selected scenario.
    When: run_kraken_futures executes the scenario.
    Then: The request, subsequent calls, and emitted fixtures match the scenario.

    The correlation id is asserted by SHAPE — see the Walutomat sibling
    for why the previous scenario-and-clock literal was itself a
    collision hazard in a real-money script.
    """
    symbol = "BTC-USD-PERP"
    amount = 0.0001
    created = _make_order_snapshot(
        f"{scenario}-created", symbol, expected_side, amount, expected_price
    )
    get_snaps = [
        _make_order_snapshot(
            f"{scenario}-fetched-{index}",
            symbol,
            expected_side,
            amount,
            expected_price,
        )
        for index in range(expected_get_count)
    ]
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            expected_side,
            amount,
            expected_price,
            ExchangeOrderStatusEnum.CANCELED,
        )
    ] * expected_cancel_count
    client = _make_exchange_client(
        _make_ticker(symbol, 50000.0, 50100.0),
        [created],
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(
        kraken_futures_api_key="key",
        kraken_futures_api_secret="secret",
    )

    with patch(
        "scripts.test_live_orders.native_to_ccxt", return_value="PF_XBTUSD"
    ) as convert_symbol:
        constructor = await _run_with_client(
            run_kraken_futures,
            settings,
            "scripts.test_live_orders.KrakenFuturesExchangeClient",
            client,
            [scenario],
        )

    constructor.assert_called_once_with(api_key="key", api_secret="secret")
    convert_symbol.assert_called_once_with(symbol)
    client.get_ticker.assert_awaited_once_with("PF_XBTUSD")
    request = _created_requests(client)[0]
    assert request.symbol == symbol
    assert request.side == expected_side
    assert request.type == ExchangeOrderTypeEnum.LIMIT
    assert request.amount == amount
    assert request.price == expected_price
    assert is_uuid7(request.client_order_id)
    assert request.post_only is expected_post_only
    assert client.get_order.await_count == expected_get_count
    assert client.cancel_order.await_count == expected_cancel_count
    assert [call.args for call in client.get_order.await_args_list] == [
        (created.id, symbol)
    ] * expected_get_count
    assert [call.args for call in client.cancel_order.await_args_list] == [
        (created.id, symbol)
    ] * expected_cancel_count

    lines = _json_lines(capsys)
    assert [line["step"] for line in lines] == list(expected_steps)
    assert all(line["exchange"] == "kraken_futures" for line in lines)
    assert all(line["scenario"] == scenario for line in lines)
    create_data = _line_data(lines[0])
    assert create_data["id"] == created.id
    assert create_data["side"] == expected_side.value
    assert create_data["type"] == ExchangeOrderTypeEnum.LIMIT.value


@pytest.mark.asyncio
async def test_run_kraken_futures_defaults_to_all_scenarios(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify Kraken Futures runs every scenario when no filter is provided.

    Given: A mocked Kraken Futures client with enough snapshots for all scenarios.
    When: run_kraken_futures is called without a scenario filter.
    Then: All expected creates, fetches, cancels, and post-cancel fetch are emitted.
    """
    symbol = "BTC-USD-PERP"
    scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    sides = {
        "passive_buy": OrderSideEnum.BUY,
        "passive_sell": OrderSideEnum.SELL,
        "aggressive_buy": OrderSideEnum.BUY,
        "aggressive_sell": OrderSideEnum.SELL,
        "cancel_inflight": OrderSideEnum.BUY,
    }
    prices = {
        "passive_buy": 45000.0,
        "passive_sell": 55110.0,
        "aggressive_buy": 50350.5,
        "aggressive_sell": 49750.0,
        "cancel_inflight": 45000.0,
    }
    create_snaps = [
        _make_order_snapshot(
            f"{scenario}-created", symbol, sides[scenario], 0.0001, prices[scenario]
        )
        for scenario in scenarios
    ]
    get_snaps = [
        _make_order_snapshot("passive_buy-fetched", symbol, OrderSideEnum.BUY, 0.0001, 45000.0),
        _make_order_snapshot(
            "passive_buy-fetched-after-cancel",
            symbol,
            OrderSideEnum.BUY,
            0.0001,
            45000.0,
            ExchangeOrderStatusEnum.CANCELED,
        ),
        _make_order_snapshot("passive_sell-fetched", symbol, OrderSideEnum.SELL, 0.0001, 55110.0),
        _make_order_snapshot("aggressive_buy-fetched", symbol, OrderSideEnum.BUY, 0.0001, 50350.5),
        _make_order_snapshot(
            "aggressive_sell-fetched",
            symbol,
            OrderSideEnum.SELL,
            0.0001,
            49750.0,
        ),
    ]
    cancel_snaps = [
        _make_order_snapshot(
            f"{scenario}-canceled",
            symbol,
            sides[scenario],
            0.0001,
            prices[scenario],
            ExchangeOrderStatusEnum.CANCELED,
        )
        for scenario in ("passive_buy", "passive_sell", "cancel_inflight")
    ]
    client = _make_exchange_client(
        _make_ticker(symbol, 50000.0, 50100.0),
        create_snaps,
        get_snaps,
        cancel_snaps,
    )
    settings = _make_settings(
        kraken_futures_api_key="key",
        kraken_futures_api_secret="secret",
    )

    with patch("scripts.test_live_orders.native_to_ccxt", return_value="PF_XBTUSD"):
        await _run_with_client(
            run_kraken_futures,
            settings,
            "scripts.test_live_orders.KrakenFuturesExchangeClient",
            client,
            None,
        )

    assert client.create_order.await_count == 5
    assert client.get_order.await_count == 5
    _assert_ids_unique_and_well_formed(client, 5)
    assert client.cancel_order.await_count == 3
    lines = _json_lines(capsys)
    assert len(lines) == 13
    assert [line["scenario"] for line in lines if line["step"] == "create"] == scenarios
    assert ("passive_buy", "fetch_after_cancel") in [
        (line["scenario"], line["step"]) for line in lines
    ]


def test_main_runs_without_keys() -> None:
    """Verify main exits cleanly when no API keys configured.

    Given: Mock settings with no exchange credentials.
    When: main is called.
    Then: Returns 0 without errors.
    """
    mock_settings = _make_settings()
    with (
        patch(
            "scripts.test_live_orders.get_settings",
            new_callable=AsyncMock,
            return_value=mock_settings,
        ),
        patch("sys.argv", ["test_live_orders.py"]),
    ):
        result = main()
    assert result == 0


def test_main_unknown_exchange() -> None:
    """Verify main returns 1 for unknown exchange name.

    Given: argv with unknown exchange name.
    When: main is called.
    Then: Returns 1.
    """
    mock_settings = _make_settings()
    with (
        patch(
            "scripts.test_live_orders.get_settings",
            new_callable=AsyncMock,
            return_value=mock_settings,
        ),
        patch("sys.argv", ["test_live_orders.py", "nonexistent"]),
    ):
        result = main()
    assert result == 1


def test_main_emits_runner_exceptions(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify main captures runner exceptions as JSON fixtures.

    Given: A selected runner that raises an exception.
    When: main executes the filtered exchange and scenario.
    Then: It emits an ERROR fixture and still returns zero.
    """

    async def failing_runner(
        settings: SimpleNamespace,
        scenarios: list[str] | None = None,
    ) -> None:
        """Raise an exception after verifying forwarded arguments."""
        assert settings.kraken_api_key == "key"
        assert scenarios == ["passive_buy"]
        raise RuntimeError("boom")

    mock_settings = _make_settings(kraken_api_key="key")
    with (
        patch(
            "scripts.test_live_orders.get_settings",
            new_callable=AsyncMock,
            return_value=mock_settings,
        ),
        patch("scripts.test_live_orders.EXCHANGE_RUNNERS", {"kraken": failing_runner}),
        patch("sys.argv", ["test_live_orders.py", "kraken", "passive_buy"]),
    ):
        result = main()
    assert result == 0
    lines = _json_lines(capsys)
    assert len(lines) == 1
    assert lines[0]["exchange"] == "kraken"
    assert lines[0]["scenario"] == "ERROR"
    assert lines[0]["step"] == "exception"
    assert lines[0]["data"] == "boom"


def _permissive_client(bid: float, ask: float) -> MagicMock:
    """Build an exchange client double that answers any call count.

    Unlike ``_make_exchange_client`` this uses ``return_value`` rather
    than a fixed ``side_effect`` list, so one double serves any scenario
    on any venue without the caller having to know how many fetches or
    cancels that scenario performs.

    Args:
        bid: Ticker bid to report.
        ask: Ticker ask to report.

    Returns:
        MagicMock implementing the async client context manager protocol.
    """
    snapshot = _make_order_snapshot("oid-1", "SYM", OrderSideEnum.BUY, 1.0, bid)
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_ticker = AsyncMock(return_value=_make_ticker("SYM", bid, ask))
    client.create_order = AsyncMock(return_value=snapshot)
    client.get_order = AsyncMock(return_value=snapshot)
    client.cancel_order = AsyncMock(return_value=snapshot)
    return client


@pytest.mark.parametrize(
    ("runner", "patch_target", "credentials", "bid", "ask"),
    [
        (
            run_walutomat,
            "scripts.test_live_orders.WalutomatExchangeClient",
            {"walutomat_api_key": "key", "walutomat_private_key": "private"},
            4.2,
            4.3,
        ),
        (
            run_kraken_spot,
            "scripts.test_live_orders.KrakenExchangeClient",
            {"kraken_api_key": "key", "kraken_api_secret": "secret"},
            50000.0,
            50100.0,
        ),
        (
            run_kraken_futures,
            "scripts.test_live_orders.KrakenFuturesExchangeClient",
            {"kraken_futures_api_key": "key", "kraken_futures_api_secret": "secret"},
            50000.0,
            50100.0,
        ),
    ],
)
@pytest.mark.asyncio
async def test_repeating_one_scenario_never_reuses_a_correlation_id(
    runner: LiveRunner,
    patch_target: str,
    credentials: dict[str, str],
    bid: float,
    ask: float,
) -> None:
    """Repeating one scenario on one venue mints a fresh id each time.

    Given: One venue runner and one scenario,
    When: The runner is invoked twice in immediate succession,
    Then: Both submits carry uuid7-shaped correlation ids and the two
        ids differ.

    Regression test for the id scheme this replaced. Every scenario
    used to mint ``f"test-{scenario}-{int(time.time())}"``, so this
    exact sequence produced the SAME id twice whenever both runs landed
    in one wall-clock second — and a venue duplicate-id refusal is a
    plain ExchangeError, not a NetworkError, so the submit path does not
    wrap it as ambiguous. It lands in the executor's generic handler as
    a REJECTED plus a durable order_rejected row that permanently
    exempts the command from the unresolved-dispatched sweep, for an
    order that may be live and resting. In a script that places real
    money orders that is exactly the false-terminal hazard the
    surrounding change exists to remove.

    The two assertions guard different regressions and both are needed.
    The SHAPE assertion is what fails against the old scheme: no
    ``test-...-<epoch>`` label is a uuid7, so a revert to any
    clock-derived or scenario-derived label cannot pass. The
    DISTINCTNESS assertion catches the other realistic way to
    reintroduce the collision — hoisting the mint to a module constant,
    a default argument, or a single local reused across submits — which
    would keep the shape valid while restoring the duplicate.
    """
    settings = _make_settings(**credentials)
    ids: list[str] = []
    for _ in range(2):
        client = _permissive_client(bid, ask)
        with patch("scripts.test_live_orders.native_to_ccxt", return_value="PF_XBTUSD"):
            await _run_with_client(runner, settings, patch_target, client, ["aggressive_buy"])
        ids.extend(request.client_order_id for request in _created_requests(client))
    assert len(ids) == 2
    assert all(is_uuid7(value) for value in ids)
    assert len(set(ids)) == 2
