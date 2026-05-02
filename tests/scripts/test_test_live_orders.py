"""Tests for test_live_orders script."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.test_live_orders import EXCHANGE_RUNNERS
from scripts.test_live_orders import emit
from scripts.test_live_orders import main
from scripts.test_live_orders import run_kraken_futures
from scripts.test_live_orders import run_kraken_spot
from scripts.test_live_orders import run_walutomat
from scripts.test_live_orders import safe_get_ticker
from scripts.test_live_orders import snapshot_to_dict
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot


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
    client._ccxt_client.fetch_ticker = MagicMock(
        return_value={"bid": 100.0, "ask": 101.0, "last": 100.5, "timestamp": 1000}
    )
    with patch(
        "scripts.test_live_orders.native_to_ccxt",
        return_value="BTC/EUR",
    ):
        result = await safe_get_ticker(client, "BTC-EUR")
    assert result.bid == 100.0
    assert result.ask == 101.0
    assert result.symbol == "BTC-EUR"


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
    with patch("scripts.test_live_orders.WalutomatExchangeClient", return_value=mock_client):
        await run_walutomat(settings, ["passive_buy"])

    output = capsys.readouterr().out.strip()
    lines = [json.loads(line) for line in output.split("\n") if line.strip()]
    assert len(lines) == 3
    assert lines[0]["step"] == "create"
    assert lines[1]["step"] == "fetch"
    assert lines[2]["step"] == "cancel"


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
