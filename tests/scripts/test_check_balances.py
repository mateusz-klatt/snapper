"""Tests for check_balances script."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.check_balances import _run
from scripts.check_balances import build_exchange_factories
from scripts.check_balances import check_single_exchange
from scripts.check_balances import format_balances
from scripts.check_balances import main
from snapper.infrastructure.exchanges.contracts import AccountBalance


def _make_settings(**overrides: str) -> SimpleNamespace:
    """Create mock settings with optional API keys."""
    defaults = {
        "kraken_api_key": "",
        "kraken_api_secret": "",
        "kraken_futures_api_key": "",
        "kraken_futures_api_secret": "",
        "zonda_api_key": "",
        "zonda_api_secret": "",
        "walutomat_api_key": "",
        "walutomat_private_key": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_build_factories_no_keys() -> None:
    """Verify no factories when no API keys configured.

    Given: Settings with empty credentials,
    When: build_exchange_factories is called,
    Then: Empty list returned.
    """
    settings = _make_settings()
    factories = build_exchange_factories(settings)
    assert len(factories) == 0


def test_build_factories_kraken_only() -> None:
    """Verify only Kraken factory when only Kraken keys set.

    Given: Settings with Kraken keys only,
    When: build_exchange_factories is called,
    Then: One factory named 'Kraken'.
    """
    settings = _make_settings(kraken_api_key="k", kraken_api_secret="s")
    factories = build_exchange_factories(settings)
    assert len(factories) == 1
    assert factories[0][0] == "Kraken"


def test_build_factories_walutomat_without_private_key() -> None:
    """Verify Walutomat factory created with only api_key.

    Given: Settings with walutomat_api_key but no private_key,
    When: build_exchange_factories is called,
    Then: Walutomat factory is included.
    """
    settings = _make_settings(walutomat_api_key="w")
    factories = build_exchange_factories(settings)
    assert len(factories) == 1
    assert factories[0][0] == "Walutomat"


def test_build_factories_all_exchanges() -> None:
    """Verify all four factories when all keys set.

    Given: Settings with all exchange keys,
    When: build_exchange_factories is called,
    Then: Four factories returned.
    """
    settings = _make_settings(
        kraken_api_key="k",
        kraken_api_secret="s",
        kraken_futures_api_key="kf",
        kraken_futures_api_secret="kfs",
        zonda_api_key="z",
        zonda_api_secret="zs",
        walutomat_api_key="w",
    )
    factories = build_exchange_factories(settings)
    assert len(factories) == 4
    names = [f[0] for f in factories]
    assert "Kraken" in names
    assert "Kraken Futures" in names
    assert "Zonda" in names
    assert "Walutomat" in names


@pytest.mark.asyncio
async def test_check_single_exchange_success() -> None:
    """Verify successful balance fetch returns non-zero holdings.

    Given: Exchange client returning BTC and zero-balance USD,
    When: check_single_exchange is called,
    Then: Only BTC (non-zero) returned.
    """
    mock_client = AsyncMock()
    mock_client.get_balance = AsyncMock(
        return_value={
            "BTC": AccountBalance(currency="BTC", free=0.1, used=0.0, total=0.1),
            "USD": AccountBalance(currency="USD", free=0.0, used=0.0, total=0.0),
        }
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    result = await check_single_exchange("Test", lambda: mock_client)
    assert "BTC" in result
    assert "USD" not in result


@pytest.mark.asyncio
async def test_check_single_exchange_error() -> None:
    """Verify exchange error returns empty dict.

    Given: Exchange client that raises on connect,
    When: check_single_exchange is called,
    Then: Empty dict returned, no exception.
    """

    def bad_factory() -> MagicMock:
        raise RuntimeError("connection failed")

    result = await check_single_exchange("Bad", bad_factory)
    assert result == {}


def test_format_balances_with_data() -> None:
    """Verify format output for exchange with holdings.

    Given: Exchange with BTC balance,
    When: format_balances is called,
    Then: Output includes exchange name and formatted balance line.
    """
    balances = {
        "Kraken": {
            "BTC": AccountBalance(currency="BTC", free=0.1, used=0.0, total=0.1),
        }
    }
    lines = format_balances(balances)
    assert lines[0] == "Kraken:"
    assert "BTC" in lines[1]
    assert "0.10000000" in lines[1]


def test_format_balances_empty() -> None:
    """Verify format output for exchange with no holdings.

    Given: Exchange with no non-zero balances,
    When: format_balances is called,
    Then: Shows 'no non-zero balances' message.
    """
    balances: dict[str, dict[str, AccountBalance]] = {"Empty": {}}
    lines = format_balances(balances)
    assert lines[0] == "Empty: (no non-zero balances)"


def test_format_balances_used_shown() -> None:
    """Verify used amount shown when non-zero.

    Given: Balance with used > 0,
    When: format_balances is called,
    Then: Output includes 'used=' field.
    """
    balances = {
        "Test": {
            "ETH": AccountBalance(currency="ETH", free=0.5, used=0.2, total=0.7),
        }
    }
    lines = format_balances(balances)
    assert "used=" in lines[1]


@patch("scripts.check_balances._run", new_callable=AsyncMock, return_value=0)
def test_main_returns_exit_code(mock_run: AsyncMock) -> None:
    """Verify main() returns exit code from _run().

    Given: Mocked _run returning 0,
    When: main() is called,
    Then: Returns 0.
    """
    result = main()
    assert result == 0


@pytest.mark.asyncio
@patch("scripts.check_balances.get_bootstrap_settings")
@patch("scripts.check_balances.get_settings_service", new_callable=AsyncMock)
@patch("scripts.check_balances.get_settings_with_service")
async def test_run_no_factories_returns_one(
    mock_get_settings: MagicMock,
    mock_svc: AsyncMock,
    mock_bootstrap: MagicMock,
) -> None:
    """Verify _run returns 1 when no exchange keys configured.

    Given: Settings with no API keys,
    When: _run() is called,
    Then: Returns 1.
    """
    mock_bootstrap.return_value = SimpleNamespace(db_url="sqlite://", zmq_broker_xpub="tcp://x")
    mock_get_settings.return_value = _make_settings()
    result = await _run()
    assert result == 1


@pytest.mark.asyncio
@patch("scripts.check_balances.get_bootstrap_settings")
@patch("scripts.check_balances.get_settings_service", new_callable=AsyncMock)
@patch("scripts.check_balances.get_settings_with_service")
@patch("scripts.check_balances.check_single_exchange", new_callable=AsyncMock)
async def test_run_with_factories_returns_zero(
    mock_check: AsyncMock,
    mock_get_settings: MagicMock,
    mock_svc: AsyncMock,
    mock_bootstrap: MagicMock,
) -> None:
    """Verify _run returns 0 when exchanges are configured and checked.

    Given: Settings with Kraken keys,
    When: _run() is called,
    Then: Returns 0 after checking balances.
    """
    mock_bootstrap.return_value = SimpleNamespace(db_url="sqlite://", zmq_broker_xpub="tcp://x")
    mock_get_settings.return_value = _make_settings(kraken_api_key="k", kraken_api_secret="s")
    mock_check.return_value = {"BTC": AccountBalance(currency="BTC", free=0.1, used=0.0, total=0.1)}
    result = await _run()
    assert result == 0
    mock_check.assert_awaited_once()
