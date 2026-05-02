"""Tests for check_balances script."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.check_balances import _load_live_credentials
from scripts.check_balances import _run
from scripts.check_balances import build_exchange_factories
from scripts.check_balances import check_single_exchange
from scripts.check_balances import format_balances
from scripts.check_balances import main
from snapper.infrastructure.exchanges.contracts import AccountBalance


def _make_credentials(
    **per_exchange: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Build a credentials mapping for tests.

    Accepts kwargs like ``kraken={"api_key": "k", "api_secret": "s"}``
    and returns the dict shape ``build_exchange_factories`` expects.
    """
    return dict(per_exchange)


def test_build_factories_no_credentials() -> None:
    """Verify no factories when no credential envelopes provided.

    Given: An empty credentials mapping,
    When: build_exchange_factories is called,
    Then: Empty list returned.
    """
    factories = build_exchange_factories({})
    assert len(factories) == 0


def test_build_factories_kraken_only() -> None:
    """Verify only Kraken factory when only Kraken envelope provided.

    Given: Credentials mapping with a Kraken envelope,
    When: build_exchange_factories is called,
    Then: One factory named 'Kraken'.
    """
    credentials = _make_credentials(kraken={"api_key": "k", "api_secret": "s"})
    factories = build_exchange_factories(credentials)
    assert len(factories) == 1
    assert factories[0][0] == "Kraken"


def test_build_factories_walutomat_without_private_key() -> None:
    """Verify Walutomat factory created with only api_key.

    Given: Walutomat credential envelope without ``private_key_pem``,
    When: build_exchange_factories is called,
    Then: Walutomat factory is included and the private key passed to
        the client is ``None``.
    """
    credentials = _make_credentials(walutomat={"api_key": "w"})
    factories = build_exchange_factories(credentials)
    assert len(factories) == 1
    assert factories[0][0] == "Walutomat"


def test_build_factories_all_exchanges() -> None:
    """Verify all factories when all envelopes provided.

    Given: Credentials for Kraken, Kraken Futures, and Walutomat,
    When: build_exchange_factories is called,
    Then: Three factories returned in deterministic order.
    """
    credentials = _make_credentials(
        kraken={"api_key": "k", "api_secret": "s"},
        kraken_futures={"api_key": "kf", "api_secret": "kfs"},
        walutomat={"api_key": "w", "private_key_pem": "-----BEGIN PEM-----"},
    )
    factories = build_exchange_factories(credentials)
    assert len(factories) == 3
    names = [f[0] for f in factories]
    assert "Kraken" in names
    assert "Kraken Futures" in names
    assert "Walutomat" in names


def test_build_factories_incomplete_kraken_envelope() -> None:
    """Verify Kraken factory is skipped when the envelope is missing ``api_secret``.

    Given: Kraken envelope with only ``api_key`` (no ``api_secret``),
    When: build_exchange_factories is called,
    Then: No factory is returned — the envelope is incomplete.
    """
    credentials = _make_credentials(kraken={"api_key": "k"})
    factories = build_exchange_factories(credentials)
    assert factories == []


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
@patch("scripts.check_balances.get_repository")
@patch("scripts.check_balances.get_encryption_service")
async def test_load_live_credentials_skips_paper_rows_and_decrypts(
    mock_encryption: MagicMock,
    mock_get_repository: MagicMock,
    mock_bootstrap: MagicMock,
) -> None:
    """Verify ``_load_live_credentials`` returns only decrypted live envelopes.

    Given: ``wallet_credentials`` containing one paper row and one
        live Kraken row,
    When: ``_load_live_credentials`` is called,
    Then: The paper row is skipped and the Kraken row's encrypted
        payload is decrypted via the master-password Fernet service.
    """
    mock_bootstrap.return_value = SimpleNamespace(db_url="sqlite://")
    mock_repo = MagicMock()
    mock_repo.list_active_wallet_credentials = AsyncMock(
        return_value=[
            {
                "public_id": "cred-paper",
                "wallet_public_id": "w-paper",
                "exchange": "paper",
                "credential_type": "paper",
                "encrypted_payload": "enc-paper",
                "label": None,
                "timestamp": None,
                "session_id": "s",
                "sequence_id": 1,
            },
            {
                "public_id": "cred-kraken",
                "wallet_public_id": "w-live",
                "exchange": "kraken",
                "credential_type": "api_key_secret",
                "encrypted_payload": "enc-kraken",
                "label": None,
                "timestamp": None,
                "session_id": "s",
                "sequence_id": 2,
            },
        ]
    )
    mock_get_repository.return_value = mock_repo
    mock_encryption.return_value.decrypt.return_value = (
        '{"api_key": "decrypted-k", "api_secret": "decrypted-s"}'
    )
    credentials = await _load_live_credentials()
    assert "paper" not in credentials
    assert credentials["kraken"] == {
        "api_key": "decrypted-k",
        "api_secret": "decrypted-s",
    }


@pytest.mark.asyncio
@patch("scripts.check_balances._load_live_credentials", new_callable=AsyncMock)
async def test_run_no_factories_returns_one(
    mock_load_credentials: AsyncMock,
) -> None:
    """Verify _run returns 1 when no live credentials are seeded.

    Given: Empty credentials mapping,
    When: _run() is called,
    Then: Returns 1.
    """
    mock_load_credentials.return_value = {}
    result = await _run()
    assert result == 1


@pytest.mark.asyncio
@patch("scripts.check_balances._load_live_credentials", new_callable=AsyncMock)
@patch("scripts.check_balances.check_single_exchange", new_callable=AsyncMock)
async def test_run_with_factories_returns_zero(
    mock_check: AsyncMock,
    mock_load_credentials: AsyncMock,
) -> None:
    """Verify _run returns 0 when exchanges are configured and checked.

    Given: Credentials mapping with a Kraken envelope,
    When: _run() is called,
    Then: Returns 0 after checking balances.
    """
    mock_load_credentials.return_value = {
        "kraken": {"api_key": "k", "api_secret": "s"},
    }
    mock_check.return_value = {"BTC": AccountBalance(currency="BTC", free=0.1, used=0.0, total=0.1)}
    result = await _run()
    assert result == 0
    mock_check.assert_awaited_once()
