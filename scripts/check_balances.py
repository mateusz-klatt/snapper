"""Check wallet balances across all configured exchanges.

Usage:
    python scripts/check_balances.py

Requires: database with ``wallet_credentials`` rows seeded for the
live-money wallet(s). Credentials live in the ``wallet_credentials``
table — the script enumerates active rows via ``CredentialResolver``
and builds one exchange client per row.
"""

import asyncio
import json
import sys
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.security.encryption import get_encryption_service

logger.remove()
logger.add(sys.stderr, level="WARNING")

ExchangeFactory = Callable[[], Any]
CredentialEnvelopes = dict[str, dict[str, str]]


def build_exchange_factories(
    credentials_by_exchange: CredentialEnvelopes,
) -> list[tuple[str, ExchangeFactory]]:
    """Build list of exchange client factories from decrypted credentials.

    Only includes exchanges whose credential envelope carries the
    fields needed to construct a client — e.g. ``api_key`` +
    ``api_secret`` for Kraken / Zonda / Kraken Futures, and
    ``api_key`` + ``private_key_pem`` for Walutomat.

    Args:
        credentials_by_exchange: Mapping of exchange name to decrypted
            credential dict (the envelope shape depends on the
            credential_type — see ``WalletCredential`` docstring).

    Returns:
        List of (display name, factory) tuples for configured exchanges.
    """
    factories: list[tuple[str, ExchangeFactory]] = []
    kraken = credentials_by_exchange.get("kraken", {})
    if kraken.get("api_key") and kraken.get("api_secret"):
        factories.append(
            (
                "Kraken",
                lambda: KrakenExchangeClient(
                    api_key=kraken["api_key"],
                    api_secret=kraken["api_secret"],
                ),
            )
        )
    kraken_futures = credentials_by_exchange.get("kraken_futures", {})
    if kraken_futures.get("api_key") and kraken_futures.get("api_secret"):
        factories.append(
            (
                "Kraken Futures",
                lambda: KrakenFuturesExchangeClient(
                    api_key=kraken_futures["api_key"],
                    api_secret=kraken_futures["api_secret"],
                ),
            )
        )
    zonda = credentials_by_exchange.get("zonda", {})
    if zonda.get("api_key") and zonda.get("api_secret"):
        factories.append(
            (
                "Zonda",
                lambda: ZondaExchangeClient(
                    api_key=zonda["api_key"],
                    api_secret=zonda["api_secret"],
                ),
            )
        )
    walutomat = credentials_by_exchange.get("walutomat", {})
    if walutomat.get("api_key"):
        factories.append(
            (
                "Walutomat",
                lambda: WalutomatExchangeClient(
                    api_key=walutomat["api_key"],
                    private_key_data=walutomat.get("private_key_pem") or None,
                ),
            )
        )
    return factories


async def check_single_exchange(name: str, factory: ExchangeFactory) -> dict[str, AccountBalance]:
    """Connect to an exchange and fetch non-zero balances.

    Args:
        name: Exchange display name.
        factory: Callable returning exchange client instance.

    Returns:
        Dict of currency to balance for non-zero holdings.
    """
    try:
        client = factory()
        async with client:
            result = await client.get_balance()
            return {k: v for k, v in result.items() if v.total > 0}
    except Exception as e:
        print(f"  {name}: SKIP ({e})")
        return {}


def format_balances(all_balances: dict[str, dict[str, AccountBalance]]) -> list[str]:
    """Format balance data into display lines.

    Args:
        all_balances: Dict of exchange name to currency balances.

    Returns:
        List of formatted lines ready for printing.
    """
    lines: list[str] = []
    for name, assets in all_balances.items():
        if not assets:
            lines.append(f"{name}: (no non-zero balances)")
            continue
        lines.append(f"{name}:")
        for currency, bal in sorted(assets.items()):
            parts = [f"  {currency:>8s}: total={bal.total:>14.8f}"]
            if bal.used > 0:
                parts.append(f"  used={bal.used:.8f}")
            lines.append("".join(parts))
        lines.append("")
    return lines


async def _load_live_credentials() -> CredentialEnvelopes:
    """Load decrypted credentials for all live-money wallet rows.

    Queries ``wallet_credentials`` via the repository, skips paper
    rows (paper wallets have no external exchange to query), and
    decrypts each envelope with the master-password Fernet key. When
    multiple live wallets have credentials for the same exchange, the
    last one wins — single-user deployments typically have exactly
    one live wallet so this degenerate case does not arise.
    """
    bootstrap = get_bootstrap_settings()
    repository = get_repository(bootstrap.db_url)
    rows = await repository.list_active_wallet_credentials(as_of=datetime.now(UTC))
    encryption = get_encryption_service()
    credentials: CredentialEnvelopes = {}
    for row in rows:
        if row["credential_type"] == "paper":
            continue
        envelope_json = encryption.decrypt(row["encrypted_payload"])
        envelope = json.loads(envelope_json)
        credentials[row["exchange"]] = envelope
    return credentials


async def _run() -> int:
    """Fetch and display balances from all exchanges.

    Returns:
        Exit code (0 on success, 1 if no keys configured).
    """
    credentials_by_exchange = await _load_live_credentials()
    factories = build_exchange_factories(credentials_by_exchange)
    if not factories:
        print("No exchange credentials configured in wallet_credentials.")
        return 1
    print(f"Checking {len(factories)} exchange(s)...\n")
    all_balances: dict[str, dict[str, AccountBalance]] = {}
    for name, factory in factories:
        all_balances[name] = await check_single_exchange(name, factory)
    for line in format_balances(all_balances):
        print(line)
    return 0


def main() -> int:
    """Entry point for balance checker.

    Returns:
        Exit code (0 on success).
    """
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
