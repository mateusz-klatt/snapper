"""Check wallet balances across all configured exchanges.

Usage:
    python scripts/check_balances.py

Requires: database with API keys configured (via settings UI or API).
"""

import asyncio
import sys
from collections.abc import Callable
from typing import Any

from loguru import logger

from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient

logger.remove()
logger.add(sys.stderr, level="WARNING")

ExchangeFactory = Callable[[], Any]


def build_exchange_factories(settings: Any) -> list[tuple[str, ExchangeFactory]]:
    """Build list of exchange client factories from settings.

    Only includes exchanges that have API keys configured.

    Args:
        settings: AppSettings instance with exchange credentials.

    Returns:
        List of (name, factory) tuples for configured exchanges.
    """
    factories: list[tuple[str, ExchangeFactory]] = []
    if settings.kraken_api_key and settings.kraken_api_secret:
        factories.append(
            (
                "Kraken",
                lambda: KrakenExchangeClient(
                    api_key=settings.kraken_api_key,
                    api_secret=settings.kraken_api_secret,
                ),
            )
        )
    if settings.kraken_futures_api_key and settings.kraken_futures_api_secret:
        factories.append(
            (
                "Kraken Futures",
                lambda: KrakenFuturesExchangeClient(
                    api_key=settings.kraken_futures_api_key,
                    api_secret=settings.kraken_futures_api_secret,
                ),
            )
        )
    if settings.zonda_api_key and settings.zonda_api_secret:
        factories.append(
            (
                "Zonda",
                lambda: ZondaExchangeClient(
                    api_key=settings.zonda_api_key,
                    api_secret=settings.zonda_api_secret,
                ),
            )
        )
    if settings.walutomat_api_key:
        private_key = settings.walutomat_private_key or None
        factories.append(
            (
                "Walutomat",
                lambda: WalutomatExchangeClient(
                    api_key=settings.walutomat_api_key,
                    private_key_data=private_key,
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


async def _run() -> int:
    """Fetch and display balances from all exchanges.

    Returns:
        Exit code (0 on success, 1 if no keys configured).
    """
    bootstrap = get_bootstrap_settings()
    settings_service = await get_settings_service(bootstrap.db_url, bootstrap.zmq_broker_xpub)
    settings = get_settings_with_service(settings_service)
    factories = build_exchange_factories(settings)
    if not factories:
        print("No exchange API keys configured in database settings.")
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
