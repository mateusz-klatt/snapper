"""Live order testing across all exchanges — capture real API responses.

Places real orders on each exchange using minimum amounts, captures
raw API responses as JSON fixtures for unit test generation.

Scenarios per exchange:
  1. passive_buy   — deep bid (well below market), create → fetch → cancel
  2. passive_sell  — deep ask (well above market), create → fetch → cancel
  3. aggressive_buy — cross the spread, fills immediately
  4. aggressive_sell — cross the spread, fills immediately
  5. cancel_inflight — create passive → immediately cancel

Usage:
    python scripts/test_live_orders.py [exchange] [scenario]

    python scripts/test_live_orders.py              # all exchanges, all scenarios
    python scripts/test_live_orders.py walutomat     # all Walutomat scenarios
    python scripts/test_live_orders.py kraken passive_buy
    python scripts/test_live_orders.py kraken_futures aggressive_buy

Output:
    Prints JSON fixtures to stdout, one per scenario. Copy into unit tests.
"""

import asyncio
import json
import sys
import time
from dataclasses import asdict
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from loguru import logger

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.infrastructure.symbols.functions import native_to_ccxt

logger.remove()
logger.add(sys.stderr, level="INFO")

DELAY_BETWEEN_SCENARIOS = 2.0
DELAY_AFTER_CREATE = 1.0


async def safe_get_ticker(client: Any, symbol: str) -> TickerSnapshot:
    """Get ticker with fallback for exchanges where get_ticker has quirks.

    Handles None timestamp from Kraken CCXT (sync client).

    Args:
        client: Exchange client instance.
        symbol: Native symbol (e.g., BTC-EUR).

    Returns:
        TickerSnapshot with current bid/ask/last.
    """
    try:
        result: TickerSnapshot = await client.get_ticker(symbol)
        return result
    except TypeError:
        ccxt_client = getattr(client, "_ccxt_client", None)
        if ccxt_client:
            from snapper.infrastructure.symbols.functions import native_to_ccxt

            ccxt_symbol = native_to_ccxt(symbol)
            data = await asyncio.to_thread(ccxt_client.fetch_ticker, ccxt_symbol)
            return TickerSnapshot(
                symbol=symbol,
                bid=float(data["bid"]),
                ask=float(data["ask"]),
                last=float(data["last"]),
                timestamp=float(data.get("timestamp") or 0) / 1000.0,
            )
        raise


def snapshot_to_dict(snap: ExchangeOrderSnapshot) -> dict[str, Any]:
    """Convert ExchangeOrderSnapshot to a JSON-serializable dict.

    Args:
        snap: Order snapshot to convert.

    Returns:
        Dictionary with enum values converted to strings.
    """
    raw = asdict(snap)
    for key, val in raw.items():
        if hasattr(val, "value"):
            raw[key] = val.value
    return raw


def emit(exchange: str, scenario: str, step: str, data: Any) -> None:
    """Print a structured JSON fixture line.

    Args:
        exchange: Exchange name.
        scenario: Test scenario name.
        step: Step within scenario (create/fetch/cancel).
        data: Payload to include in the JSON output.
    """
    print(json.dumps({"exchange": exchange, "scenario": scenario, "step": step, "data": data}))


async def get_settings() -> Any:
    """Load decrypted exchange credentials from ``wallet_credentials``.

    Wallet-scoped credentials live in the ``wallet_credentials`` table
    rather than ``AppSettings``. This helper queries every active
    live-money credential row, decrypts the Fernet envelope with the
    master password, and returns a ``SimpleNamespace`` with the
    attribute names (``kraken_api_key`` / ``kraken_api_secret`` /
    etc.) the per-exchange runners downstream expect.

    Returns:
        ``SimpleNamespace`` with eight credential attributes (empty
        string when the corresponding wallet credential row is absent).
    """
    bootstrap = get_bootstrap_settings()
    repository = get_repository(bootstrap.db_url)
    rows = await repository.list_active_wallet_credentials(as_of=datetime.now(UTC))
    encryption = get_encryption_service()
    attrs: dict[str, str] = {
        "walutomat_api_key": "",
        "walutomat_private_key": "",
        "kraken_api_key": "",
        "kraken_api_secret": "",
        "kraken_futures_api_key": "",
        "kraken_futures_api_secret": "",
        "zonda_api_key": "",
        "zonda_api_secret": "",
    }
    for row in rows:
        if row["credential_type"] == "paper":
            continue
        envelope = json.loads(encryption.decrypt(row["encrypted_payload"]))
        exchange = row["exchange"]
        if exchange == "kraken":
            attrs["kraken_api_key"] = envelope.get("api_key", "")
            attrs["kraken_api_secret"] = envelope.get("api_secret", "")
        elif exchange == "kraken_futures":
            attrs["kraken_futures_api_key"] = envelope.get("api_key", "")
            attrs["kraken_futures_api_secret"] = envelope.get("api_secret", "")
        elif exchange == "walutomat":
            attrs["walutomat_api_key"] = envelope.get("api_key", "")
            attrs["walutomat_private_key"] = envelope.get("private_key_pem", "")
        elif exchange == "zonda":
            attrs["zonda_api_key"] = envelope.get("api_key", "")
            attrs["zonda_api_secret"] = envelope.get("api_secret", "")
    return SimpleNamespace(**attrs)


async def run_walutomat(settings: Any, scenarios: list[str] | None = None) -> None:
    """Test order lifecycle on Walutomat EUR-PLN.

    Walutomat specifics:
    - Only limit orders (no market orders, P2P matching)
    - create_order returns minimal {"success": true, "result": {"orderId": "..."}}
    - cancel_order returns full order details via /orders/close
    - get_order returns full details via /orders?orderId=...
    - Minimum volume: 1.00 EUR (2 decimal places)

    Args:
        settings: AppSettings with exchange credentials.
        scenarios: Optional list of scenarios to run.
    """
    if not settings.walutomat_api_key:
        logger.warning("Walutomat: no API key, skipping")
        return
    all_scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    run = scenarios or all_scenarios
    client = WalutomatExchangeClient(
        api_key=settings.walutomat_api_key,
        private_key_data=settings.walutomat_private_key,
    )
    async with client:
        ticker = await client.get_ticker("EUR-PLN")
        mid = (ticker.bid + ticker.ask) / 2
        logger.info(f"Walutomat EUR-PLN: bid={ticker.bid}, ask={ticker.ask}, mid={mid:.4f}")

        if "passive_buy" in run:
            logger.info("Walutomat: passive_buy — deep bid")
            passive_price = round(ticker.bid * 0.95, 4)
            request = ExchangeOrderRequest(
                symbol="EUR-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=passive_price,
                client_order_id=f"test-pass-buy-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("walutomat", "passive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id)
            emit("walutomat", "passive_buy", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id)
            emit("walutomat", "passive_buy", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "passive_sell" in run:
            logger.info("Walutomat: passive_sell ��� deep ask")
            passive_price = round(ticker.ask * 1.05, 4)
            request = ExchangeOrderRequest(
                symbol="EUR-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=passive_price,
                client_order_id=f"test-pass-sell-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("walutomat", "passive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id)
            emit("walutomat", "passive_sell", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id)
            emit("walutomat", "passive_sell", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_buy" in run:
            logger.info("Walutomat: aggressive_buy — cross spread at ask")
            request = ExchangeOrderRequest(
                symbol="EUR-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=round(ticker.ask * 1.01, 4),
                client_order_id=f"test-aggr-buy-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("walutomat", "aggressive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE * 3)

            fetched = await client.get_order(snap.id)
            emit("walutomat", "aggressive_buy", "fetch", snapshot_to_dict(fetched))
            if fetched.status.value != "closed":
                canceled = await client.cancel_order(snap.id)
                emit("walutomat", "aggressive_buy", "cancel_unfilled", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_sell" in run:
            logger.info("Walutomat: aggressive_sell — cross spread at bid")
            request = ExchangeOrderRequest(
                symbol="EUR-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=round(ticker.bid * 0.99, 4),
                client_order_id=f"test-aggr-sell-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("walutomat", "aggressive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE * 3)

            fetched = await client.get_order(snap.id)
            emit("walutomat", "aggressive_sell", "fetch", snapshot_to_dict(fetched))
            if fetched.status.value != "closed":
                canceled = await client.cancel_order(snap.id)
                emit("walutomat", "aggressive_sell", "cancel_unfilled", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "cancel_inflight" in run:
            logger.info("Walutomat: cancel_inflight — create+cancel immediately")
            request = ExchangeOrderRequest(
                symbol="EUR-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=round(ticker.bid * 0.95, 4),
                client_order_id=f"test-cancel-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("walutomat", "cancel_inflight", "create", snapshot_to_dict(snap))

            canceled = await client.cancel_order(snap.id)
            emit("walutomat", "cancel_inflight", "cancel", snapshot_to_dict(canceled))


async def run_kraken_spot(settings: Any, scenarios: list[str] | None = None) -> None:
    """Test order lifecycle on Kraken Spot BTC-EUR via CCXT path.

    Kraken specifics:
    - CCXT create_order may return status=None → follow-up fetch_order
    - CCXT cancel returns empty → follow-up fetch_order
    - Minimum: 0.0001 BTC
    - Leverage via params["leverage"] (integer, 2-5x)
    - post_only via params["postOnly"]

    Args:
        settings: AppSettings with exchange credentials.
        scenarios: Optional list of scenarios to run.
    """
    if not settings.kraken_api_key or not settings.kraken_api_secret:
        logger.warning("Kraken Spot: no API keys, skipping")
        return
    all_scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    run = scenarios or all_scenarios
    client = KrakenExchangeClient(
        api_key=settings.kraken_api_key,
        api_secret=settings.kraken_api_secret,
    )
    async with client:
        ticker = await safe_get_ticker(client, "BTC-EUR")
        mid = (ticker.bid + ticker.ask) / 2
        logger.info(f"Kraken BTC-EUR: bid={ticker.bid}, ask={ticker.ask}, mid={mid:.2f}")

        if "passive_buy" in run:
            logger.info("Kraken: passive_buy — deep bid, post_only")
            passive_price = round(ticker.bid * 0.90, 1)
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=passive_price,
                post_only=True,
            )
            snap = await client.create_order(request)
            emit("kraken", "passive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, "BTC-EUR")
            emit("kraken", "passive_buy", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id, "BTC-EUR")
            emit("kraken", "passive_buy", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "passive_sell" in run:
            logger.info("Kraken: passive_sell — deep ask, post_only")
            passive_price = round(ticker.ask * 1.10, 1)
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=passive_price,
                post_only=True,
            )
            snap = await client.create_order(request)
            emit("kraken", "passive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, "BTC-EUR")
            emit("kraken", "passive_sell", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id, "BTC-EUR")
            emit("kraken", "passive_sell", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_buy" in run:
            logger.info("Kraken: aggressive_buy — at ask, immediate fill")
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.ask * 1.005, 1),
            )
            snap = await client.create_order(request)
            emit("kraken", "aggressive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, "BTC-EUR")
            emit("kraken", "aggressive_buy", "fetch", snapshot_to_dict(fetched))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_sell" in run:
            logger.info("Kraken: aggressive_sell — at bid, immediate fill")
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.bid * 0.995, 1),
            )
            snap = await client.create_order(request)
            emit("kraken", "aggressive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, "BTC-EUR")
            emit("kraken", "aggressive_sell", "fetch", snapshot_to_dict(fetched))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "cancel_inflight" in run:
            logger.info("Kraken: cancel_inflight — create+cancel immediately")
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.bid * 0.90, 1),
            )
            snap = await client.create_order(request)
            emit("kraken", "cancel_inflight", "create", snapshot_to_dict(snap))

            canceled = await client.cancel_order(snap.id, "BTC-EUR")
            emit("kraken", "cancel_inflight", "cancel", snapshot_to_dict(canceled))


async def run_kraken_futures(settings: Any, scenarios: list[str] | None = None) -> None:
    """Test order lifecycle on Kraken Futures PF_XBTUSD.

    Kraken Futures specifics:
    - SDK-based (not CCXT), Trade.create_order() / cancel_order()
    - cancel returns minimal cancelStatus, need get_order for details
    - Leverage per-instrument (not per-order)
    - reduceOnly flag supported
    - post_only via orderType="post"
    - Minimum: 0.0001 BTC for PF_XBTUSD (linear perpetual)

    Args:
        settings: AppSettings with exchange credentials.
        scenarios: Optional list of scenarios to run.
    """
    if not settings.kraken_futures_api_key or not settings.kraken_futures_api_secret:
        logger.warning("Kraken Futures: no API keys, skipping")
        return
    all_scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    run = scenarios or all_scenarios
    symbol = "BTC-USD-PERP"
    client = KrakenFuturesExchangeClient(
        api_key=settings.kraken_futures_api_key,
        api_secret=settings.kraken_futures_api_secret,
    )
    async with client:

        ccxt_symbol = native_to_ccxt(symbol)
        ticker = await client.get_ticker(ccxt_symbol)
        mid = (ticker.bid + ticker.ask) / 2
        logger.info(f"Kraken Futures {symbol}: bid={ticker.bid}, ask={ticker.ask}, mid={mid:.2f}")

        if "passive_buy" in run:
            logger.info("Futures: passive_buy — deep bid, post_only")
            passive_price = round(ticker.bid * 0.90, 1)
            request = ExchangeOrderRequest(
                symbol=symbol,
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=passive_price,
                client_order_id=f"test-pass-buy-{int(time.time())}",
                post_only=True,
            )
            snap = await client.create_order(request)
            emit("kraken_futures", "passive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, symbol)
            emit("kraken_futures", "passive_buy", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id, symbol)
            emit("kraken_futures", "passive_buy", "cancel", snapshot_to_dict(canceled))

            fetched_after = await client.get_order(snap.id, symbol)
            emit(
                "kraken_futures",
                "passive_buy",
                "fetch_after_cancel",
                snapshot_to_dict(fetched_after),
            )
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "passive_sell" in run:
            logger.info("Futures: passive_sell — deep ask, post_only")
            passive_price = round(ticker.ask * 1.10, 1)
            request = ExchangeOrderRequest(
                symbol=symbol,
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=passive_price,
                client_order_id=f"test-pass-sell-{int(time.time())}",
                post_only=True,
            )
            snap = await client.create_order(request)
            emit("kraken_futures", "passive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, symbol)
            emit("kraken_futures", "passive_sell", "fetch", snapshot_to_dict(fetched))

            canceled = await client.cancel_order(snap.id, symbol)
            emit("kraken_futures", "passive_sell", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_buy" in run:
            logger.info("Futures: aggressive_buy — at ask, immediate fill")
            request = ExchangeOrderRequest(
                symbol=symbol,
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.ask * 1.005, 1),
                client_order_id=f"test-aggr-buy-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("kraken_futures", "aggressive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, symbol)
            emit("kraken_futures", "aggressive_buy", "fetch", snapshot_to_dict(fetched))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_sell" in run:
            logger.info("Futures: aggressive_sell — at bid, immediate fill")
            request = ExchangeOrderRequest(
                symbol=symbol,
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.bid * 0.995, 1),
                client_order_id=f"test-aggr-sell-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("kraken_futures", "aggressive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            fetched = await client.get_order(snap.id, symbol)
            emit("kraken_futures", "aggressive_sell", "fetch", snapshot_to_dict(fetched))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "cancel_inflight" in run:
            logger.info("Futures: cancel_inflight — create+cancel immediately")
            request = ExchangeOrderRequest(
                symbol=symbol,
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=0.0001,
                price=round(ticker.bid * 0.90, 1),
                client_order_id=f"test-cancel-{int(time.time())}",
            )
            snap = await client.create_order(request)
            emit("kraken_futures", "cancel_inflight", "create", snapshot_to_dict(snap))

            canceled = await client.cancel_order(snap.id, symbol)
            emit("kraken_futures", "cancel_inflight", "cancel", snapshot_to_dict(canceled))


async def run_zonda(settings: Any, scenarios: list[str] | None = None) -> None:
    """Test order lifecycle on Zonda BTC-PLN via CCXT.

    Zonda specifics:
    - CCXT create_order may return filled=None, remaining=None
    - cancel_order requires fetch_open_orders first (to get side+price)
    - No fetchOrder support → uses fetch_open_orders or findOrders
    - client_order_id always null in responses
    - Minimum: 0.00001 BTC (5e-05)

    Args:
        settings: AppSettings with exchange credentials.
        scenarios: Optional list of scenarios to run.
    """
    if not settings.zonda_api_key or not settings.zonda_api_secret:
        logger.warning("Zonda: no API keys, skipping")
        return
    all_scenarios = [
        "passive_buy",
        "passive_sell",
        "aggressive_buy",
        "aggressive_sell",
        "cancel_inflight",
    ]
    run = scenarios or all_scenarios
    client = ZondaExchangeClient(
        api_key=settings.zonda_api_key,
        api_secret=settings.zonda_api_secret,
    )
    async with client:
        ticker = await client.get_ticker("BTC-PLN")
        mid = (ticker.bid + ticker.ask) / 2
        logger.info(f"Zonda BTC-PLN: bid={ticker.bid}, ask={ticker.ask}, mid={mid:.2f}")

        if "passive_buy" in run:
            logger.info("Zonda: passive_buy — deep bid")
            passive_price = round(ticker.bid * 0.90, 2)
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=passive_price,
            )
            snap = await client.create_order(request)
            emit("zonda", "passive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            canceled = await client.cancel_order(snap.id, "BTC-PLN")
            emit("zonda", "passive_buy", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "passive_sell" in run:
            logger.info("Zonda: passive_sell — deep ask")
            passive_price = round(ticker.ask * 1.10, 2)
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=passive_price,
            )
            snap = await client.create_order(request)
            emit("zonda", "passive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_AFTER_CREATE)

            canceled = await client.cancel_order(snap.id, "BTC-PLN")
            emit("zonda", "passive_sell", "cancel", snapshot_to_dict(canceled))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_buy" in run:
            logger.info("Zonda: aggressive_buy — at ask")
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=round(ticker.ask * 1.01, 2),
            )
            snap = await client.create_order(request)
            emit("zonda", "aggressive_buy", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "aggressive_sell" in run:
            logger.info("Zonda: aggressive_sell — at bid")
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=round(ticker.bid * 0.99, 2),
            )
            snap = await client.create_order(request)
            emit("zonda", "aggressive_sell", "create", snapshot_to_dict(snap))
            await asyncio.sleep(DELAY_BETWEEN_SCENARIOS)

        if "cancel_inflight" in run:
            logger.info("Zonda: cancel_inflight — create+cancel immediately")
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=round(ticker.bid * 0.90, 2),
            )
            snap = await client.create_order(request)
            emit("zonda", "cancel_inflight", "create", snapshot_to_dict(snap))

            canceled = await client.cancel_order(snap.id, "BTC-PLN")
            emit("zonda", "cancel_inflight", "cancel", snapshot_to_dict(canceled))


EXCHANGE_RUNNERS = {
    "walutomat": run_walutomat,
    "kraken": run_kraken_spot,
    "kraken_futures": run_kraken_futures,
    "zonda": run_zonda,
}


async def _run() -> int:
    """Main entry point — parse args and run selected exchanges/scenarios.

    Returns:
        Exit code (0 on success, 1 on error).
    """
    args = sys.argv[1:]
    exchange_filter = args[0] if args else None
    scenario_filter = args[1:] if len(args) > 1 else None

    settings = await get_settings()

    if exchange_filter and exchange_filter not in EXCHANGE_RUNNERS:
        print(f"Unknown exchange: {exchange_filter}")
        print(f"Available: {', '.join(EXCHANGE_RUNNERS)}")
        return 1

    runners = (
        {exchange_filter: EXCHANGE_RUNNERS[exchange_filter]}
        if exchange_filter
        else EXCHANGE_RUNNERS
    )

    for name, runner in runners.items():
        logger.info(f"=== {name.upper()} ===")
        try:
            await runner(settings, scenario_filter)
        except Exception as exc:
            logger.error(f"{name}: {exc}")
            emit(name, "ERROR", "exception", str(exc))

    return 0


def main() -> int:
    """Entry point.

    Returns:
        Exit code (0 on success).
    """
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
