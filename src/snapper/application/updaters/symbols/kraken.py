"""Kraken symbol mapping updater module.

This module provides symbol mapping updates for Kraken exchange.
It fetches trading pairs via both REST API and WebSocket verification,
including support for tokenized assets.
"""

import asyncio
import inspect
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import SymbolMappingUpdaterService
from snapper.config.settings import AppSettings
from snapper.data.models import SymbolMapping
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.symbols.mapper import make_native_symbol


@register_process(
    "kraken_symbol_mapping_updater",
    method="start",
    description="Kraken symbol mapping updater (REST + WebSocket verification)",
    priority=15,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "kraken"),
    enabled=True,
    mode="thread",
    args=[],
)
class KrakenSymbolMappingUpdaterService(SymbolMappingUpdaterService[KrakenExchangeClient]):
    """Symbol mapping updater for Kraken exchange.

    Fetches trading pairs from Kraken via:
    - CCXT load_markets() for standard pairs
    - REST API for tokenized assets
    - WebSocket verification for symbol validity

    Registered as one-shot task process with 6-hour update threshold.
    """

    VERIFICATION_TIMEOUT_SECONDS = 30.0

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default constructor kwargs.

        Args:
            settings: Application settings.

        Returns:
            Default kwargs with threshold and force settings.
        """
        return {
            "update_threshold_hours": 6,
            "force": False,
        }

    def __init__(self, update_threshold_hours: int = 6, force: bool = False) -> None:
        """Initialize the Kraken symbol updater.

        Args:
            update_threshold_hours: Hours between updates. Defaults to 6.
            force: Whether to bypass threshold check.
        """
        super().__init__(update_threshold_hours=update_threshold_hours, force=force)
        self._kraken_client: KrakenExchangeClient | None = None

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create Kraken exchange client.

        Returns:
            KrakenExchangeClient instance.
        """
        if self._kraken_client is None:
            self._kraken_client = KrakenExchangeClient()
        return self._kraken_client

    def _get_setting_key(self) -> str:
        """Get settings key for last update timestamp.

        Returns:
            Settings key string.
        """
        return "kraken_symbol_mappings_last_update"

    @staticmethod
    def _extract_ccxt_market_pair(symbol: str, market: dict[str, Any]) -> dict[str, str] | None:
        """Extract base/quote pair info from a single CCXT market entry.

        Args:
            symbol: CCXT symbol string.
            market: CCXT market data dictionary.

        Returns:
            Pair info dict, or None if required fields are missing.
        """
        rest_id = market.get("id", "")
        if not rest_id:
            return None
        info = market.get("info", {})
        base = info.get("base", "") or market.get("base", "")
        quote = info.get("quote", "") or market.get("quote", "")
        if not base or not quote:
            return None
        return {
            "rest_id": rest_id,
            "base": base,
            "quote": quote,
            "ccxt_symbol": symbol,
            "asset_class": "currency",
        }

    @staticmethod
    def _extract_tokenized_pair(pair_name: str, pair_data: Any) -> dict[str, Any] | None:
        """Extract base/quote from a tokenized asset pair entry.

        Args:
            pair_name: Kraken REST pair name.
            pair_data: Pair data dictionary from Kraken API.

        Returns:
            Pair info dict, or None if data is invalid.
        """
        if not isinstance(pair_data, dict):
            return None
        token_base: str = pair_data.get("base", "")
        token_quote: str = pair_data.get("quote", "")
        if not token_base or not token_quote:
            return None
        return {
            "base": token_base,
            "quote": token_quote,
            "ccxt_symbol": None,
            "asset_class": "tokenized_asset",
        }

    def _collect_ccxt_pairs(self, markets: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Collect asset pairs from CCXT markets data.

        Args:
            markets: CCXT markets dictionary.

        Returns:
            Dict mapping REST symbol to pair info.
        """
        asset_pairs: dict[str, dict[str, str]] = {}
        for symbol, market in markets.items():
            pair_info = self._extract_ccxt_market_pair(symbol, market)
            if pair_info is not None:
                rest_id = pair_info.pop("rest_id")
                asset_pairs[rest_id] = pair_info
        logger.debug(f"Loaded {len(asset_pairs)} pairs from CCXT markets")
        return asset_pairs

    def _collect_tokenized_pairs(
        self, tokenized_result: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        """Collect asset pairs from tokenized assets API response.

        Args:
            tokenized_result: Tokenized assets result dictionary.

        Returns:
            Dict mapping pair name to pair info.
        """
        pairs: dict[str, dict[str, Any]] = {}
        for pair_name, pair_data in tokenized_result.items():
            token_info = self._extract_tokenized_pair(pair_name, pair_data)
            if token_info is not None:
                pairs[pair_name] = token_info
        return pairs

    async def _fetch_ccxt_markets(self) -> tuple[Any, dict[str, Any]]:
        """Connect to Kraken and fetch CCXT markets data.

        Returns:
            Tuple of (ccxt_client, markets dict).
        """
        client = self._create_exchange_client()
        await client.connect()
        ccxt_client = client.get_ccxt_client()
        markets = await asyncio.to_thread(ccxt_client.load_markets)
        return ccxt_client, markets

    async def _fetch_tokenized_result(self, ccxt_client: Any) -> dict[str, Any]:
        """Fetch tokenized asset pairs from Kraken REST API.

        Args:
            ccxt_client: CCXT Kraken client instance.

        Returns:
            Dict of tokenized asset pair data.

        Raises:
            RuntimeError: If response result is not a dict.
        """
        tokenized_response: dict[str, Any] = await asyncio.to_thread(
            ccxt_client.publicGetAssetPairs,
            {"aclass_base": "tokenized_asset"},
        )
        tokenized_result: dict[str, Any] = tokenized_response.get("result", {})
        if not isinstance(tokenized_result, dict):
            raise RuntimeError(f"Unexpected tokenized result type: {type(tokenized_result)}")
        return tokenized_result

    async def load_kraken_rest_symbols(self) -> dict[str, dict[str, str]]:
        """Load symbols from CCXT and Kraken REST API.

        Fetches both standard pairs via CCXT and tokenized assets
        via direct REST API call.

        Returns:
            Dict mapping REST symbol to pair info (base, quote, ccxt_symbol).
        """
        logger.info("Loading symbols from CCXT markets + Kraken REST API")
        try:
            ccxt_client, markets = await self._fetch_ccxt_markets()
            asset_pairs: dict[str, Any] = self._collect_ccxt_pairs(markets)
            tokenized_result = await self._fetch_tokenized_result(ccxt_client)
            asset_pairs.update(self._collect_tokenized_pairs(tokenized_result))
            logger.info(
                f"Loaded {len(asset_pairs)} trading pairs "
                f"({len(markets)} CCXT + {len(tokenized_result)} tokenized)"
            )
            return asset_pairs
        except Exception as e:
            logger.error(f"Error loading Kraken symbols: {e}")
            raise

    def _normalize_currency(self, currency_code: str) -> str:
        """Normalize Kraken currency codes to standard format.

        Kraken uses prefixed codes (ZUSD, XXBT) that need normalization.

        Args:
            currency_code: Kraken currency code.

        Returns:
            Normalized currency code (e.g., "XXBT" -> "BTC").
        """
        currency_mapping = {
            "ZUSD": "USD",
            "ZEUR": "EUR",
            "ZGBP": "GBP",
            "ZJPY": "JPY",
            "ZCAD": "CAD",
            "ZAUD": "AUD",
            "ZCHF": "CHF",
            "XETH": "ETH",
            "XLTC": "LTC",
            "XETC": "ETC",
            "XREP": "REP",
            "XZEC": "ZEC",
            "XMLN": "MLN",
            "XXBT": "BTC",
            "XXDG": "DOGE",
            "XXRP": "XRP",
            "XXLM": "XLM",
            "XXMR": "XMR",
        }
        return currency_mapping.get(currency_code, currency_code)

    async def verify_websocket_symbols(
        self,
        ws_symbols: set[str],
    ) -> set[str]:
        """Verify symbols exist via WebSocket v2 API.

        Subscribes to instrument feed and collects available symbols
        to verify they're tradeable.

        Args:
            ws_symbols: Set of symbols to verify.

        Returns:
            Set of verified symbols that exist on WebSocket feed.
        """
        logger.info("Verifying {} symbols against WebSocket v2 API", len(ws_symbols))
        verified_symbols: set[str] = set()
        max_snapshot_time = 5.0
        client = self._create_exchange_client()
        try:
            verified_symbols = await self._collect_verified_symbols(
                client, ws_symbols, max_snapshot_time
            )
        except asyncio.CancelledError:
            logger.info(
                "WebSocket instrument verification cancelled ({}/{}) verified",
                len(verified_symbols),
                len(ws_symbols),
            )
            raise
        except Exception as e:
            logger.error(f"Error verifying symbols via WebSocket: {e}")
            logger.warning("Falling back to assume all symbols valid")
            return ws_symbols
        finally:
            await self._safe_disconnect_ws(client)
        self._log_unverified_symbols(verified_symbols, ws_symbols)
        return verified_symbols

    async def _collect_verified_symbols(
        self,
        client: Any,
        ws_symbols: set[str],
        timeout_seconds: float,
    ) -> set[str]:
        """Collect verified symbols from WebSocket instrument feed.

        Args:
            client: Kraken exchange client.
            ws_symbols: Set of symbols to look for.
            timeout_seconds: Maximum time to spend collecting.

        Returns:
            Set of symbols confirmed on the instrument feed.
        """
        verified: set[str] = set()
        try:
            async with asyncio.timeout(timeout_seconds):
                async for raw_instrument in client.subscribe_instruments(raw=True):
                    ws_symbol = raw_instrument.get("symbol")
                    if isinstance(ws_symbol, str) and ws_symbol in ws_symbols:
                        verified.add(ws_symbol)
                    if len(verified) == len(ws_symbols):
                        logger.info("All {} symbols verified via WebSocket", len(ws_symbols))
                        return verified
        except TimeoutError:
            logger.info(
                "Snapshot collection timeout ({:.1f}s): verified {}/{} symbols",
                timeout_seconds,
                len(verified),
                len(ws_symbols),
            )
        return verified

    @staticmethod
    async def _safe_disconnect_ws(client: Any) -> None:
        """Safely disconnect WebSocket client if possible.

        Args:
            client: Exchange client that may have a disconnect method.
        """
        disconnect = getattr(client, "disconnect_websocket", None)
        if not callable(disconnect):
            return
        try:
            result = disconnect()
            if inspect.isawaitable(result):
                await result
        except Exception as err:
            logger.debug("Error disconnecting websocket during verification: {}", err)

    @staticmethod
    def _log_unverified_symbols(verified: set[str], expected: set[str]) -> None:
        """Log a warning if some symbols could not be verified.

        Args:
            verified: Set of successfully verified symbols.
            expected: Set of symbols that were expected.
        """
        if len(verified) < len(expected):
            unverified = expected - verified
            logger.warning(
                "Only {}/{} symbols verified via WebSocket. Unverified sample: {}",
                len(verified),
                len(expected),
                list(unverified)[:10],
            )

    _FIAT_CURRENCIES: set[str] = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"}

    def _resolve_tokenized_asset(
        self, kraken_rest_symbol: str, base: str, quote: str
    ) -> tuple[str, str, str, str] | None:
        """Resolve tokenized asset pair into mapping components.

        Args:
            kraken_rest_symbol: Kraken REST pair name.
            base: Raw base currency from exchange.
            quote: Raw quote currency from exchange.

        Returns:
            Tuple of (native_symbol, ws_symbol, base_currency, quote_currency)
            or None if the pair should be skipped.
        """
        if not base.endswith("x"):
            logger.warning(
                f"Tokenized asset {kraken_rest_symbol} base '{base}' "
                f"does not end with 'x' - skipping"
            )
            return None
        quote_currency = self._normalize_currency(quote)
        if quote_currency not in self._FIAT_CURRENCIES:
            logger.warning(
                f"Tokenized asset {kraken_rest_symbol} has "
                f"unexpected quote '{quote_currency}' - skipping"
            )
            return None
        return base[:-1].upper(), f"{base}/{quote_currency}", base, quote_currency

    def _resolve_standard_pair(self, base: str, quote: str) -> tuple[str, str, str, str]:
        """Resolve standard currency pair into mapping components.

        Args:
            base: Raw base currency from exchange.
            quote: Raw quote currency from exchange.

        Returns:
            Tuple of (native_symbol, ws_symbol, base_currency, quote_currency).
        """
        base_currency = self._normalize_currency(base)
        quote_currency = self._normalize_currency(quote)
        ws_symbol = f"{base_currency}/{quote_currency}"
        native_symbol = make_native_symbol(base_currency, quote_currency)
        return native_symbol, ws_symbol, base_currency, quote_currency

    def _resolve_pair(
        self, kraken_rest_symbol: str, pair_info: dict[str, str]
    ) -> tuple[str, str, str, str] | None:
        """Resolve a single REST pair into mapping components.

        Args:
            kraken_rest_symbol: Kraken REST pair name.
            pair_info: Pair info dictionary with base, quote, asset_class.

        Returns:
            Tuple of (native_symbol, ws_symbol, base_currency, quote_currency)
            or None if the pair should be skipped.
        """
        base = pair_info.get("base", "")
        quote = pair_info.get("quote", "")
        if not base or not quote:
            return None
        asset_class = pair_info.get("asset_class", "currency")
        if asset_class == "tokenized_asset":
            return self._resolve_tokenized_asset(kraken_rest_symbol, base, quote)
        return self._resolve_standard_pair(base, quote)

    def _build_mappings_from_rest(
        self, kraken_rest_symbols: dict[str, dict[str, str]]
    ) -> tuple[dict[str, SymbolMapping], set[str]]:
        """Build SymbolMapping instances from REST symbol data.

        Args:
            kraken_rest_symbols: Dict mapping REST symbol to pair info.

        Returns:
            Tuple of (mappings dict, ws_symbols_to_verify set).
        """
        mappings: dict[str, SymbolMapping] = {}
        ws_symbols_to_verify: set[str] = set()
        for kraken_rest_symbol, pair_info in kraken_rest_symbols.items():
            resolved = self._resolve_pair(kraken_rest_symbol, pair_info)
            if resolved is None:
                continue
            native_symbol, ws_symbol, base_currency, quote_currency = resolved
            ws_symbols_to_verify.add(ws_symbol)
            mappings[native_symbol] = SymbolMapping(
                native_symbol=native_symbol,
                kraken_websocket_symbol=ws_symbol,
                kraken_rest_symbol=kraken_rest_symbol,
                ccxt_symbol=pair_info.get("ccxt_symbol"),
                base_currency=base_currency,
                quote_currency=quote_currency,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        return mappings, ws_symbols_to_verify

    async def build_verified_mappings(self) -> tuple[dict[str, SymbolMapping], bool]:
        """Build verified symbol mappings from REST and WebSocket data.

        Loads symbols from REST, verifies via WebSocket, and creates
        SymbolMapping instances.

        Returns:
            Tuple of (mappings dict, success boolean).
        """
        try:
            kraken_rest_symbols = await self.load_kraken_rest_symbols()
            logger.info(f"Loaded {len(kraken_rest_symbols)} symbols from REST API")
            mappings, ws_symbols_to_verify = self._build_mappings_from_rest(kraken_rest_symbols)
            verified_ws_symbols = await self.verify_websocket_symbols(ws_symbols_to_verify)
            verification_threshold = 0.95
            verification_rate = (
                len(verified_ws_symbols) / len(ws_symbols_to_verify)
                if ws_symbols_to_verify
                else 0.0
            )
            verification_success = verification_rate >= verification_threshold
            if not verification_success:
                logger.warning(
                    f"Verification rate {verification_rate:.1%} below threshold "
                    f"{verification_threshold:.0%}"
                )
            logger.info(
                f"Built {len(mappings)} mappings, "
                f"verification: {len(verified_ws_symbols)}/{len(ws_symbols_to_verify)} "
                f"({verification_rate:.1%})"
            )
            return mappings, verification_success
        except Exception as e:
            logger.error(f"Error building verified mappings: {e}")
            return {}, False

    async def _fetch_symbols(self, client: KrakenExchangeClient) -> list[dict[str, Any]]:
        """Fetch and verify symbols from Kraken.

        Builds verified mappings and converts to list format.

        Args:
            client: Kraken exchange client (unused, mappings built internally).

        Returns:
            List of symbol data dicts.

        Raises:
            RuntimeError: If WebSocket verification fails or no mappings.
        """
        mappings, verification_success = await self.build_verified_mappings()
        if not verification_success:
            raise RuntimeError("WebSocket verification failed - aborting update")
        if not mappings:
            raise RuntimeError("No mappings generated - aborting update")
        symbols = []
        for native_symbol, mapping in mappings.items():
            symbols.append(
                {
                    "native_symbol": native_symbol,
                    "kraken_websocket_symbol": mapping.kraken_websocket_symbol,
                    "kraken_rest_symbol": mapping.kraken_rest_symbol,
                    "ccxt_symbol": mapping.ccxt_symbol,
                    "base_currency": mapping.base_currency,
                    "quote_currency": mapping.quote_currency,
                }
            )
        logger.info(f"Fetched and verified {len(symbols)} Kraken symbols")
        return symbols

    _KRAKEN_UPDATE_FIELDS: tuple[str, ...] = (
        "kraken_websocket_symbol",
        "kraken_rest_symbol",
        "ccxt_symbol",
    )

    @staticmethod
    def _apply_kraken_field_updates(
        existing: SymbolMapping,
        symbol_data: dict[str, Any],
        fields: tuple[str, ...],
    ) -> bool:
        """Apply field-level updates to an existing mapping if values differ.

        Args:
            existing: Existing SymbolMapping ORM object.
            symbol_data: New symbol data dictionary.
            fields: Tuple of field names to compare and update.

        Returns:
            True if any field was updated.
        """
        updated = False
        for field_name in fields:
            if getattr(existing, field_name) != symbol_data[field_name]:
                setattr(existing, field_name, symbol_data[field_name])
                updated = True
        return updated

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Update database with Kraken symbol data.

        Creates new mappings or updates existing ones with
        kraken_websocket_symbol, kraken_rest_symbol, and ccxt_symbol.

        Args:
            symbols: List of symbol data dicts from Kraken.
        """
        assert self.repository is not None, "Repository not initialized"
        updated_count = 0
        created_count = 0
        try:
            with self.repository.get_session() as session:
                for symbol_data in symbols:
                    native_symbol = symbol_data["native_symbol"]
                    stmt = select(SymbolMapping).where(SymbolMapping.native_symbol == native_symbol)
                    existing = session.execute(stmt).scalar_one_or_none()
                    now = datetime.now(UTC)
                    if existing:
                        if self._apply_kraken_field_updates(
                            existing, symbol_data, self._KRAKEN_UPDATE_FIELDS
                        ):
                            existing.updated_at = now
                            updated_count += 1
                    else:
                        new_mapping = SymbolMapping(
                            native_symbol=native_symbol,
                            kraken_websocket_symbol=symbol_data["kraken_websocket_symbol"],
                            kraken_rest_symbol=symbol_data["kraken_rest_symbol"],
                            ccxt_symbol=symbol_data["ccxt_symbol"],
                            base_currency=symbol_data["base_currency"],
                            quote_currency=symbol_data["quote_currency"],
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(new_mapping)
                        created_count += 1
                session.commit()
                logger.info(
                    f"Kraken update complete: {created_count} created, "
                    f"{updated_count} updated (total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            raise
