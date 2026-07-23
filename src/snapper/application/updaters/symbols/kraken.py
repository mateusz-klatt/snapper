"""Kraken symbol updater module.

This module provides symbol updates for Kraken exchange.
It fetches trading pairs via both REST API and WebSocket verification,
including support for tokenized assets.
"""

import asyncio
import inspect
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any
from typing import TypedDict
from typing import cast

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from snapper.application.portfolio.spot_precision_certification import KRAKEN_INSTRUMENT_SPEC_SOURCE
from snapper.application.portfolio.spot_precision_certification import (
    kraken_instrument_precision_version,
)
from snapper.application.process_manager.process_parameters import SymbolUpdaterParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import PRESERVE_EXISTING
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.config.settings import AppSettings
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import FundingRate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.symbols.mapper import make_native_symbol

_SEQ_KEY_CAPABILITIES = "capabilities"
_SEQ_KEY_FUNDING = "funding_rates"

_SPOT_ROLLOVER_RATE_LONG = 0.00025
_SPOT_ROLLOVER_RATE_SHORT = 0.00010
SPOT_ROLLOVER_RATES_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

_BTNL_WIRE_SUFFIX = ":BTNL"
_BTNL_NATIVE_SUFFIX = "-BTNL"
_BTNL_CAPABILITY_REASON = (
    "Kraken WS relay of Bitnomial spot venue (CFTC-regulated US DCM) — market data "
    "only; direct Bitnomial order route not integrated"
)
_BTNL_DISCOVERY_WINDOW_SECONDS = 90.0


class _KrakenPairInfo(TypedDict, total=False):
    """Normalized Kraken REST pair fields used before native-symbol mapping."""

    base: str
    quote: str
    ccxt_symbol: str | None
    asset_class: str
    margin: str
    metadata: InstrumentMetadataInput


class _KrakenExtractedPair(_KrakenPairInfo):
    """CCXT pair extraction result including its Kraken REST identifier."""

    rest_id: str


def _positive_decimal(value: object) -> Decimal | None:
    """Parse a finite positive venue number without a float-to-Decimal conversion."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation, ValueError:
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _precision_decimals(value: object) -> int | None:
    """Derive decimal places from a finite non-negative CCXT precision increment."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation, ValueError:
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    exponent = cast(int, parsed.normalize().as_tuple().exponent)
    return max(0, -exponent)


def _amount_limit(market: dict[str, Any], key: str) -> float | None:
    """Read one positive CCXT amount limit from the external market payload."""
    limits = market.get("limits")
    if not isinstance(limits, dict):
        return None
    amount = limits.get("amount")
    if not isinstance(amount, dict):
        return None
    parsed = _positive_decimal(amount.get(key))
    return float(parsed) if parsed is not None else None


def _spot_metadata(market: dict[str, Any], observed_at: datetime) -> InstrumentMetadataInput:
    """Build the atomic spot metadata block from one CCXT market definition."""
    precision = market.get("precision")
    price_precision = precision.get("price") if isinstance(precision, dict) else None
    amount_precision = precision.get("amount") if isinstance(precision, dict) else None
    cost_precision = precision.get("cost") if isinstance(precision, dict) else None
    tick_decimal = _positive_decimal(price_precision)
    lot_decimal = _positive_decimal(amount_precision)
    min_order_size = _amount_limit(market, "min")
    max_order_size = _amount_limit(market, "max")
    cost_decimals = _precision_decimals(cost_precision)
    qty_decimals = _precision_decimals(amount_precision)
    active = market.get("active")
    status = None
    if active is True:
        status = "active"
    elif active is False:
        status = "inactive"
    version = kraken_instrument_precision_version(
        tick_size=tick_decimal,
        lot_size=lot_decimal,
        min_order_size=min_order_size,
        max_order_size=max_order_size,
        cost_decimals=cost_decimals,
        qty_decimals=qty_decimals,
        status=status,
        quantity_unit="base_asset",
    )
    return InstrumentMetadataInput(
        tick_size=float(tick_decimal) if tick_decimal is not None else None,
        lot_size=float(lot_decimal) if lot_decimal is not None else None,
        min_order_size=min_order_size,
        max_order_size=max_order_size,
        cost_decimals=cost_decimals,
        qty_decimals=qty_decimals,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status=status,
        contract_size=None,
        quantity_unit="base_asset",
        spec_source=KRAKEN_INSTRUMENT_SPEC_SOURCE,
        spec_version=version,
        spec_observed_at=observed_at,
        unit_certified=False,
    )


@register_process(
    "kraken_symbol_updater",
    method="start",
    description="Kraken symbol updater",
    priority=15,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "kraken"),
    parameters_model=SymbolUpdaterParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenSymbolUpdaterService(SymbolUpdaterService[KrakenExchangeClient]):
    """Symbol updater for Kraken exchange.

    Fetches trading pairs from Kraken via:
    - CCXT load_markets() for standard pairs
    - REST API for tokenized assets
    - WebSocket verification for symbol validity

    Registered as one-shot task process with 6-hour update threshold.
    """

    VERIFICATION_TIMEOUT_SECONDS = 5.0

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters.

        Args:
            settings: Application settings.

        Returns:
            Default parameters with threshold and force settings.
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
        return "kraken_symbols_last_update"

    @staticmethod
    def _extract_ccxt_market_pair(
        symbol: str,
        market: dict[str, Any],
        observed_at: datetime | None = None,
    ) -> _KrakenExtractedPair | None:
        """Extract base/quote pair info from a single CCXT market entry.

        Args:
            symbol: CCXT symbol string.
            market: CCXT market data dictionary.
            observed_at: Shared UTC catalog observation time, or current UTC when omitted.

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
            "margin": "true" if market.get("margin") else "false",
            "metadata": _spot_metadata(market, observed_at or datetime.now(UTC)),
        }

    @staticmethod
    def _extract_tokenized_pair(pair_name: str, pair_data: Any) -> _KrakenPairInfo | None:
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

    def _collect_ccxt_pairs(
        self,
        markets: dict[str, Any],
        observed_at: datetime | None = None,
    ) -> dict[str, _KrakenPairInfo]:
        """Collect asset pairs from CCXT markets data.

        Args:
            markets: CCXT markets dictionary.
            observed_at: Shared UTC catalog observation time, or current UTC when omitted.

        Returns:
            Dict mapping REST symbol to pair info.
        """
        asset_pairs: dict[str, _KrakenPairInfo] = {}
        observation_time = observed_at or datetime.now(UTC)
        for symbol, market in markets.items():
            pair_info = self._extract_ccxt_market_pair(symbol, market, observation_time)
            if pair_info is not None:
                asset_pairs[pair_info["rest_id"]] = _KrakenPairInfo(
                    base=pair_info["base"],
                    quote=pair_info["quote"],
                    ccxt_symbol=pair_info["ccxt_symbol"],
                    asset_class=pair_info["asset_class"],
                    margin=pair_info["margin"],
                    metadata=pair_info["metadata"],
                )
        logger.debug(f"Loaded {len(asset_pairs)} pairs from CCXT markets")
        return asset_pairs

    def _collect_tokenized_pairs(
        self, tokenized_result: dict[str, Any]
    ) -> dict[str, _KrakenPairInfo]:
        """Collect asset pairs from tokenized assets API response.

        Args:
            tokenized_result: Tokenized assets result dictionary.

        Returns:
            Dict mapping pair name to pair info.
        """
        pairs: dict[str, _KrakenPairInfo] = {}
        for pair_name, pair_data in tokenized_result.items():
            token_info = self._extract_tokenized_pair(pair_name, pair_data)
            if token_info is not None:
                pairs[pair_name] = token_info
        return pairs

    async def _fetch_ccxt_markets(self) -> tuple[Any, dict[str, Any]]:
        """Connect to Kraken and fetch CCXT markets data.

        The blocking CCXT call rides the client-owned bounded REST pool
        (audit P1-5) — never the loop's shared default executor — so a
        stalled Kraken REST round-trip cannot starve unrelated
        ``to_thread`` users process-wide. The client is the updater's
        cached singleton; its lifecycle (connect at run start,
        disconnect in the run ``finally``) is owned by
        ``SymbolUpdaterService.run``.

        Returns:
            Tuple of (ccxt_client, markets dict).
        """
        client = self._create_exchange_client()
        await client.connect()
        ccxt_client = client.get_ccxt_client()
        markets = await client._dispatch_blocking(ccxt_client.load_markets)
        return ccxt_client, markets

    async def _fetch_tokenized_result(self, ccxt_client: Any) -> dict[str, Any]:
        """Fetch tokenized asset pairs from Kraken REST API.

        Dispatches through the cached client's bounded REST pool — see
        ``_fetch_ccxt_markets`` for the rationale.

        Args:
            ccxt_client: CCXT Kraken client instance.

        Returns:
            Dict of tokenized asset pair data.

        Raises:
            RuntimeError: If response result is not a dict.
        """
        client = self._create_exchange_client()
        tokenized_response: dict[str, Any] = await client._dispatch_blocking(
            ccxt_client.publicGetAssetPairs,
            {"aclass_base": "tokenized_asset"},
        )
        tokenized_result: dict[str, Any] = tokenized_response.get("result", {})
        if not isinstance(tokenized_result, dict):
            raise RuntimeError(f"Unexpected tokenized result type: {type(tokenized_result)}")
        return tokenized_result

    async def load_kraken_rest_symbols(self) -> dict[str, _KrakenPairInfo]:
        """Load symbols from CCXT and Kraken REST API.

        Fetches both standard pairs via CCXT and tokenized assets
        via direct REST API call.

        Returns:
            Dict mapping REST symbol to pair info (base, quote, ccxt_symbol).
        """
        logger.info("Loading symbols from CCXT markets + Kraken REST API")
        try:
            ccxt_client, markets = await self._fetch_ccxt_markets()
            asset_pairs = self._collect_ccxt_pairs(markets, datetime.now(UTC))
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
    ) -> tuple[set[str], list[dict[str, str]]]:
        """Verify symbols and discover WS-only instruments via WebSocket v2 API.

        Subscribes to instrument feed, verifies REST-derived symbols exist,
        and collects symbols present only on WebSocket (not in REST markets).

        Args:
            ws_symbols: Set of REST-derived symbols to verify.

        Returns:
            Tuple of (verified symbols, WS-only instrument dicts).
            On error fallback, returns (all ws_symbols, empty WS-only list).
        """
        logger.info("Verifying {} symbols against WebSocket v2 API", len(ws_symbols))
        verified_symbols: set[str] = set()
        ws_only_instruments: list[dict[str, str]] = []
        max_snapshot_time = self.VERIFICATION_TIMEOUT_SECONDS
        client = self._create_exchange_client()
        try:
            verified_symbols, ws_only_instruments = await self._collect_verified_symbols(
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
            return ws_symbols, []
        finally:
            await self._safe_disconnect_ws(client)
        self._log_unverified_symbols(verified_symbols, ws_symbols)
        return verified_symbols, ws_only_instruments

    async def _collect_verified_symbols(
        self,
        client: Any,
        ws_symbols: set[str],
        timeout_seconds: float,
    ) -> tuple[set[str], list[dict[str, str]]]:
        """Collect verified symbols and WS-only instruments from feed.

        Reads the full instrument snapshot to both verify REST-derived symbols
        and discover symbols present only on WebSocket (not in REST markets).

        Args:
            client: Kraken exchange client.
            ws_symbols: Set of REST-derived WS symbols to verify.
            timeout_seconds: Maximum time to spend collecting.

        Returns:
            Tuple of (verified REST symbols, WS-only instrument dicts).
            Each WS-only dict has keys: symbol, base, quote.
        """
        verified: set[str] = set()
        ws_only: list[dict[str, str]] = []
        try:
            async with asyncio.timeout(timeout_seconds):
                async for raw_instrument in client.subscribe_instruments(raw=True):
                    ws_symbol = raw_instrument.get("symbol")
                    if not isinstance(ws_symbol, str):
                        continue
                    if ws_symbol in ws_symbols:
                        verified.add(ws_symbol)
                    else:
                        base = raw_instrument.get("base", "")
                        quote = raw_instrument.get("quote", "")
                        if base and quote:
                            ws_only.append({"symbol": ws_symbol, "base": base, "quote": quote})
        except TimeoutError:
            logger.info(
                "Snapshot collection timeout ({:.1f}s): verified {}/{} symbols, "
                "{} WS-only discovered",
                timeout_seconds,
                len(verified),
                len(ws_symbols),
                len(ws_only),
            )
        if len(verified) == len(ws_symbols) and ws_symbols:
            logger.info(
                "All {} symbols verified via WebSocket, {} WS-only discovered",
                len(ws_symbols),
                len(ws_only),
            )
        return verified, ws_only

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

    @staticmethod
    def _parse_btnl_wire_symbol(wire: str) -> tuple[str, str] | None:
        """Parse a Kraken-relayed Bitnomial spot wire symbol into (base, quote).

        Accepts strings of the form ``<BASE>/<QUOTE>:BTNL`` (the wire
        form Kraken emits on the WS ticker stream as a relay of
        Bitnomial's spot venue — ``BTNL`` is a registered Bitnomial
        trademark, confirmed against the public Bitnomial product
        catalog where ``BTCUSD``, ``ETHUSD``, ``LTCUSD``, ``SUIUSD``,
        ``ZECUSD``, ``AAVEUSD``, and ``PAXGUSD`` are all spot
        products). Returns ``None`` for any input that does not
        match — the conservative allow-list keeps unknown
        product-type suffixes on the warning-rate-limit path
        instead of silently registering them.

        Args:
            wire: Raw symbol string from a Kraken WS ticker frame.

        Returns:
            Tuple of ``(base, quote)`` both upper-cased, or ``None``
            if the symbol does not match the ``<BASE>/<QUOTE>:BTNL``
            shape.
        """
        if not wire.endswith(_BTNL_WIRE_SUFFIX):
            return None
        stem = wire[: -len(_BTNL_WIRE_SUFFIX)]
        if "/" not in stem:
            return None
        base, _, quote = stem.partition("/")
        if not base or not quote:
            return None
        return base.upper(), quote.upper()

    async def discover_btnl_symbols(self) -> dict[str, KrakenSymbolRecord]:
        """Discover Bitnomial spot symbols relayed via Kraken's WS feed.

        Subscribes to the live wildcard ticker stream for
        ``_BTNL_DISCOVERY_WINDOW_SECONDS`` seconds via
        :meth:`KrakenExchangeClient.collect_raw_ticker_symbols` and
        collects every distinct symbol whose wire form ends with
        ``:BTNL`` (a registered Bitnomial trademark). The Bitnomial
        product catalog enumerates these as SPOT products with their
        own venue order books, distinct from Kraken's primary spot
        market — measurement confirmed persistent non-zero offsets
        vs Kraken spot prices, consistent with two separate spot
        venues for the same underlying asset.

        BTNL ticker updates run 5-100× less frequently than Kraken's
        primary spot stream, so the discovery window must be sized
        long enough to observe each pair at least once. Conservative
        allow-list: only ``:BTNL`` suffix; any other colon-suffix is
        ignored so future product types do not land on this persist
        path by accident.

        Returns:
            Dict mapping native symbol (``BTC-USD-BTNL``) to its
            record dict suitable for ``_update_database``.
        """
        logger.info(
            "BTNL discovery: starting {:.0f}s wildcard ticker capture "
            "(updater will appear idle while subscribed)",
            _BTNL_DISCOVERY_WINDOW_SECONDS,
        )
        client = self._create_exchange_client()
        captured = await client.collect_raw_ticker_symbols(_BTNL_DISCOVERY_WINDOW_SECONDS)
        discoveries: dict[str, KrakenSymbolRecord] = {}
        for wire_symbol in captured:
            parsed = self._parse_btnl_wire_symbol(wire_symbol)
            if parsed is None:
                continue
            base, quote = parsed
            native = f"{base}-{quote}{_BTNL_NATIVE_SUFFIX}"
            discoveries[native] = KrakenSymbolRecord(
                native_symbol=native,
                base_currency=base,
                quote_currency=quote,
                asset_class="crypto",
                is_btnl="true",
                kraken_websocket_symbol=wire_symbol,
            )
        logger.info(
            "BTNL discovery: {} native symbols from {} captured frames",
            len(discoveries),
            len(captured),
        )
        return discoveries

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
        canonical_base = base[:-1].upper()
        return canonical_base, f"{base}/{quote_currency}", canonical_base, quote_currency

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
        self, kraken_rest_symbol: str, pair_info: _KrakenPairInfo
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
        self, kraken_rest_symbols: dict[str, _KrakenPairInfo]
    ) -> tuple[dict[str, KrakenSymbolRecord], set[str]]:
        """Build symbol data dicts from REST symbol data.

        Args:
            kraken_rest_symbols: Dict mapping REST symbol to pair info.

        Returns:
            Tuple of (mappings dict keyed by native_symbol, ws_symbols_to_verify set).
        """
        mappings: dict[str, KrakenSymbolRecord] = {}
        ws_symbols_to_verify: set[str] = set()
        for kraken_rest_symbol, pair_info in kraken_rest_symbols.items():
            resolved = self._resolve_pair(kraken_rest_symbol, pair_info)
            if resolved is None:
                continue
            native_symbol, ws_symbol, base_currency, quote_currency = resolved
            ws_symbols_to_verify.add(ws_symbol)
            record = KrakenSymbolRecord(
                native_symbol=native_symbol,
                kraken_websocket_symbol=ws_symbol,
                kraken_rest_symbol=kraken_rest_symbol,
                ccxt_symbol=pair_info.get("ccxt_symbol", ""),
                base_currency=base_currency,
                quote_currency=quote_currency,
                asset_class=pair_info.get("asset_class", "currency"),
                margin=pair_info.get("margin", "false"),
            )
            metadata = pair_info.get("metadata")
            if metadata is not None:
                record["metadata"] = metadata
            mappings[native_symbol] = record
        return mappings, ws_symbols_to_verify

    @staticmethod
    def _is_tokenized_base(base: str) -> bool:
        """Detect Kraken tokenized asset (xStock) from base currency name.

        Kraken xStock bases end with lowercase ``x`` and have mixed case
        (e.g., ``NVDAx``, ``BTGOx``), while standard crypto bases are
        all-uppercase (``BTC``, ``ETH``).

        Args:
            base: Base currency code from exchange.

        Returns:
            True if the base matches the tokenized asset naming convention.
        """
        return len(base) > 1 and base.endswith("x") and base != base.upper()

    @staticmethod
    def _build_ws_only_mapping(instrument: dict[str, str]) -> KrakenSymbolRecord:
        """Build a mapping dict for a WS-only instrument.

        WS-only instruments have no REST/CCXT representation and are
        marked with ``ws_only=true`` for downstream processing.
        Asset class is inferred from the base currency naming convention:
        mixed-case bases ending in lowercase ``x`` are tokenized assets.

        Args:
            instrument: Dict with keys: symbol, base, quote.

        Returns:
            Mapping dict compatible with ``_update_database``.
        """
        ws_symbol = instrument["symbol"]
        base = instrument["base"]
        quote = instrument["quote"]
        native_symbol = make_native_symbol(base, quote)
        asset_class = (
            "tokenized_asset" if KrakenSymbolUpdaterService._is_tokenized_base(base) else "crypto"
        )
        return KrakenSymbolRecord(
            native_symbol=native_symbol,
            kraken_websocket_symbol=ws_symbol,
            kraken_rest_symbol="",
            ccxt_symbol="",
            base_currency=base,
            quote_currency=quote,
            asset_class=asset_class,
            ws_only="true",
        )

    async def build_verified_mappings(self) -> tuple[dict[str, KrakenSymbolRecord], bool]:
        """Build verified symbol data from REST and WebSocket data.

        Loads symbols from REST, verifies via WebSocket, discovers WS-only
        symbols, and returns all symbol data dicts.

        Returns:
            Tuple of (mappings dict keyed by native_symbol, success boolean).
        """
        try:
            kraken_rest_symbols = await self.load_kraken_rest_symbols()
            logger.info(f"Loaded {len(kraken_rest_symbols)} symbols from REST API")
            mappings, ws_symbols_to_verify = self._build_mappings_from_rest(kraken_rest_symbols)
            verified_ws_symbols, ws_only_instruments = await self.verify_websocket_symbols(
                ws_symbols_to_verify,
            )
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
            ws_only_count = 0
            for instrument in ws_only_instruments:
                ws_only_mapping = self._build_ws_only_mapping(instrument)
                native = ws_only_mapping["native_symbol"]
                if native not in mappings:
                    mappings[native] = ws_only_mapping
                    ws_only_count += 1
            logger.info(
                f"Built {len(mappings)} mappings ({ws_only_count} WS-only), "
                f"verification: {len(verified_ws_symbols)}/{len(ws_symbols_to_verify)} "
                f"({verification_rate:.1%})"
            )
            return mappings, verification_success
        except Exception as e:
            logger.error(f"Error building verified mappings: {e}")
            return {}, False

    async def _fetch_symbols(self, client: KrakenExchangeClient) -> list[dict[str, Any]]:
        """Fetch and verify symbols from Kraken, then run BTNL discovery.

        Builds verified mappings from REST + WS instrument feed,
        then runs a best-effort wildcard ticker snapshot to discover
        Kraken Bitnomial perpetual (``:BTNL``) symbols that the
        REST/CCXT/instrument paths don't expose. BTNL discovery
        failure is non-fatal — the rest of the update proceeds.

        Args:
            client: Kraken exchange client (unused, mappings built internally).

        Returns:
            List of symbol data dicts including any discovered BTNL
            records flagged with ``is_btnl="true"``.

        Raises:
            RuntimeError: If WebSocket verification fails or no mappings.
        """
        mappings, verification_success = await self.build_verified_mappings()
        if not verification_success:
            raise RuntimeError("WebSocket verification failed - aborting update")
        if not mappings:
            raise RuntimeError("No mappings generated - aborting update")
        btnl_count = 0
        try:
            btnl_discoveries = await self.discover_btnl_symbols()
        except Exception as btnl_exc:
            logger.warning(
                "BTNL discovery failed (non-fatal, continuing without BTNL pairs): {}",
                btnl_exc,
            )
            btnl_discoveries = {}
        for native, btnl_mapping in btnl_discoveries.items():
            if native not in mappings:
                mappings[native] = btnl_mapping
                btnl_count += 1
        logger.info(f"Fetched and verified {len(mappings)} Kraken symbols ({btnl_count} BTNL)")
        return cast(list[dict[str, Any]], list(mappings.values()))

    def _persist_ws_only_symbol(
        self, session: Any, symbol_data: KrakenSymbolRecord, symbol_public_id: str, now: datetime
    ) -> tuple[int, int]:
        """Persist a WS-only symbol: one WS alias + market-data-only capability.

        Args:
            session: SQLAlchemy session.
            symbol_data: Symbol data dict with ws_only flag.
            symbol_public_id: Public ID of the symbol.
            now: Current UTC timestamp.

        Returns:
            Tuple of (created_count, updated_count) for alias operations.
        """
        created = 0
        updated = 0
        sid = self._tracker.session_id
        ws_symbol = symbol_data.get("kraken_websocket_symbol", "")
        if ws_symbol:
            result = self._upsert_alias(
                session,
                symbol_public_id,
                ExchangeEnum.KRAKEN,
                AliasChannelEnum.WS,
                ws_symbol,
                now,
                session_id=sid,
                sequence_id=self._tracker.next_sequence("aliases"),
            )
            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1
        self._upsert_capability(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            True,
            False,
            "kraken_updater",
            "WS-only, not in REST markets",
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
        )
        instrument_public_id = self._ensure_instrument_identity(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("instruments"),
        )
        self._revise_instrument_spec(
            session,
            instrument_public_id,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("specs"),
            instrument_kind="spot",
            funding_type=None,
            funding_frequency_hours=None,
            rollover_rate_long=None,
            rollover_rate_short=None,
        )
        self._deactivate_spot_rollover_rates(session, instrument_public_id, now)
        return created, updated

    def _persist_btnl_symbol(
        self, session: Any, symbol_data: KrakenSymbolRecord, symbol_public_id: str, now: datetime
    ) -> tuple[int, int]:
        """Persist a Kraken WS relay of a Bitnomial spot symbol.

        Routes a BTNL discovery through one WS alias (``BTC/USD:BTNL``
        wire form), a market-data-only capability with
        ``can_trade=False`` and the BTNL gate reason, and an
        InstrumentSpec carrying ``instrument_kind="spot"`` plus
        explicit funding-field clears (no spot-margin rollover, no
        perpetual funding accrual — Bitnomial's catalog confirms BTNL
        symbols on Kraken WS correspond to its SPOT venue products,
        not perpetuals; their only perpetual ``PBUCZ50`` uses a
        different ``base_symbol`` of ``BUC`` and would surface as a
        separate native symbol if ever streamed). Any stale
        spot-margin rollover rows attached to this instrument are
        deactivated for safety.

        Args:
            session: SQLAlchemy session.
            symbol_data: Symbol data dict with ``is_btnl == "true"``.
            symbol_public_id: Public ID of the symbol.
            now: Current UTC timestamp.

        Returns:
            Tuple of ``(created_count, updated_count)`` for the WS
            alias upsert.
        """
        created = 0
        updated = 0
        sid = self._tracker.session_id
        ws_symbol = symbol_data.get("kraken_websocket_symbol", "")
        if ws_symbol:
            result = self._upsert_alias(
                session,
                symbol_public_id,
                ExchangeEnum.KRAKEN,
                AliasChannelEnum.WS,
                ws_symbol,
                now,
                session_id=sid,
                sequence_id=self._tracker.next_sequence("aliases"),
            )
            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1
        self._upsert_capability(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            True,
            False,
            "kraken_updater",
            _BTNL_CAPABILITY_REASON,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
        )
        instrument_public_id = self._ensure_instrument_identity(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("instruments"),
        )
        self._revise_instrument_spec(
            session,
            instrument_public_id,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("specs"),
            instrument_kind="spot",
            funding_type=None,
            funding_frequency_hours=None,
            rollover_rate_long=None,
            rollover_rate_short=None,
        )
        self._deactivate_spot_rollover_rates(session, instrument_public_id, now)
        return created, updated

    def _persist_rest_symbol(
        self, session: Any, symbol_data: KrakenSymbolRecord, symbol_public_id: str, now: datetime
    ) -> tuple[int, int]:
        """Persist a REST symbol: ws/rest/ccxt aliases + full capability.

        Args:
            session: SQLAlchemy session.
            symbol_data: Symbol data dict from REST resolution.
            symbol_public_id: Public ID of the symbol.
            now: Current UTC timestamp.

        Returns:
            Tuple of (created_count, updated_count) for alias operations.
        """
        created = 0
        updated = 0
        sid = self._tracker.session_id
        alias_mappings: tuple[tuple[str, AliasChannelEnum, str], ...] = (
            (ExchangeEnum.KRAKEN, AliasChannelEnum.WS, "kraken_websocket_symbol"),
            (ExchangeEnum.KRAKEN, AliasChannelEnum.REST, "kraken_rest_symbol"),
            (ExchangeEnum.KRAKEN, AliasChannelEnum.CCXT, "ccxt_symbol"),
        )
        for exchange, channel, key in alias_mappings:
            exchange_symbol = symbol_data.get(key)
            if not isinstance(exchange_symbol, str) or not exchange_symbol:
                continue
            result = self._upsert_alias(
                session,
                symbol_public_id,
                exchange,
                channel,
                exchange_symbol,
                now,
                session_id=sid,
                sequence_id=self._tracker.next_sequence("aliases"),
            )
            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1
        self._upsert_capability(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            True,
            True,
            "kraken_updater",
            None,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
        )
        instrument_public_id = self._ensure_instrument_identity(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("instruments"),
        )
        is_margin = symbol_data.get("margin") == "true"
        self._revise_instrument_spec(
            session,
            instrument_public_id,
            now,
            session_id=sid,
            sequence_id=self._tracker.next_sequence("specs"),
            instrument_kind="spot",
            funding_type="spot_margin_rollover" if is_margin else None,
            funding_frequency_hours=4 if is_margin else None,
            rollover_rate_long=_SPOT_ROLLOVER_RATE_LONG if is_margin else None,
            rollover_rate_short=_SPOT_ROLLOVER_RATE_SHORT if is_margin else None,
            metadata=symbol_data.get("metadata", PRESERVE_EXISTING),
        )
        if is_margin:
            self._seed_spot_rollover_rates(
                session, instrument_public_id, symbol_data["quote_currency"], now
            )
        else:
            self._deactivate_spot_rollover_rates(session, instrument_public_id, now)
        return created, updated

    @staticmethod
    def _deactivate_spot_rollover_rates(
        session: Any,
        instrument_public_id: str,
        now: datetime,
    ) -> int:
        """Close active FundingRate rows for a pair that is no longer marginable.

        Sets ``known_to = now`` on any active spot_margin_rollover rows
        for this instrument. Returns the count of deactivated rows (0
        when the pair was never marginable).

        Args:
            session: SQLAlchemy sync session.
            instrument_public_id: Public ID of the instrument.
            now: Current UTC timestamp used to close active rows.

        Returns:
            Number of rows deactivated.
        """
        stmt = select(FundingRate).where(
            FundingRate.instrument_public_id == instrument_public_id,
            FundingRate.exchange == ExchangeEnum.KRAKEN,
            FundingRate.rate_type == "spot_margin_rollover",
            FundingRate.timestamp <= now,
            FundingRate.known_to > now,
        )
        active_rows = session.execute(stmt).scalars().all()
        for row in active_rows:
            row.known_to = now
        if active_rows:
            session.flush()
            logger.debug(
                "Deactivated {} spot rollover rate(s) for {}",
                len(active_rows),
                instrument_public_id,
            )
        return len(active_rows)

    def _seed_spot_rollover_rates(
        self,
        session: Any,
        instrument_public_id: str,
        quote_currency: str,
        now: datetime,
    ) -> None:
        """Seed FundingRate rows for a marginable Kraken Spot pair.

        Inserts one row per direction (long/short) with source
        ``exchange_docs`` and a stable ``effective_from`` epoch so
        re-runs are idempotent (the partial unique index catches
        duplicates, which are silently swallowed via SAVEPOINT).

        Args:
            session: SQLAlchemy sync session.
            instrument_public_id: Public ID of the instrument.
            quote_currency: Quote asset for the notional_asset field.
            now: Current UTC timestamp for the SCD2 temporal envelope.
        """
        sid = self._tracker.session_id
        for direction, rate in (
            ("long", _SPOT_ROLLOVER_RATE_LONG),
            ("short", _SPOT_ROLLOVER_RATE_SHORT),
        ):
            obj = FundingRate(
                instrument_public_id=instrument_public_id,
                exchange=ExchangeEnum.KRAKEN,
                rate_type="spot_margin_rollover",
                direction=direction,
                rate=rate,
                notional_asset=quote_currency,
                effective_from=SPOT_ROLLOVER_RATES_EPOCH,
                source="exchange_docs",
                session_id=sid,
                sequence_id=self._tracker.next_sequence(_SEQ_KEY_FUNDING),
                timestamp=now,
            )
            try:
                with session.begin_nested():
                    session.add(obj)
                    session.flush()
            except IntegrityError:
                logger.debug(
                    "Spot rollover rate already seeded for {} direction={}",
                    instrument_public_id,
                    direction,
                )

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist symbol catalog and alias rows to the database.

        Each REST symbol produces one symbol row, up to three alias rows
        (kraken ws, kraken rest, kraken ccxt), and a capability row with
        ``can_trade=True``.

        WS-only symbols (``ws_only=true``) produce one symbol row, one WS
        alias, and a capability row with ``can_trade=False`` and a reason
        explaining they are not available via REST.

        Args:
            symbols: List of symbol data dicts from Kraken.
        """
        assert self.repository is not None, "Repository not initialized"
        records = cast(list[KrakenSymbolRecord], symbols)
        created_count = 0
        updated_count = 0
        ws_only_count = 0
        btnl_count = 0
        try:
            with self.repository.get_session() as session:
                processed_symbol_public_ids: set[str] = set()
                now = datetime.now(UTC)
                for symbol_data in records:
                    native_symbol = symbol_data["native_symbol"]
                    asset_type: AssetTypeEnum = (
                        AssetTypeEnum.EQUITY
                        if symbol_data.get("asset_class") == "tokenized_asset"
                        else AssetTypeEnum.CRYPTO
                    )
                    symbol_public_id = self._upsert_symbol(
                        session,
                        native_symbol,
                        symbol_data["base_currency"],
                        symbol_data["quote_currency"],
                        asset_type,
                        now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence("symbols"),
                    )
                    processed_symbol_public_ids.add(symbol_public_id)
                    is_btnl = symbol_data.get("is_btnl") == "true"
                    is_ws_only = symbol_data.get("ws_only") == "true"
                    if is_btnl:
                        c, u = self._persist_btnl_symbol(
                            session, symbol_data, symbol_public_id, now
                        )
                        btnl_count += 1
                    elif is_ws_only:
                        c, u = self._persist_ws_only_symbol(
                            session, symbol_data, symbol_public_id, now
                        )
                        ws_only_count += 1
                    else:
                        c, u = self._persist_rest_symbol(
                            session, symbol_data, symbol_public_id, now
                        )
                    created_count += c
                    updated_count += u
                deactivated = self._reconcile_capabilities(
                    session,
                    ExchangeEnum.KRAKEN,
                    processed_symbol_public_ids,
                    "kraken_updater",
                    now,
                    session_id=self._tracker.session_id,
                    next_sequence_fn=lambda: self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
                )
                closed_aliases = self._reconcile_aliases(session, ExchangeEnum.KRAKEN, now)
                session.commit()
                logger.info(
                    f"Kraken update complete: {created_count} created, "
                    f"{updated_count} updated, {ws_only_count} WS-only, "
                    f"{btnl_count} BTNL, {deactivated} deactivated, "
                    f"{closed_aliases} aliases closed (total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            raise
