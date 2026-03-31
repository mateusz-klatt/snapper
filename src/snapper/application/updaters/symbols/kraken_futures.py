"""Kraken Futures symbol updater module.

This module fetches product metadata from the Kraken Futures REST API
and persists symbol mappings, aliases, and exchange capabilities to the
database. Uses metadata-driven mapping (not regex parsing) to construct
native symbols from the ``base``, ``quote``, and ``type`` fields returned
by ``Market.get_instruments()``.

Native symbol format:
    - Perpetual linear:  ``BTC-USD-PERP``
    - Perpetual inverse: ``BTC-USD-PERP-INV``
    - Fixed-maturity linear:  ``BTC-USD-250627``
    - Fixed-maturity inverse: ``BTC-USD-250627-INV``
    - Reference rate:    ``BTC-USD-RR``
    - Index:             ``BTC-USD-IDX``
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import SymbolUpdaterParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.config.settings import AppSettings
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema

_SEQ_KEY_CAPABILITIES = "capabilities"

_INVERSE_TYPES = frozenset({"futures_inverse"})
_RR_PREFIX = "rr_"
_IN_PREFIX = "in_"

_XBT_TO_BTC = {"XBT": "BTC"}


@register_process(
    "kraken_futures_symbol_updater",
    method="start",
    description="Kraken Futures symbol updater",
    priority=15,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "kraken_futures"),
    parameters_model=SymbolUpdaterParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenFuturesSymbolUpdaterService(SymbolUpdaterService[KrakenFuturesExchangeClient]):
    """Symbol updater for Kraken Futures exchange.

    Fetches product catalog via ``Market.get_instruments()`` and CCXT
    ``load_markets()``. Constructs native symbols from metadata fields
    (base, quote, type) rather than parsing exchange symbol strings.

    Registered as one-shot task process with 6-hour update threshold.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters.

        Args:
            settings: Application settings.

        Returns:
            Dictionary with update threshold and force flag.
        """
        return {"update_threshold_hours": 6, "force": False}

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Create exchange client for fetching instruments.

        Returns:
            KrakenFuturesExchangeClient configured for REST access.
        """
        return KrakenFuturesExchangeClient()

    def _get_setting_key(self) -> str:
        """Return the database setting key for last update timestamp.

        Returns:
            Setting key string for Kraken Futures symbol updater.
        """
        return "kraken_futures_symbols_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist Kraken Futures symbol catalog to the database.

        Each tradeable instrument produces one symbol row, one WS alias,
        one CCXT alias (if available), and a capability row with
        ``can_trade=False`` (Phase 1 is market data only) and
        ``can_market_data=True``.

        Non-tradeable instruments are skipped.

        Args:
            symbols: List of raw instrument dicts from Kraken Futures API.
        """
        assert self.repository is not None, "Repository not initialized"
        created_count = 0
        skipped_count = 0
        try:
            with self.repository.get_session() as session:
                processed_symbol_public_ids: set[str] = set()
                now = datetime.now(UTC)
                for raw_instrument in symbols:
                    try:
                        schema = KrakenFuturesInstrumentSchema.model_validate(raw_instrument)
                    except Exception as exc:
                        logger.warning(
                            f"Skipping invalid instrument: {raw_instrument.get('symbol', '?')}: {exc}"
                        )
                        skipped_count += 1
                        continue

                    native = _build_native_symbol(schema)
                    if native is None:
                        logger.debug(f"Skipping unmappable instrument: {schema.symbol}")
                        skipped_count += 1
                        continue

                    base = _normalize_currency(schema.base or "")
                    quote = _normalize_currency(schema.quote or "")
                    asset_type = _classify_asset_type(schema)

                    symbol_public_id = self._upsert_symbol(
                        session,
                        native,
                        base,
                        quote,
                        asset_type,
                        now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence("symbols"),
                    )
                    processed_symbol_public_ids.add(symbol_public_id)

                    self._upsert_alias(
                        session,
                        symbol_public_id,
                        ExchangeEnum.KRAKEN_FUTURES,
                        AliasChannelEnum.WS,
                        schema.symbol,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("aliases"),
                    )

                    self._upsert_capability(
                        session,
                        symbol_public_id,
                        ExchangeEnum.KRAKEN_FUTURES,
                        schema.tradeable,
                        False,
                        "kraken_futures_updater",
                        None,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
                    )

                    self._ensure_instrument_identity(
                        session,
                        symbol_public_id,
                        ExchangeEnum.KRAKEN_FUTURES,
                        now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence("instruments"),
                    )
                    created_count += 1

                deactivated = self._reconcile_capabilities(
                    session,
                    ExchangeEnum.KRAKEN_FUTURES,
                    processed_symbol_public_ids,
                    "kraken_futures_updater",
                    now,
                    session_id=self._tracker.session_id,
                    next_sequence_fn=lambda: self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
                )
                session.commit()
                logger.info(
                    f"Kraken Futures update complete: {created_count} processed, "
                    f"{skipped_count} skipped, {deactivated} deactivated "
                    f"(total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error updating Kraken Futures database: {e}")
            raise


def _normalize_currency(raw: str) -> str:
    """Normalize currency code (e.g., XBT -> BTC).

    Args:
        raw: Raw currency code from exchange metadata.

    Returns:
        Normalized uppercase currency code.
    """
    upper = raw.upper()
    return _XBT_TO_BTC.get(upper, upper)


def _build_native_symbol(schema: KrakenFuturesInstrumentSchema) -> str | None:
    """Build native symbol from instrument metadata.

    Uses the ``base``, ``quote``, ``type``, and ``symbol`` fields to
    construct a Snapper-native symbol. Returns None if metadata is
    insufficient for mapping.

    Args:
        schema: Validated instrument schema.

    Returns:
        Native symbol string, or None if unmappable.
    """
    if not schema.base or not schema.quote:
        return None

    base = _normalize_currency(schema.base)
    quote = _normalize_currency(schema.quote)
    product_type = schema.type

    symbol_lower = schema.symbol.lower()
    if symbol_lower.startswith(_RR_PREFIX):
        return f"{base}-{quote}-RR"
    if symbol_lower.startswith(_IN_PREFIX):
        return f"{base}-{quote}-IDX"

    is_inverse = product_type in _INVERSE_TYPES
    is_perpetual = not schema.last_trading_time

    if is_perpetual:
        suffix = "PERP-INV" if is_inverse else "PERP"
        return f"{base}-{quote}-{suffix}"

    expiry = _extract_expiry(schema)
    if expiry:
        suffix = f"{expiry}-INV" if is_inverse else expiry
        return f"{base}-{quote}-{suffix}"

    return None


def _extract_expiry(schema: KrakenFuturesInstrumentSchema) -> str | None:
    """Extract expiry date string from instrument metadata.

    Uses ``last_trading_time`` field (ISO 8601) to derive a ``YYMMDD``
    expiry suffix.

    Args:
        schema: Validated instrument schema.

    Returns:
        Expiry string in ``YYMMDD`` format, or None if not available.
    """
    ltt = schema.last_trading_time
    if not ltt:
        return None
    try:
        dt = datetime.fromisoformat(ltt.replace("Z", "+00:00"))
        return dt.strftime("%y%m%d")
    except (ValueError, AttributeError):
        return None


def _classify_asset_type(schema: KrakenFuturesInstrumentSchema) -> AssetTypeEnum:
    """Classify the asset type from instrument metadata.

    Args:
        schema: Validated instrument schema.

    Returns:
        AssetTypeEnum based on product classification.
    """
    if schema.tradfi:
        return AssetTypeEnum.INDEX
    return AssetTypeEnum.CRYPTO
