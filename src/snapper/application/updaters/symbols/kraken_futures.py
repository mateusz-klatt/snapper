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

    def __init__(self, update_threshold_hours: int = 6, force: bool = False) -> None:
        """Initialize the instance.

        Args:
            update_threshold_hours: Hours between updates. Defaults to 6.
            force: If True, bypass the update threshold check.
        """
        super().__init__(update_threshold_hours=update_threshold_hours, force=force)

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

        Each tradeable instrument produces one symbol row, one WS alias
        one CCXT alias (if available), and a capability row with
        ``can_trade=False`` and
        ``can_market_data=True``.
        Non-tradeable instruments are skipped.

        Args:
            symbols: List of raw instrument dicts from Kraken Futures API.
        """
        if self.repository is None:
            raise RuntimeError("Repository not initialized")
        created_count = 0
        skipped_count = 0
        try:
            with self.repository.get_session() as session:
                processed_symbol_public_ids: set[str] = set()
                now = datetime.now(UTC)
                for raw_instrument in symbols:
                    schema = self._validate_instrument_schema(raw_instrument)
                    if schema is None:
                        skipped_count += 1
                        continue

                    symbol_public_id = self._persist_instrument_schema(session, schema, now)
                    if symbol_public_id is None:
                        skipped_count += 1
                        continue

                    processed_symbol_public_ids.add(symbol_public_id)
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
                closed_aliases = self._reconcile_aliases(session, ExchangeEnum.KRAKEN_FUTURES, now)
                session.commit()
                logger.info(
                    f"Kraken Futures update complete: {created_count} processed, "
                    f"{skipped_count} skipped, {deactivated} deactivated, "
                    f"{closed_aliases} aliases closed (total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error updating Kraken Futures database: {e}")
            raise

    def _validate_instrument_schema(
        self,
        raw_instrument: dict[str, Any],
    ) -> KrakenFuturesInstrumentSchema | None:
        """Validate one raw Kraken Futures instrument payload."""
        try:
            return KrakenFuturesInstrumentSchema.model_validate(raw_instrument)
        except Exception as exc:
            logger.warning(
                f"Skipping invalid instrument: {raw_instrument.get('symbol', '?')}: {exc}"
            )
            return None

    def _persist_instrument_schema(
        self,
        session: Any,
        schema: KrakenFuturesInstrumentSchema,
        now: datetime,
    ) -> str | None:
        """Persist one validated instrument and return its symbol public ID."""
        symbol_metadata = self._resolve_symbol_metadata(schema)
        if symbol_metadata is None:
            logger.debug(f"Skipping unmappable instrument: {schema.symbol}")
            return None

        native, base, quote, asset_type = symbol_metadata
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
        self._upsert_symbol_aliases(session, symbol_public_id, schema, now)
        self._upsert_capability(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN_FUTURES,
            True,
            False,
            "kraken_futures_updater",
            None,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
        )
        instrument_public_id = self._ensure_instrument_identity(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN_FUTURES,
            now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
        )
        self._revise_schema_instrument_spec(session, instrument_public_id, schema, now)
        return symbol_public_id

    @staticmethod
    def _resolve_symbol_metadata(
        schema: KrakenFuturesInstrumentSchema,
    ) -> tuple[str, str, str, AssetTypeEnum] | None:
        """Resolve the native symbol and normalized identity fields."""
        native = _build_native_symbol(schema)
        if native is None:
            return None
        return (
            native,
            _normalize_currency(schema.base or ""),
            _normalize_currency(schema.quote or ""),
            _classify_asset_type(schema),
        )

    def _upsert_symbol_aliases(
        self,
        session: Any,
        symbol_public_id: str,
        schema: KrakenFuturesInstrumentSchema,
        now: datetime,
    ) -> None:
        """Persist the WS alias and optional CCXT alias for one symbol."""
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
        ccxt_symbol = _build_ccxt_symbol(schema)
        if ccxt_symbol is None:
            return
        self._upsert_alias(
            session,
            symbol_public_id,
            ExchangeEnum.KRAKEN_FUTURES,
            AliasChannelEnum.CCXT,
            ccxt_symbol,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("aliases"),
        )

    def _revise_schema_instrument_spec(
        self,
        session: Any,
        instrument_public_id: str,
        schema: KrakenFuturesInstrumentSchema,
        now: datetime,
    ) -> None:
        """Persist instrument spec fields derived from the Kraken catalog."""
        expiry_at, kind, funding_type, funding_frequency_hours, max_funding_rate = (
            self._instrument_spec_fields(schema)
        )
        self._revise_instrument_spec(
            session,
            instrument_public_id,
            now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("specs"),
            expiry_at=expiry_at,
            instrument_kind=kind,
            funding_type=funding_type,
            funding_frequency_hours=funding_frequency_hours,
            max_funding_rate=max_funding_rate,
        )

    @staticmethod
    def _instrument_spec_fields(
        schema: KrakenFuturesInstrumentSchema,
    ) -> tuple[datetime | None, str, str | None, int | None, float | None]:
        """Build the instrument spec fields derived from exchange metadata."""
        expiry_at = _parse_expiry_datetime(schema)
        if schema.last_trading_time:
            return expiry_at, "future", None, None, None
        return expiry_at, "perpetual", "perpetual_funding", 1, 0.0025


def _parse_expiry_datetime(schema: KrakenFuturesInstrumentSchema) -> datetime | None:
    """Parse expiry as full datetime from last_trading_time field.

    Args:
        schema: Validated instrument schema.

    Returns:
        Expiry datetime in UTC, or None for perpetuals.
    """
    ltt = schema.last_trading_time
    if not ltt:
        return None
    try:
        return datetime.fromisoformat(ltt.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(f"Failed to parse last_trading_time '{ltt}' for {schema.symbol}")
        return None


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

    Reference rates (rr_*) and indices (in_*) are classified as INDEX
    regardless of the tradfi flag. TradFi products (CME contracts) are
    also INDEX. Everything else is CRYPTO.

    Args:
        schema: Validated instrument schema.

    Returns:
        AssetTypeEnum based on product classification.
    """
    if schema.tradfi:
        return AssetTypeEnum.INDEX
    symbol_lower = schema.symbol.lower()
    if symbol_lower.startswith(_RR_PREFIX) or symbol_lower.startswith(_IN_PREFIX):
        return AssetTypeEnum.INDEX
    return AssetTypeEnum.CRYPTO


def _build_ccxt_symbol(schema: KrakenFuturesInstrumentSchema) -> str | None:
    """Build CCXT symbol from instrument metadata.

    Only perpetual futures get a CCXT alias because dated futures
    share the same ``BASE/QUOTE:SETTLE`` format and would collide
    on the unique alias constraint.

    Linear perpetual: BASE/QUOTE:QUOTE (e.g., BTC/USD:USD).
    Inverse perpetual: BASE/QUOTE:BASE (e.g., BTC/USD:BTC).

    Args:
        schema: Validated instrument schema.

    Returns:
        CCXT symbol string, or None for dated futures and non-tradeable products.
    """
    if not schema.base or not schema.quote:
        return None
    if schema.last_trading_time:
        return None
    symbol_lower = schema.symbol.lower()
    if symbol_lower.startswith(_RR_PREFIX) or symbol_lower.startswith(_IN_PREFIX):
        return None
    base = _normalize_currency(schema.base)
    quote = _normalize_currency(schema.quote)
    settle = base if schema.type in _INVERSE_TYPES else quote
    return f"{base}/{quote}:{settle}"
