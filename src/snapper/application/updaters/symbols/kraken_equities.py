"""Kraken Equities (FCM Futures) symbol updater module.

This module fetches FCM futures contract metadata from the Kraken
internal REST API and persists symbol mappings, aliases, and exchange
capabilities to the database.

Native symbol format: ``CLM6-NYMEX`` (contract code + dash + exchange).
Exchange WS format: ``CLM6.NYMEX`` (dot-separated).

Category-to-asset-type mapping:
    - Energies, Metals, Grains → COMMODITY
    - Indices → INDEX
    - Yields (treasury bonds) → YIELD
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
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesInstrumentSchema

_SEQ_KEY_CAPABILITIES = "capabilities"

_COMMODITY_CATEGORIES = frozenset({"Energies", "Metals", "Grains"})
_YIELD_CATEGORIES = frozenset({"Yields"})


@register_process(
    "kraken_equities_symbol_updater",
    method="start",
    description="Kraken Equities (FCM Futures) symbol updater",
    priority=15,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "kraken_equities"),
    parameters_model=SymbolUpdaterParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenEquitiesSymbolUpdaterService(SymbolUpdaterService[KrakenEquitiesExchangeClient]):
    """Symbol updater for Kraken Equities (FCM commodity/index futures).

    Fetches contract catalog via internal REST API and constructs
    native symbols from the ``symbol`` and ``exchange`` fields.
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

    def _create_exchange_client(self) -> KrakenEquitiesExchangeClient:
        """Create exchange client for fetching instruments.

        Returns:
            KrakenEquitiesExchangeClient configured for REST access.
        """
        return KrakenEquitiesExchangeClient()

    def _get_setting_key(self) -> str:
        """Return the database setting key for last update timestamp.

        Returns:
            Setting key string for Kraken Equities symbol updater.
        """
        return "kraken_equities_symbols_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist Kraken Equities symbol catalog to the database.

        Each tradable contract produces one symbol row, one WS alias,
        and a capability row with ``can_trade=False`` and
        ``can_market_data=True``.

        Args:
            symbols: List of raw contract dicts from REST API.
        """
        if self.repository is None:
            raise RuntimeError("Repository not initialized")
        created_count = 0
        skipped_count = 0
        try:
            with self.repository.get_session() as session:
                processed_symbol_public_ids: set[str] = set()
                now = datetime.now(UTC)
                for raw_contract in symbols:
                    try:
                        schema = KrakenEquitiesInstrumentSchema.model_validate(raw_contract)
                    except Exception as exc:
                        logger.warning(
                            f"Skipping invalid contract: {raw_contract.get('symbol', '?')}: {exc}"
                        )
                        skipped_count += 1
                        continue

                    native = _build_native_symbol(schema)
                    if native is None:
                        logger.debug(f"Skipping unmappable contract: {schema.symbol}")
                        skipped_count += 1
                        continue

                    asset_type = _classify_asset_type(schema)
                    base = schema.contract_name or schema.symbol.split(".")[0]
                    quote = schema.quote or "USD"

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
                        ExchangeEnum.KRAKEN_EQUITIES,
                        AliasChannelEnum.WS,
                        schema.symbol,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("aliases"),
                    )

                    self._upsert_capability(
                        session,
                        symbol_public_id,
                        ExchangeEnum.KRAKEN_EQUITIES,
                        True,
                        False,
                        "kraken_equities_updater",
                        None,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
                    )

                    instrument_public_id = self._ensure_instrument_identity(
                        session,
                        symbol_public_id,
                        ExchangeEnum.KRAKEN_EQUITIES,
                        now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence("instruments"),
                    )

                    expiry_dt = (
                        datetime.fromtimestamp(schema.maturity, tz=UTC)
                        if schema.maturity is not None
                        else None
                    )
                    self._revise_instrument_spec(
                        session,
                        instrument_public_id,
                        now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence("specs"),
                        expiry_at=expiry_dt,
                        instrument_kind="future",
                    )
                    created_count += 1

                deactivated = self._reconcile_capabilities(
                    session,
                    ExchangeEnum.KRAKEN_EQUITIES,
                    processed_symbol_public_ids,
                    "kraken_equities_updater",
                    now,
                    session_id=self._tracker.session_id,
                    next_sequence_fn=lambda: self._tracker.next_sequence(_SEQ_KEY_CAPABILITIES),
                )
                session.commit()
                logger.info(
                    f"Kraken Equities update complete: {created_count} processed, "
                    f"{skipped_count} skipped, {deactivated} deactivated "
                    f"(total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error updating Kraken Equities database: {e}")
            raise


def _build_native_symbol(schema: KrakenEquitiesInstrumentSchema) -> str | None:
    """Build native symbol from contract metadata.

    Converts exchange dot format to native dash format.
    Example: ``CLM6.NYMEX`` → ``CLM6-NYMEX``

    Args:
        schema: Validated contract schema.

    Returns:
        Native symbol string, or None if unmappable.
    """
    exchange_symbol = schema.symbol
    if "." not in exchange_symbol:
        return None
    return exchange_symbol.replace(".", "-")


def _classify_asset_type(schema: KrakenEquitiesInstrumentSchema) -> AssetTypeEnum:
    """Classify the asset type from contract category.

    Args:
        schema: Validated contract schema.

    Returns:
        AssetTypeEnum based on product category.
    """
    category = schema.category
    if category in _COMMODITY_CATEGORIES:
        return AssetTypeEnum.COMMODITY
    if category in _YIELD_CATEGORIES:
        return AssetTypeEnum.YIELD
    return AssetTypeEnum.INDEX
