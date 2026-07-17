"""Walutomat symbol updater service.

Fetches and persists FX pair symbols from Walutomat REST API.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

from loguru import logger

import snapper.application.portfolio.walutomat_precision_artifact as walutomat_artifact
from snapper.application.portfolio.spot_precision_evidence import SpotInstrumentPrecisionObservation
from snapper.application.portfolio.spot_precision_evidence import (
    derive_walutomat_precision_from_instruments,
)
from snapper.application.process_manager.process_parameters import SymbolUpdaterParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.application.updaters.symbols.types import WalutomatSymbolRecord
from snapper.config.settings import AppSettings
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository_types import SpotAssetPrecisionEvidenceUpsertRow
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient


def _spot_metadata() -> InstrumentMetadataInput:
    """Build Walutomat spot metadata from its documented FX conventions.

    The public ``marketBrief`` pair feed establishes only that a pair is
    currently quoted. Order precision comes from the checked-in reviewed
    artifact, whose content digest and review time remain unchanged across
    updater runs. The configured adapter submits volume in the native base
    asset. The API exposes no stable per-pair minimum or maximum order size, so
    those fields remain explicitly uncertified.

    Returns:
        Atomic venue- and rules-evidenced spot metadata.
    """
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    return InstrumentMetadataInput(
        tick_size=10.0**-artifact.limit_price_max_decimals,
        lot_size=10.0**-artifact.market_order_volume_decimals,
        min_order_size=None,
        max_order_size=None,
        cost_decimals=artifact.cost_decimals,
        qty_decimals=artifact.market_order_volume_decimals,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active",
        contract_size=None,
        quantity_unit="base_asset",
        spec_source=walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE,
        spec_version=walutomat_artifact.walutomat_precision_artifact_version(artifact),
        spec_observed_at=artifact.reviewed_at,
        unit_certified=False,
    )


@register_process(
    "walutomat_symbol_updater",
    method="start",
    description="Walutomat symbol updater",
    priority=16,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "walutomat"),
    parameters_model=SymbolUpdaterParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class WalutomatSymbolUpdaterService(SymbolUpdaterService[WalutomatExchangeClient]):
    """Service for updating Walutomat symbol mappings from REST API.

    Documentary precision stays certified only while its exact reviewed
    artifact remains current. The weekly catalog cadence therefore does not
    fabricate new documentary observation times.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for the Walutomat updater service.

        The weekly cadence refreshes pair availability without re-dating the
        independently reviewed precision artifact.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with update_threshold_hours and force parameters.
        """
        return {
            "update_threshold_hours": 168,
            "force": False,
        }

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        """Create a Walutomat exchange client instance.

        Returns:
            Configured WalutomatExchangeClient with default polling interval and timeout.
        """
        return WalutomatExchangeClient(polling_interval=10.0, timeout=5.0)

    def _get_setting_key(self) -> str:
        """Get the settings key for tracking last update timestamp.

        Returns:
            Settings key string for Walutomat symbol updates.
        """
        return "walutomat_symbols_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist symbol catalog and alias rows to the database.

        Args:
            symbols: List of symbol dictionaries containing symbol, walutomat_rest_symbol,
                native_symbol, base, and quote keys.
        """
        assert self.repository is not None, "Repository not initialized"
        records = cast(list[WalutomatSymbolRecord], symbols)
        created_count = 0
        updated_count = 0
        with self.repository.get_session() as session:
            processed_symbol_public_ids: set[str] = set()
            now = datetime.now(UTC)
            sid = self._tracker.session_id
            metadata = _spot_metadata()
            precision_observations = [
                SpotInstrumentPrecisionObservation(
                    base_asset=instrument["base"],
                    quote_asset=instrument["quote"],
                    qty_decimals=metadata.qty_decimals,
                    cost_decimals=metadata.cost_decimals,
                    source=metadata.spec_source,
                    version=metadata.spec_version,
                    observed_at=metadata.spec_observed_at,
                )
                for instrument in records
            ]
            for instrument in records:
                native_symbol = instrument["native_symbol"]
                symbol_public_id = self._upsert_symbol(
                    session,
                    native_symbol,
                    instrument["base"],
                    instrument["quote"],
                    AssetTypeEnum.FOREX,
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("symbols"),
                )
                processed_symbol_public_ids.add(symbol_public_id)
                ws_result = self._upsert_alias(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    AliasChannelEnum.WS,
                    instrument["symbol"],
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("aliases"),
                )
                if ws_result == "created":
                    created_count += 1
                elif ws_result == "updated":
                    updated_count += 1
                rest_result = self._upsert_alias(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    AliasChannelEnum.REST,
                    instrument["walutomat_rest_symbol"],
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("aliases"),
                )
                if rest_result == "created":
                    created_count += 1
                elif rest_result == "updated":
                    updated_count += 1
                self._upsert_capability(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    True,
                    True,
                    "walutomat_updater",
                    None,
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("capabilities"),
                )
                instrument_public_id = self._ensure_instrument_identity(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
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
                    metadata=metadata,
                )
            for candidate in derive_walutomat_precision_from_instruments(
                precision_observations,
                now,
            ):
                evidence_row = SpotAssetPrecisionEvidenceUpsertRow(
                    exchange=candidate.exchange,
                    asset=candidate.asset,
                    balance_decimals=candidate.balance_decimals,
                    balance_source=None,
                    balance_version=None,
                    balance_observed_at=None,
                    fee_decimals=candidate.fee_decimals,
                    fee_source=candidate.source,
                    fee_version=candidate.version,
                    fee_observed_at=candidate.observed_at,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("spot_asset_precision"),
                    timestamp=now,
                )
                self.repository.upsert_spot_asset_precision_evidence_sync(
                    session,
                    evidence_row,
                )
            deactivated = self._reconcile_capabilities(
                session,
                ExchangeEnum.WALUTOMAT,
                processed_symbol_public_ids,
                "walutomat_updater",
                now,
                session_id=self._tracker.session_id,
                next_sequence_fn=lambda: self._tracker.next_sequence("capabilities"),
            )
            closed_aliases = self._reconcile_aliases(session, ExchangeEnum.WALUTOMAT, now)
            session.commit()
        logger.info(
            f"Walutomat update complete: {created_count} created, "
            f"{updated_count} updated, {deactivated} deactivated, "
            f"{closed_aliases} aliases closed (total: {len(symbols)})"
        )
