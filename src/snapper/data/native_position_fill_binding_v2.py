"""Bind original fill lineage for native positions without rewriting legacy unit or authority evidence.

This historical adapter creates no Execution, VenueEvent, unit witness or
financial fact. Its knowledge clock must come from the actual Execution row.
Later native account-log authority and ownership remain separate references
in the position payload and cannot retroactively establish an older fill.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.private_fill_source import FuturesPrivateFill
from snapper.application.trade.private_fill_source import parse_futures_private_fill
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.application.trade.unit_snapshot_fill import bind_futures_fill_source
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import client_selection_from_native_authority
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import UnitCatalogObservation
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.models import SymbolAlias
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_financial_source_binding_v2 import verify_private_ownership_reference
from snapper.data.private_source_ownership_store import BoundPrivateSourceOwnership
from snapper.data.private_source_ownership_store import get_bound_private_source_ownership

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


class OriginalPositionFillRequestV2(BaseModel):
    """Actual original source references without a later authority preference or fabricated native UID."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    wallet_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    authority_reference: AccountAuthorityReference
    ownership_reference: PrivateSourceOwnershipReference

    @model_validator(mode="after")
    def validate_original_raw_reference(self) -> Self:
        """Keep the exact original raw identity independent of a later position-log receipt.

        Returns:
            The unchanged reference shape awaiting every actual graph and recording-clock check.
        """
        if self.raw_receipt_public_id != self.ownership_reference.raw_receipt_public_id:
            raise ValueError("Original position fill ownership names another raw receipt")
        return self


@dataclass(frozen=True, slots=True)
class BoundOriginalPositionFillV2:
    """Actual original raw, authority, authenticated UID and historical provider-to-instrument lineage."""

    raw: RawAccountingObservation
    authority: BoundAccountAuthority
    ownership: BoundPrivateSourceOwnership
    fill: FuturesPrivateFill
    alias: SymbolAlias
    catalog: UnitCatalogObservation


def _verify_original_client(
    request: OriginalPositionFillRequestV2,
    raw: RawAccountingObservation,
    authority: BoundAccountAuthority,
    ownership: BoundPrivateSourceOwnership,
) -> None:
    """Bind the original actual selected client; a later same-UID authority cannot replace its source."""
    payload = authority.payload
    selected = (
        client_selection_from_native_authority(payload)
        if isinstance(payload, NativeAccountAuthorityPayload)
        else client_selection_from_authority(payload)
    )
    frame = ownership.payload.frame_context
    require_native_financial_binding(
        selected == frame.client_selection
        and selected.wallet_public_id == request.wallet_public_id
        and raw.scope.model_dump(exclude={"margin_account"})
        == {
            name: getattr(selected, name)
            for name in ("wallet_public_id", "exchange", "mode", "environment")
        }
        and raw.credential_public_id == selected.credential_public_id
        and raw.credential_timestamp == selected.credential_timestamp
        and raw.route_revision == client_selection_digest(selected)
        and raw.scope.margin_account in {None, payload.scope.margin_account},
        "native_position_original_fill_client_mismatch",
    )
    require_native_financial_binding(
        frame.subscription_feed == "fills"
        and frame.raw_feed in {"fills", "fills_snapshot"}
        and raw.channel == f"kraken_futures.ws.{frame.raw_feed}"
        and raw.target_json_pointer == ownership.payload.target_json_pointer
        and accounting_observation_digest(raw) == ownership.payload.raw_content_digest,
        "native_position_original_fill_frame_mismatch",
    )
    if isinstance(payload, NativeAccountAuthorityPayload):
        require_native_financial_binding(
            ownership.account_uid == payload.venue_account_id
            and payload.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2",
            "native_position_original_fill_native_uid_unproven",
        )


async def _native_fill_catalog(
    session: AsyncSession, fill: FuturesPrivateFill, known_at: datetime
) -> tuple[SymbolAlias, UnitCatalogObservation]:
    """Apply the existing exact historical alias/catalog rules without inferring an account UID."""
    alias = (
        await session.execute(
            select(SymbolAlias).where(
                SymbolAlias.exchange == "kraken_futures",
                SymbolAlias.channel == "ws",
                SymbolAlias.exchange_symbol == fill.provider_symbol,
                SymbolAlias.timestamp <= known_at,
                SymbolAlias.known_to > known_at,
            )
        )
    ).scalar_one_or_none()
    if alias is None:
        raise NativeFinancialBindingError("native_position_original_provider_alias_missing")
    catalog = await load_unit_catalog(
        session,
        UnitCatalogLookup(exchange="kraken_futures", symbol_public_id=alias.symbol_public_id),
        known_at,
    )
    catalog_economic_context(catalog)
    return alias, catalog


async def bind_original_position_fill_v2(
    session: AsyncSession,
    request: OriginalPositionFillRequestV2,
    execution_known_at: datetime,
) -> BoundOriginalPositionFillV2:
    """Bind genuine original fill evidence at its actual Execution knowledge clock.

    The original selected authority must match the actual retained private
    client. A later native log may use another independently proved same-UID
    credential, but it must not replace this original fill authority or clock.
    Current freshness, financial ownership and posting admission are separate
    checks at the later position writer boundary.

    Args:
        session: Actual caller-owned historical or financial transaction.
        request: Exact original raw, tagged authority and six-field private ownership reference.
        execution_known_at: Actual Execution.timestamp, distinct from provider executed_at and later posting.

    Returns:
        Actual original fill, source identity and instrument/catalog facts without current permission.
    """
    actual = OriginalPositionFillRequestV2.model_validate(request)
    clock = _INSTANT.validate_python(execution_known_at)
    authority = await get_bound_account_authority_reference(session, actual.authority_reference)
    ownership = await get_bound_private_source_ownership(
        session, actual.raw_receipt_public_id, clock
    )
    verify_private_ownership_reference(actual.ownership_reference, ownership)
    require_native_financial_binding(
        authority.recorded_at <= clock and ownership.recorded_at <= clock,
        "native_position_original_fill_knowledge_after_execution",
    )
    if actual.authority_reference.family == "legacy_route_v1":
        legacy = await bind_futures_fill_source(
            session,
            FuturesFillSourceRequest(
                wallet_public_id=actual.wallet_public_id,
                raw_receipt_public_id=actual.raw_receipt_public_id,
                route_authority_public_id=actual.authority_reference.public_id,
            ),
            clock,
        )
        raw, fill, alias, catalog = (
            legacy.raw,
            legacy.fill,
            legacy.alias,
            legacy.catalog,
        )
        require_native_financial_binding(
            legacy.authority == authority.payload,
            "native_position_original_legacy_authority_changed",
        )
    else:
        raw = await get_bound_accounting_observation(session, actual.raw_receipt_public_id)
        fill = parse_futures_private_fill(raw)
        alias, catalog = await _native_fill_catalog(session, fill, clock)
    _verify_original_client(actual, raw, authority, ownership)
    return BoundOriginalPositionFillV2(raw, authority, ownership, fill, alias, catalog)
