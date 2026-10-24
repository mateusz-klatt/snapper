"""Bind and atomically claim native-v2 source consumption in the existing shared0060 namespace.

The native-ID unique key, claim UUID derivation and semantic source digests do
not change with financial schema, kind, instrument, margin or credential.
V1 readers and canonical bytes stay unchanged. Complete ledger readers verify
anchors, economic chains and actual execution/unit lineage above this source
claim layer, using the actual event and source material returned here.
"""

from datetime import datetime
from uuid import NAMESPACE_URL
from uuid import uuid5

from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.accounting_native_source import (
    AccountingNativeSourceClaimPayload,
)
from snapper.application.portfolio.accounting_native_source import native_source_family_digest
from snapper.application.portfolio.accounting_native_source import native_source_identity_digest
from snapper.application.portfolio.accounting_native_source import native_source_log_id
from snapper.application.portfolio.accounting_native_source import native_source_semantic_digest
from snapper.application.portfolio.accounting_native_source_v2 import (
    AccountingNativeSourceClaimPayloadV2,
)
from snapper.application.portfolio.accounting_native_source_v2 import (
    canonical_native_source_claim_v2,
)
from snapper.application.portfolio.accounting_native_source_v2 import native_source_claim_digest_v2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.data.accounting_native_source_store import get_bound_native_source_claim
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards
from snapper.data.native_financial_event_binding_v2 import NativeFinancialEventMaterialV2
from snapper.data.native_financial_event_binding_v2 import bind_native_financial_event_material
from snapper.data.native_financial_event_binding_v2 import get_native_financial_event_material
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)


def native_claim_public_id_v2(event_public_id: str) -> str:
    """Reuse the existing event-derived identity across financial claim versions.

    Args:
        event_public_id: Actual financial event UUID, already validated from its session-owned row.

    Returns:
        The unchanged v1-derived claim identity; schema does not create a second ownership slot.
    """
    return str(uuid5(NAMESPACE_URL, f"snapper:accounting-native-source-claim:v1:{event_public_id}"))


def native_claim_payload_v2(
    material: NativeFinancialEventMaterialV2,
) -> AccountingNativeSourceClaimPayloadV2:
    """Project the actual bound event and selected native child into one complete source claim.

    Args:
        material: Session-owned event with actual original source, policy and private ownership bound.

    Returns:
        Exact canonical claim material before checking its real immutable0060 row.
    """
    value = material.payload
    raw_id = (
        value.operands.raw_receipt_public_id
        if isinstance(value, FuturesNativeCashEventV2)
        else value.operands.position_receipt_public_id
    )
    return AccountingNativeSourceClaimPayloadV2(
        scope=value.operands.scope,
        accounting_event_public_id=material.row.public_id,
        accounting_event_content_digest=material.row.content_digest,
        raw_receipt_public_id=raw_id,
        route_authority_public_id=value.source.authority_reference.public_id,
        kind=value.kind,
        source=value.source,
        source_entry=material.source.entry,
    )


def native_claim_columns_v2(payload: AccountingNativeSourceClaimPayloadV2) -> dict[str, object]:
    """Derive every indexed copy while retaining existing native source identity hash domains.

    Args:
        payload: Strict complete claim whose actual event/source graph has already been bound.

    Returns:
        Exact physical0060 mirrors; kind, margin or version never partition native-ID uniqueness.
    """
    return {
        **payload.scope.model_dump(mode="python"),
        "scope_digest": native_source_family_digest(payload.scope),
        "account_scope_digest": account_scope_digest(payload.scope),
        "source_domain": payload.source_domain,
        "source_log_id": native_source_log_id(payload.source_entry),
        "source_identity_digest": native_source_identity_digest(payload.source_entry),
        "semantic_digest": native_source_semantic_digest(payload.source_entry),
        "accounting_event_public_id": payload.accounting_event_public_id,
        "raw_receipt_public_id": payload.raw_receipt_public_id,
        "route_authority_public_id": payload.route_authority_public_id,
        "kind": payload.kind,
        "booking_uid": payload.source_entry.booking_uid,
        "content_digest": native_source_claim_digest_v2(payload),
        "canonical_payload": canonical_native_source_claim_v2(payload),
    }


async def _bound_v2_claim(
    session: AsyncSession, row: AccountingNativeSourceClaim, known_at: datetime
) -> AccountingNativeSourceClaimPayloadV2:
    """Rebind every row mirror plus original event/provenance, without trusting a mere claim join."""
    payload = AccountingNativeSourceClaimPayloadV2.model_validate_json(row.canonical_payload)
    require_native_financial_binding(
        is_open_ended_version(row.known_to)
        and row.public_id == native_claim_public_id_v2(payload.accounting_event_public_id)
        and all(
            getattr(row, name) == value for name, value in native_claim_columns_v2(payload).items()
        ),
        "native_financial_claim_projection_mismatch",
    )
    material = await get_native_financial_event_material(
        session, payload.accounting_event_public_id, known_at
    )
    event = material.row
    require_native_financial_binding(
        (row.timestamp, row.session_id, row.sequence_id)
        == (event.timestamp, event.session_id, event.sequence_id)
        and row.timestamp <= known_at,
        "native_financial_claim_provenance_mismatch",
    )
    require_native_financial_binding(
        native_claim_payload_v2(material) == payload,
        "native_financial_claim_original_lineage_mismatch",
    )
    return payload


async def _bound_existing_claim(
    session: AsyncSession, row: AccountingNativeSourceClaim, known_at: datetime
) -> AccountingNativeSourceClaimPayload | AccountingNativeSourceClaimPayloadV2:
    """Dispatch only an explicit strict version, without a preference or parse-error fallback."""
    document = decode_private_execution_receipt(row.canonical_payload.encode())
    version = document.get("schema_version")
    require_native_financial_binding(
        type(version) is int and version in {1, 2} and row.timestamp <= known_at,
        "native_financial_existing_claim_schema_or_clock_unsupported",
    )
    if version == 1:
        return await get_bound_native_source_claim(session, row.accounting_event_public_id)
    return await _bound_v2_claim(session, row, known_at)


async def get_bound_native_source_claim_v2(
    session: AsyncSession, accounting_event_public_id: str, known_at: datetime
) -> AccountingNativeSourceClaimPayloadV2:
    """Require an actual native-v2 source claim for complete ledger replay or publication binding.

    Args:
        session: Actual caller-owned historical or financial transaction.
        accounting_event_public_id: Actual retained native-v2 financial event UUID.
        known_at: Required knowledge cutoff; original row and source clocks remain independently bound.

    Returns:
        Its exact source/event/policy/sidecar claim, without a current financial admission grant.
    """
    with session.no_autoflush:
        row = await session.scalar(
            select(AccountingNativeSourceClaim).where(
                AccountingNativeSourceClaim.accounting_event_public_id == accounting_event_public_id
            )
        )
        if row is None:
            raise NativeFinancialBindingError("native_financial_actual_claim_missing")
        return await _bound_v2_claim(session, row, known_at)


async def _require_claimed_family(
    session: AsyncSession, scope: ExecutionAccountScope, known_at: datetime
) -> None:
    """Keep legacy or orphaned family history unavailable instead of promoting it from a new claim."""
    rows = await session.execute(
        select(ContractAccountingEvent.public_id, AccountingNativeSourceClaim)
        .outerjoin(
            AccountingNativeSourceClaim,
            AccountingNativeSourceClaim.accounting_event_public_id
            == ContractAccountingEvent.public_id,
        )
        .where(
            ContractAccountingEvent.wallet_public_id == scope.wallet_public_id,
            ContractAccountingEvent.exchange == scope.exchange,
            ContractAccountingEvent.mode == scope.mode,
            ContractAccountingEvent.environment == scope.environment,
        )
    )
    event_ids: set[str] = set()
    for event_id, row in rows:
        if row is None:
            raise NativeFinancialBindingError("native_financial_family_history_unclaimed")
        payload = await _bound_existing_claim(session, row, known_at)
        require_native_financial_binding(
            isinstance(payload, AccountingNativeSourceClaimPayloadV2)
            and payload.accounting_event_public_id == event_id
            and native_source_family_digest(payload.scope) == native_source_family_digest(scope),
            "native_financial_family_history_requires_explicit_migration",
        )
        event_ids.add(event_id)
    claimed_ids = await session.scalars(
        select(AccountingNativeSourceClaim.accounting_event_public_id).where(
            AccountingNativeSourceClaim.wallet_public_id == scope.wallet_public_id,
            AccountingNativeSourceClaim.exchange == scope.exchange,
            AccountingNativeSourceClaim.mode == scope.mode,
            AccountingNativeSourceClaim.environment == scope.environment,
        )
    )
    actual_claimed_ids = list(claimed_ids)
    require_native_financial_binding(
        len(actual_claimed_ids) == len(event_ids) and set(actual_claimed_ids) == event_ids,
        "native_financial_family_claim_inventory_orphaned",
    )


async def _require_unused_source(
    session: AsyncSession, payload: AccountingNativeSourceClaimPayloadV2, known_at: datetime
) -> None:
    """Detect source reuse across cash, positions, margin, instrument and both financial schemas."""
    scope = payload.scope
    existing = await session.scalar(
        select(AccountingNativeSourceClaim).where(
            AccountingNativeSourceClaim.source_domain == payload.source_domain,
            or_(
                and_(
                    AccountingNativeSourceClaim.wallet_public_id == scope.wallet_public_id,
                    AccountingNativeSourceClaim.exchange == scope.exchange,
                    AccountingNativeSourceClaim.mode == scope.mode,
                    AccountingNativeSourceClaim.environment == scope.environment,
                    AccountingNativeSourceClaim.source_log_id
                    == native_source_log_id(payload.source_entry),
                ),
                and_(
                    AccountingNativeSourceClaim.scope_digest == native_source_family_digest(scope),
                    AccountingNativeSourceClaim.source_identity_digest
                    == native_source_identity_digest(payload.source_entry),
                ),
            ),
        )
    )
    if existing is not None:
        await _bound_existing_claim(session, existing, known_at)
        raise NativeFinancialBindingError("native_financial_source_already_consumed")


async def claim_new_accounting_source_v2(
    session: AsyncSession, event: ContractAccountingEvent
) -> AccountingNativeSourceClaimPayloadV2:
    """Flush one actual newly pending event and its exclusive native claim in the caller's transaction.

    Args:
        session: Caller-owned wallet-fenced transaction, after current owner/source admission.
        event: Sole pending financial event, already validated by the complete cash/position writer.
            A failure must roll back all surrounding financial rows; this method never commits.

    Returns:
        The exact new native claim after both rows flush together, with all holds still pending.
    """
    require_native_financial_binding(
        event in session.new
        and sum(isinstance(row, ContractAccountingEvent) for row in session.new) == 1
        and not any(isinstance(row, AccountingNativeSourceClaim) for row in session.new),
        "native_financial_claim_requires_one_immediate_pending_event",
    )
    with session.no_autoflush:
        await require_native_contract_accounting_guards(session)
        material = await bind_native_financial_event_material(session, event, event.timestamp)
        payload = native_claim_payload_v2(material)
        await _require_claimed_family(session, payload.scope, event.timestamp)
        await _require_unused_source(session, payload, event.timestamp)
        session.add(
            AccountingNativeSourceClaim(
                public_id=native_claim_public_id_v2(event.public_id),
                session_id=event.session_id,
                sequence_id=event.sequence_id,
                timestamp=event.timestamp,
                **native_claim_columns_v2(payload),
            )
        )
    await session.flush()
    return payload
