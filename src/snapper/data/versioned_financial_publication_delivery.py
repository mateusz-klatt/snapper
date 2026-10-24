"""Recover declared notification versions through their complete actual source graphs.

Local publication is at least once. A receipt records a successful publisher
return, without claiming broker persistence or downstream consumption. All
versions retain the existing physical intent and receipt uniqueness domains.
"""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_financial_publication_v2 import NativeContractPositionPublicationV2
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_delivery import _append_delivery
from snapper.data.financial_publication_delivery import _pending_publication_ids
from snapper.data.financial_publication_history import BoundVersionedFinancialPublication
from snapper.data.financial_publication_history import get_bound_versioned_financial_publication
from snapper.data.financial_publication_history import verify_versioned_financial_delivery
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.native_admitted_position_guards import require_native_admitted_position_guards


async def _delivery(session: AsyncSession, public_id: str) -> FinancialPublicationDelivery | None:
    """Load the actual receipt without treating a nonmatching index projection as absence."""
    return await session.scalar(
        select(FinancialPublicationDelivery).where(
            FinancialPublicationDelivery.publication_public_id == public_id
        )
    )


async def _current_pending(
    session: AsyncSession, identities: tuple[str, ...]
) -> tuple[BoundVersionedFinancialPublication, ...]:
    """Refuse damaged receipts and skip a valid receipt committed after the pending query."""
    pending: list[BoundVersionedFinancialPublication] = []
    for public_id in identities:
        intent = await get_bound_versioned_financial_publication(session, public_id)
        receipt = await _delivery(session, public_id)
        if receipt is None:
            pending.append(intent)
        else:
            verify_versioned_financial_delivery(receipt, intent)
    return tuple(pending)


async def load_pending_versioned_financial_publications(
    repository: AccountingRepository, scope: ExecutionAccountScope, limit: int = 100
) -> tuple[BoundVersionedFinancialPublication, ...]:
    """Load a bounded committed page without interpreting another notification family on failure.

    Args:
        repository: Actual financial repository, independent of any booking transaction.
        scope: Explicit original wallet, venue, mode, environment and margin account.
        limit: Exact integer page bound between one and one thousand.

    Returns:
        Complete source-bound legacy or native notifications in durable insertion order.
    """
    actual = ExecutionAccountScope.model_validate(scope)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Financial publication page limit must be between one and one thousand")
    async with repository.session() as session:
        identities = await _pending_publication_ids(session, actual, limit)
        return await _current_pending(session, identities)


async def record_versioned_financial_publication_delivery(
    repository: AccountingRepository,
    publication_public_id: EvidenceUUID,
    published_at: datetime,
    provenance: SnapshotWriteProvenance,
) -> str:
    """Commit an actual local-return receipt under the original wallet and restore barriers.

    Args:
        repository: Actual database transaction and wallet-fence implementation.
        publication_public_id: Existing immutable intent whose actual callback returned.
        published_at: Original aware local successful callback-return clock.
        provenance: Stable receipt identity and recording clock for an exact retry.

    Returns:
        The original or new receipt UUID after commit; an uncertain commit still raises.
    """
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        wallet_public_id = await session.scalar(
            select(FinancialPublicationIntent.wallet_public_id).where(
                FinancialPublicationIntent.public_id == publication_public_id
            )
        )
        if wallet_public_id is None:
            raise FinancialPublicationError("financial_publication_missing")
        await repository._acquire_execution_fence_lock(session, wallet_public_id)
        intent = await get_bound_versioned_financial_publication(session, publication_public_id)
        if isinstance(intent.payload, NativeContractPositionPublicationV2):
            await require_native_admitted_position_guards(session)
        else:
            await acquire_contract_accounting_write_barrier(session)
        intent = await get_bound_versioned_financial_publication(session, publication_public_id)
        if intent.payload.scope.wallet_public_id != wallet_public_id:
            raise FinancialPublicationError("financial_delivery_actual_wallet_mismatch")
        identity = await _append_delivery(session, intent, published_at, stamp)
        retained = (
            await session.scalars(
                select(FinancialPublicationDelivery).where(
                    FinancialPublicationDelivery.publication_public_id == intent.public_id
                )
            )
        ).one()
        verify_versioned_financial_delivery(retained, intent)
        await session.commit()
        return identity
