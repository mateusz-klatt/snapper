"""Recover committed notification intents and retain honest local publication receipts."""

from datetime import datetime

from sqlalchemy import and_
from sqlalchemy import exists
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_delivery_types import PublicationReceiptSource
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_store import get_bound_financial_publication
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import is_open_ended_version
from snapper.data.session_context import session_no_autoflush


def _verify_delivery(row: FinancialPublicationDelivery, intent: PublicationReceiptSource) -> None:
    """Require the actual intent scope and digest before accepting a previous delivery receipt."""
    scope = intent.payload.scope
    actual = (
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.environment,
        row.margin_account,
        row.publication_public_id,
        row.publication_digest,
        row.delivery_semantics,
    )
    expected = (
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        scope.margin_account,
        intent.public_id,
        intent.content_digest,
        "publisher_returned",
    )
    if (
        not is_open_ended_version(row.known_to)
        or actual != expected
        or not intent.payload.recorded_at <= row.published_at <= row.timestamp
    ):
        raise FinancialPublicationError("financial_delivery_actual_intent_mismatch")


async def _append_delivery(
    session: AsyncSession,
    intent: PublicationReceiptSource,
    published_at: datetime,
    provenance: SnapshotWriteProvenance,
) -> str:
    """Deduplicate acknowledgement retries under the actual wallet fence without changing history."""
    existing = (
        await session.execute(
            select(FinancialPublicationDelivery).where(
                FinancialPublicationDelivery.publication_public_id == intent.public_id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        _verify_delivery(existing, intent)
        return existing.public_id
    if not intent.payload.recorded_at <= published_at <= provenance.timestamp:
        raise FinancialPublicationError("financial_delivery_clock_mismatch")
    row = FinancialPublicationDelivery(
        **provenance.model_dump(mode="python"),
        **intent.payload.scope.model_dump(mode="python"),
        publication_public_id=intent.public_id,
        publication_digest=intent.content_digest,
        delivery_semantics="publisher_returned",
        published_at=published_at,
    )
    session.add(row)
    await session.flush()
    return row.public_id


async def record_financial_publication_delivery(
    repository: AccountingRepository,
    publication_public_id: EvidenceUUID,
    published_at: datetime,
    provenance: SnapshotWriteProvenance,
) -> str:
    """Commit local publisher-return evidence after an actual publication callback succeeded.

    Args:
        repository: Actual database transaction and wallet-fence implementation.
        publication_public_id: Immutable intent that the publisher just accepted.
        published_at: Actual local successful callback-return clock.
        provenance: Independent receipt identity and record clock.

    Returns:
        The original or newly committed local delivery receipt UUID.
    """
    provenance = SnapshotWriteProvenance.model_validate(provenance)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        intent = await get_bound_financial_publication(session, publication_public_id)
        await repository._acquire_execution_fence_lock(
            session, intent.payload.scope.wallet_public_id
        )
        await acquire_contract_accounting_write_barrier(session)
        intent = await get_bound_financial_publication(session, publication_public_id)
        result = await _append_delivery(session, intent, published_at, provenance)
        await session.commit()
        return result


async def load_pending_financial_publications(
    repository: AccountingRepository, scope: ExecutionAccountScope, limit: int = 100
) -> tuple[BoundFinancialPublication, ...]:
    """Read only committed pending accounting notifications for an exact actual account scope.

    Args:
        repository: Actual financial database whose session sees committed rows.
        scope: Explicit wallet, venue, environment and native margin profile.
        limit: Bounded recovery page size between one and one thousand.

    Returns:
        Source-bound notifications in durable insertion order, without publishing any.
    """
    scope = ExecutionAccountScope.model_validate(scope)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Financial publication page limit must be between one and one thousand")
    async with repository.session() as session:
        identities = await _pending_publication_ids(session, scope, limit)
        return await _current_pending_legacy(session, identities)


async def _current_pending_legacy(
    session: AsyncSession, identities: tuple[str, ...]
) -> tuple[BoundFinancialPublication, ...]:
    """Recheck actual legacy receipts before callback and skip a concurrently acknowledged intent."""
    pending: list[BoundFinancialPublication] = []
    for public_id in identities:
        intent = await get_bound_financial_publication(session, public_id)
        receipt = await session.scalar(
            select(FinancialPublicationDelivery).where(
                FinancialPublicationDelivery.publication_public_id == public_id
            )
        )
        if receipt is None:
            pending.append(intent)
        else:
            _verify_delivery(receipt, intent)
    return tuple(pending)


async def _pending_publication_ids(
    session: AsyncSession, scope: ExecutionAccountScope, limit: int
) -> tuple[str, ...]:
    """Select actual pending identities using the complete local receipt scope and clock shape."""
    digest = account_scope_digest(scope)
    with session_no_autoflush(session):
        await _verify_scoped_delivery_markers(session, digest)
        return await _pending_unacknowledged_ids(session, digest, limit)


async def _verify_scoped_delivery_markers(session: AsyncSession, scope_digest: str) -> None:
    """Refuse damaged raw lifetime markers before receipts can exclude any recovery candidates."""
    markers = await session.scalars(
        select(FinancialPublicationDelivery.known_to)
        .join(
            FinancialPublicationIntent,
            FinancialPublicationIntent.public_id
            == FinancialPublicationDelivery.publication_public_id,
        )
        .where(FinancialPublicationIntent.account_scope_digest == scope_digest)
    )
    if any(not is_open_ended_version(marker) for marker in markers):
        raise FinancialPublicationError("financial_delivery_actual_intent_mismatch")


async def _pending_unacknowledged_ids(
    session: AsyncSession, scope_digest: str, limit: int
) -> tuple[str, ...]:
    """Apply bounded ordering after scoped raw receipt markers have passed exact validation."""
    acknowledged = exists(
        select(FinancialPublicationDelivery.id).where(
            and_(
                FinancialPublicationDelivery.publication_public_id
                == FinancialPublicationIntent.public_id,
                FinancialPublicationDelivery.publication_digest
                == FinancialPublicationIntent.content_digest,
                FinancialPublicationDelivery.wallet_public_id
                == FinancialPublicationIntent.wallet_public_id,
                FinancialPublicationDelivery.exchange == FinancialPublicationIntent.exchange,
                FinancialPublicationDelivery.mode == FinancialPublicationIntent.mode,
                FinancialPublicationDelivery.environment == FinancialPublicationIntent.environment,
                FinancialPublicationDelivery.margin_account
                == FinancialPublicationIntent.margin_account,
                FinancialPublicationDelivery.published_at >= FinancialPublicationIntent.timestamp,
                FinancialPublicationDelivery.delivery_semantics == "publisher_returned",
                FinancialPublicationDelivery.published_at <= FinancialPublicationDelivery.timestamp,
            )
        )
    )
    identities = (
        await session.scalars(
            select(FinancialPublicationIntent.public_id)
            .where(
                FinancialPublicationIntent.account_scope_digest == scope_digest,
                ~acknowledged,
            )
            .order_by(FinancialPublicationIntent.id)
            .limit(limit)
        )
    ).all()
    return tuple(identities)
