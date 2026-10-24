"""Bind publication intent to actual accounting facts in the caller's financial transaction."""

from dataclasses import dataclass
from typing import cast

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_publication import ContractPositionPublication
from snapper.core.financial_publication import canonical_financial_publication
from snapper.core.financial_publication import financial_publication_digest
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_position_ledger_store import get_bound_contract_position_event
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import FinancialPublicationIntent


class FinancialPublicationError(ValueError):
    """A notification cannot be bound to the same immutable actual financial evidence."""


@dataclass(frozen=True)
class BoundFinancialPublication:
    """One source-bound notification, safe to publish only after its transaction commits."""

    public_id: str
    topic: str
    payload: ContractPositionPublication
    content_digest: str
    created: bool = False


def financial_publication_topic(payload: ContractPositionPublication) -> str:
    """Use a dedicated accounting topic whose reference message cannot enter spot fill handlers.

    Args:
        payload: Validated actual accounting scope and instrument identity.

    Returns:
        A deterministic notification topic distinct from executable order and fill topics.
    """
    scope = payload.scope
    return (
        f"accounting.contract_position.{scope.exchange}.{scope.environment}."
        f"{scope.wallet_public_id}.{payload.instrument_public_id}"
    )


def _require(condition: bool, reason: str) -> None:
    """Abort publication intent without committing any surrounding financial work."""
    if not condition:
        raise FinancialPublicationError(reason)


def _payload(
    position: ContractPositionEventPayload,
    row: ContractAccountingEvent,
    provenance: SnapshotWriteProvenance,
) -> ContractPositionPublication:
    """Copy references after the bound position reader has verified its required fill-target variant."""
    target = cast(FillSnapshotTarget, position.unit.target)
    _require(
        provenance.timestamp >= row.timestamp,
        "financial_publication_precedes_position_knowledge",
    )
    return ContractPositionPublication(
        scope=position.scope,
        source_execution_id=position.venue_execution_id,
        execution_public_id=position.execution_public_id,
        venue_event_public_id=target.venue_event_public_id,
        unit_snapshot_public_id=position.unit_snapshot_public_id,
        position_event_public_id=row.public_id,
        instrument_public_id=position.instrument_public_id,
        raw_receipt_public_id=position.raw_receipt_public_id,
        route_authority_public_id=position.route_authority_public_id,
        ledger_sequence=position.ledger_sequence,
        executed_at=position.executed_at,
        execution_known_at=position.execution_known_at,
        recorded_at=provenance.timestamp,
    )


async def _actual_payload(
    session: AsyncSession, position_event_public_id: str, provenance: SnapshotWriteProvenance
) -> ContractPositionPublication:
    """Rebind the original position event and its complete source chain, including uncommitted rows."""
    position = await get_bound_contract_position_event(session, position_event_public_id)
    row = (
        await session.execute(
            select(ContractAccountingEvent).where(
                ContractAccountingEvent.public_id == position_event_public_id
            )
        )
    ).scalar_one()
    return _payload(position, row, provenance)


def _verify_projection(
    row: FinancialPublicationIntent, payload: ContractPositionPublication
) -> None:
    """Keep indexed routing, deduplication and transport material bound to the full actual payload."""
    scope = payload.scope
    _require(
        (
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.account_scope_digest,
            row.source_execution_id,
            row.execution_public_id,
            row.position_event_public_id,
            row.topic,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            scope.margin_account,
            account_scope_digest(scope),
            payload.source_execution_id,
            payload.execution_public_id,
            payload.position_event_public_id,
            financial_publication_topic(payload),
            financial_publication_digest(payload),
            canonical_financial_publication(payload),
        ),
        "financial_publication_projection_mismatch",
    )


async def _bound_publication(
    session: AsyncSession, row: FinancialPublicationIntent
) -> BoundFinancialPublication:
    """Revalidate the canonical record against the actual original source-bound position fact."""
    payload = ContractPositionPublication.model_validate_json(row.canonical_payload)
    _verify_projection(row, payload)
    actual = await _actual_payload(
        session,
        row.position_event_public_id,
        SnapshotWriteProvenance(
            public_id=row.public_id,
            session_id=row.session_id,
            sequence_id=row.sequence_id,
            timestamp=row.timestamp,
        ),
    )
    _require(actual == payload, "financial_publication_actual_fact_mismatch")
    return BoundFinancialPublication(row.public_id, row.topic, payload, row.content_digest)


async def get_bound_financial_publication(
    session: AsyncSession, public_id: EvidenceUUID
) -> BoundFinancialPublication:
    """Read one immutable actual notification without trusting its indexed target UUID alone.

    Args:
        session: Caller-owned database session.
        public_id: Actual retained publication identity.

    Returns:
        The validated ledger-bound notification; no publication or commit occurs.
    """
    row = (
        await session.execute(
            select(FinancialPublicationIntent).where(
                FinancialPublicationIntent.public_id == public_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise FinancialPublicationError("financial_publication_missing")
    return await _bound_publication(session, row)


async def _existing_publication(
    session: AsyncSession, payload: ContractPositionPublication
) -> BoundFinancialPublication | None:
    """Require all immutable identity keys to select the same original financial notification."""
    rows = (
        await session.scalars(
            select(FinancialPublicationIntent).where(
                or_(
                    FinancialPublicationIntent.execution_public_id == payload.execution_public_id,
                    FinancialPublicationIntent.position_event_public_id
                    == payload.position_event_public_id,
                    (
                        FinancialPublicationIntent.account_scope_digest
                        == account_scope_digest(payload.scope)
                    )
                    & (
                        FinancialPublicationIntent.source_execution_id
                        == payload.source_execution_id
                    ),
                )
            )
        )
    ).all()
    if not rows:
        return None
    _require(len(rows) == 1, "financial_publication_identity_conflict")
    original = await _bound_publication(session, rows[0])
    expected = ContractPositionPublication.model_validate(
        {**payload.model_dump(mode="python"), "recorded_at": original.payload.recorded_at}
    )
    _require(original.payload == expected, "financial_publication_source_conflict")
    return original


async def append_bound_financial_publication(
    session: AsyncSession,
    position_event_public_id: EvidenceUUID,
    provenance: SnapshotWriteProvenance,
) -> BoundFinancialPublication:
    """Flush a notification in the same wallet-fenced transaction as its actual financial fact.

    Args:
        session: Caller-owned financial transaction already holding the wallet execution fence.
        position_event_public_id: Real source-bound position event already flushed in that session.
        provenance: Independent publication identity and actual local record clock.

    Returns:
        A flushed or original notification. The caller must commit before publishing it.
    """
    if not session.in_transaction():
        raise RuntimeError("financial_publication_requires_caller_transaction")
    await acquire_contract_accounting_write_barrier(session)
    provenance = SnapshotWriteProvenance.model_validate(provenance)
    payload = await _actual_payload(session, position_event_public_id, provenance)
    original = await _existing_publication(session, payload)
    if original is not None:
        return original
    row = FinancialPublicationIntent(
        **provenance.model_dump(mode="python"),
        **payload.scope.model_dump(mode="python"),
        account_scope_digest=account_scope_digest(payload.scope),
        source_execution_id=payload.source_execution_id,
        execution_public_id=payload.execution_public_id,
        position_event_public_id=payload.position_event_public_id,
        topic=financial_publication_topic(payload),
        content_digest=financial_publication_digest(payload),
        canonical_payload=canonical_financial_publication(payload),
    )
    session.add(row)
    await session.flush()
    return BoundFinancialPublication(row.public_id, row.topic, payload, row.content_digest, True)
