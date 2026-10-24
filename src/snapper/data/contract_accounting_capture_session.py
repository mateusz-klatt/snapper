"""Compose original raw-and-hold capture without granting a later transaction ownership of old rows.

The private registry is keyed by the actual live SQLAlchemy transaction. Only
this wrapper registers results returned by the unchanged Stage-A implementation.
No public result shape, caller flag or later transaction can reconstruct that
relationship. Entries contain independent primitive capture snapshots and no authentication data.
Public return views never alias a privately retained created/conflicted marker.
"""

from dataclasses import replace
from weakref import WeakKeyDictionary

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import SessionTransaction

from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import _capture_under_fence
from snapper.data.contract_accounting_store import _ObservationInsert

_STAGED: WeakKeyDictionary[SessionTransaction, dict[str, RawAccountingWriteResult]] = (
    WeakKeyDictionary()
)


class StagedAccountingObservationError(ValueError):
    """A result was not produced by this exact still-active caller transaction."""


def _transaction(session: AsyncSession) -> SessionTransaction:
    """Identify the actual active root transaction instead of an interchangeable async proxy or UUID."""
    if session.sync_session.in_nested_transaction():
        raise StagedAccountingObservationError(
            "Raw capture requires the root transaction without a SAVEPOINT"
        )
    transaction = session.sync_session.get_transaction()
    if transaction is None or not transaction.is_active:
        raise StagedAccountingObservationError("Actual active raw capture transaction is required")
    return transaction


def require_staged_accounting_observation(
    session: AsyncSession, raw_receipt_public_id: str
) -> RawAccountingWriteResult:
    """Require the real in-session Stage-A result before an optional sidecar can be composed.

    Args:
        session: Actual caller session whose wallet-fenced raw/hold transaction remains active.
        raw_receipt_public_id: Actual returned raw UUID, never only a claimed or content identity.

    Returns:
        An independent public view of the original captured facts, including the new-staged-raw flag.
    """
    transaction = _transaction(session)
    result = _STAGED.get(transaction, {}).get(raw_receipt_public_id)
    if result is None:
        raise StagedAccountingObservationError("Raw was not captured in this actual transaction")
    return replace(result)


async def capture_bound_accounting_observation(
    session: AsyncSession,
    observation: RawAccountingObservation,
    provenance: AccountingWriteProvenance,
) -> RawAccountingWriteResult:
    """Stage unchanged raw and coarse hold while registering their exact actual transaction.

    Args:
        session: Existing caller-owned wallet-fenced transaction; this wrapper never commits.
        observation: Original frozen financial envelope without an ownership claim.
        provenance: Stable raw identity and honest original retention clock preserved on retry.

    Returns:
        Actual raw/hold identity and conflict disposition from the original Stage-A implementation.
    """
    transaction = _transaction(session)
    evidence = RawAccountingObservation.model_validate(observation)
    stamp = AccountingWriteProvenance.model_validate(provenance)
    data = _ObservationInsert(
        evidence,
        stamp,
        canonical_accounting_observation(evidence),
        accounting_observation_digest(evidence),
    )
    result = await _capture_under_fence(session, data)
    if _transaction(session) is not transaction:
        raise StagedAccountingObservationError("Raw capture changed its actual transaction")
    staged = _STAGED.setdefault(transaction, {})
    previous = staged.get(result.public_id)
    if previous is None or not previous.created:
        staged[result.public_id] = replace(result)
    return result
