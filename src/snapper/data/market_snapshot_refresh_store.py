"""Fenced atomic selected snapshot batches with exact retained-version proof.

The fence is acquired on the same session that commits the complete batch.
Strictly newer receive times refuse a delayed older writer after a newer batch.
An exception or lost commit acknowledgement is never success, even if ordinary
freshness reads find older usable rows. The supervisor owns the fsynced intent,
whole-container cleanup, and any later readback; this store never retries or
claims that client cancellation proves server rollback. No schema is added.
"""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from uuid import UUID

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotBatch
from snapper.core.market_snapshot_refresh_scope import FrozenRefreshScope
from snapper.data.market_snapshot_refresh_rows import insert_refresh_batch
from snapper.data.market_snapshot_refresh_rows import revalidate_refresh_aliases
from snapper.data.market_snapshot_refresh_rows import revalidate_refresh_scope
from snapper.data.market_snapshot_refresh_rows import validate_refresh_batch
from snapper.data.market_snapshot_refresh_rows import validate_refresh_receive_times
from snapper.data.market_snapshot_refresh_rows import validated_refresh_scope
from snapper.data.market_snapshot_refresh_rows import verify_refresh_receipt
from snapper.data.market_snapshot_refresh_transactions import RefreshRepository
from snapper.data.market_snapshot_refresh_transactions import lock_refresh_metadata
from snapper.data.market_snapshot_refresh_transactions import lock_refresh_snapshots
from snapper.data.market_snapshot_refresh_transactions import refresh_transaction


def _utc_now() -> datetime:
    """Read a fresh aware wall clock without changing the monotonic execution budget.

    Returns:
        The current aware UTC instant.
    """
    return datetime.now(UTC)


def _run_identity(run_id: str) -> str:
    """Normalize one explicit producer-session UUID without altering collected operands.

    Args:
        run_id: Explicit producer-session UUID shared by every collected operand.

    Returns:
        The canonical producer-session UUID string.
    """
    if run_id.strip() != run_id:
        raise ValueError("Refresh run identity must be an unpadded UUID")
    return str(UUID(run_id))


def validate_refresh_provenance(run_id: str, batch: RefreshSnapshotBatch) -> None:
    """Bind every batch member to one producer session and distinct positive sequence.

    Args:
        run_id: Explicit producer-session UUID shared by every collected operand.
        batch: Exact collected operands with original receive times and selected alias proofs.
    """
    if any(row.session_id != run_id or row.sequence_id <= 0 for row in batch.snapshots):
        raise ValueError("Refresh batch must belong to the exact run producer session")
    sequences = [row.sequence_id for row in batch.snapshots]
    if len(set(sequences)) != len(sequences):
        raise ValueError("Refresh batch must contain distinct producer sequence identities")


class MarketSnapshotRefreshStore:
    """Persist and verify exact scope-bound batches under the dedicated transaction fence."""

    def __init__(
        self, repository: RefreshRepository, clock: Callable[[], datetime] = _utc_now
    ) -> None:
        """Bind a session provider and aware clock without creating or renewing a deadline.

        Args:
            repository: Database dialect and fresh-session provider owned by the caller.
            clock: Injected aware wall-clock supplier, independent of the monotonic budget.
        """
        self._repository = repository
        self._clock = clock

    def _now(self) -> datetime:
        """Reject an ambiguous wall clock instead of silently attaching a timezone.

        Returns:
            The observed wall-clock instant normalized to UTC.
        """
        value = self._clock()
        if value.utcoffset() is None:
            raise ValueError("Refresh store clock must be timezone-aware")
        return value.astimezone(UTC)

    async def persist(
        self,
        run_id: str,
        scope: FrozenRefreshScope,
        batch: RefreshSnapshotBatch,
        budget: RefreshBudget,
    ) -> RefreshCommitReceipt:
        """Acknowledge one exact complete atomic write; never infer success after lost acknowledgement.

        Args:
            run_id: Explicit producer-session UUID shared by every collected operand.
            scope: Frozen requested identities, configured FX venues and complete resolved metadata.
            batch: Exact collected operands with original receive times and selected alias proofs.
            budget: The existing absolute monotonic deadline, never renewed by this operation.

        Returns:
            The complete exact version receipt only after commit acknowledgment.
        """
        identity = _run_identity(run_id)
        frozen = validated_refresh_scope(scope)
        validate_refresh_batch(frozen, batch)
        validate_refresh_provenance(identity, batch)
        validate_refresh_receive_times(batch, self._now())
        async with refresh_transaction(self._repository, budget) as session:
            await lock_refresh_metadata(session, self._repository.dialect_name)
            await lock_refresh_snapshots(session, self._repository.dialect_name)
            await revalidate_refresh_scope(session, frozen)
            await revalidate_refresh_aliases(session, frozen, batch.alias_proofs, budget)
            snapshots = await insert_refresh_batch(session, batch)
            recorded_at = self._now()
            validate_refresh_receive_times(batch, recorded_at)
            receipt = RefreshCommitReceipt(
                run_id=identity,
                scope_digest=frozen.scope_digest,
                batch_digest=batch.batch_digest,
                collection_started_at=batch.collection_started_at,
                alias_proofs=batch.alias_proofs,
                recorded_at=recorded_at,
                snapshots=snapshots,
            )
            budget.remaining()
            await session.commit()
            return receipt

    async def verify(
        self, scope: FrozenRefreshScope, receipt: RefreshCommitReceipt, budget: RefreshBudget
    ) -> RefreshCommitReceipt:
        """Prove retained versions under a fresh fence without modifying any market data.

        Args:
            scope: Frozen requested identities, configured FX venues and complete resolved metadata.
            receipt: Acknowledged exact version identities, provenance and operand fingerprints.
            budget: The existing absolute monotonic deadline, never renewed by this operation.

        Returns:
            The unchanged receipt after exact retained-version and metadata proof.
        """
        frozen = validated_refresh_scope(scope)
        identity = _run_identity(receipt.run_id)
        if receipt.recorded_at.utcoffset() is None or receipt.recorded_at > self._now():
            raise ValueError("Refresh receipt time must be aware and no later than verification")
        async with refresh_transaction(self._repository, budget) as session:
            await lock_refresh_metadata(session, self._repository.dialect_name)
            await revalidate_refresh_scope(session, frozen)
            await revalidate_refresh_aliases(session, frozen, receipt.alias_proofs, budget)
            batch = await verify_refresh_receipt(session, frozen, receipt)
            validate_refresh_provenance(identity, batch)
            validate_refresh_receive_times(batch, receipt.recorded_at)
            if receipt.recorded_at > self._now():
                raise ValueError("Refresh verification clock moved before the receipt")
            return receipt
