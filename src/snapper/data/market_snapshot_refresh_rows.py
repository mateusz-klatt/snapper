"""Validate exact scopes and retained snapshot versions inside the caller's fence."""

import asyncio
from collections.abc import Sequence
from datetime import datetime
from uuid import uuid7

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.market_snapshot_freshness import STALENESS_THRESHOLD_SECONDS
from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshot
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotBatch
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotReceipt
from snapper.core.market_snapshot_refresh_scope import FrozenRefreshScope
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import MarketSnapshot
from snapper.data.repository import _market_snapshot_active_rows_query
from snapper.data.repository import _market_snapshot_refresh_target_query
from snapper.data.repository import _market_snapshot_refresh_target_rows
from snapper.data.selected_market_snapshot_aliases import read_selected_kraken_alias_proofs


def validated_refresh_scope(scope: FrozenRefreshScope) -> FrozenRefreshScope:
    """Revalidate explicit scope classification and require usable configured FX targets.

    Args:
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.

    Returns:
        The consistently reconstructed scope with all configured FX dependencies resolved.
    """
    result = freeze_refresh_scope(
        scope.requested_instrument_public_ids,
        scope.configured_fx_exchanges,
        [row.metadata() for row in (*scope.required, *scope.excluded)],
    )
    if result.scope_digest != scope.scope_digest:
        raise ValueError("Refresh scope classification is inconsistent")
    if result.unresolved_fx_currencies:
        raise ValueError("Refresh scope has unresolved configured FX currencies")
    return result


async def revalidate_refresh_scope(session: AsyncSession, scope: FrozenRefreshScope) -> None:
    """Bind the full required and excluded closure to metadata frozen in this transaction.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.
    """
    result = await session.execute(
        _market_snapshot_refresh_target_query(list(scope.requested_instrument_public_ids))
    )
    observed = freeze_refresh_scope(
        scope.requested_instrument_public_ids,
        scope.configured_fx_exchanges,
        _market_snapshot_refresh_target_rows(result.all()),
    )
    if observed.scope_digest != scope.scope_digest:
        raise ValueError("Refresh metadata changed after scope resolution")


def validate_refresh_batch(scope: FrozenRefreshScope, batch: RefreshSnapshotBatch) -> None:
    """Require a complete one-per-identity set with the exact frozen scope digest.

    Args:
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.
        batch: Exact collected operands with original receive times and selected alias proofs.
    """
    identities = tuple(sorted(row.instrument_public_id for row in batch.snapshots))
    if batch.scope_digest != scope.scope_digest or identities != scope.required_instrument_ids:
        raise ValueError("Refresh batch must cover the exact frozen required scope once")
    for row in batch.snapshots:
        RefreshSnapshot.model_validate_json(row.model_dump_json())
    validate_refresh_alias_proofs(scope, batch.alias_proofs)


def validate_refresh_alias_proofs(
    scope: FrozenRefreshScope, proofs: tuple[RefreshAliasProof, ...]
) -> tuple[str, ...]:
    """Require exactly one bounded version proof for each selected Kraken instrument.

    Args:
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.
        proofs: Original selected Kraken wire alias identities and physical version proofs.

    Returns:
        The exact sorted Kraken instrument identities that require alias revalidation.
    """
    expected = tuple(
        sorted(row.instrument_public_id for row in scope.required if row.exchange == "kraken")
    )
    if tuple(sorted(proof.instrument_public_id for proof in proofs)) != expected:
        raise ValueError("Refresh alias proofs must cover every selected Kraken instrument once")
    for proof in proofs:
        RefreshAliasProof.model_validate_json(proof.model_dump_json())
    return expected


async def revalidate_refresh_aliases(
    session: AsyncSession,
    scope: FrozenRefreshScope,
    proofs: tuple[RefreshAliasProof, ...],
    budget: RefreshBudget,
) -> None:
    """Compare the exact selected alias versions under the caller's metadata lock.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.
        proofs: Original selected Kraken wire alias identities and physical version proofs.
        budget: The existing absolute monotonic deadline, never renewed by this operation.
    """
    expected = validate_refresh_alias_proofs(scope, proofs)
    if not expected:
        return
    deadline = asyncio.get_running_loop().time() + budget.remaining()
    actual = await read_selected_kraken_alias_proofs(session, expected, deadline)
    if actual != tuple(sorted(proofs, key=lambda proof: proof.instrument_public_id)):
        raise ValueError("Refresh wire alias identity changed after collection selection")


def validate_refresh_receive_times(batch: RefreshSnapshotBatch, now: datetime) -> None:
    """Require original receive times inside this collection's actual wall-clock interval.

    Args:
        batch: Exact collected operands with original receive times and selected alias proofs.
        now: Fresh aware UTC time at the current validation boundary.
    """
    if any(not batch.collection_started_at <= row.timestamp <= now for row in batch.snapshots):
        raise ValueError("Refresh receive times must fall within the original collection interval")
    if any(
        (now - row.timestamp).total_seconds() > STALENESS_THRESHOLD_SECONDS
        for row in batch.snapshots
    ):
        raise ValueError("Refresh batch includes stale receive timestamps")


def snapshot_operand(row: MarketSnapshot) -> RefreshSnapshot:
    """Read only the exact collection operands, excluding SCD2 closure bookkeeping.

    Args:
        row: Persisted snapshot version whose original collection operands must be preserved.

    Returns:
        Validated original collection operands without SCD2 bookkeeping fields.
    """
    if row.last_price is None:
        raise ValueError("Refresh receipt retained version has no last price")
    return RefreshSnapshot(
        instrument_public_id=row.instrument_public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        last_price=row.last_price,
        bid=row.bid,
        bid_volume=row.bid_volume,
        ask=row.ask,
        ask_volume=row.ask_volume,
        volume_24h=row.volume_24h,
        vwap_24h=row.vwap_24h,
        low_24h=row.low_24h,
        high_24h=row.high_24h,
        change_24h=row.change_24h,
        spread=row.spread,
        spread_pct=row.spread_pct,
    )


async def insert_refresh_batch(
    session: AsyncSession, batch: RefreshSnapshotBatch
) -> tuple[RefreshSnapshotReceipt, ...]:
    """Close and insert every strictly newer member without any intermediate commit.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        batch: Exact collected operands with original receive times and selected alias proofs.

    Returns:
        Exact inserted-version receipts, still inside the caller-owned transaction.
    """
    result = await session.execute(
        _market_snapshot_active_rows_query([row.instrument_public_id for row in batch.snapshots])
    )
    active = {row.instrument_public_id: row for row in result.scalars().all()}
    _require_newer(batch.snapshots, active)
    inserted: list[MarketSnapshot] = []
    for operand in batch.snapshots:
        existing = active.get(operand.instrument_public_id)
        public_id = str(uuid7()) if existing is None else existing.public_id
        if existing is not None:
            existing.known_to = operand.timestamp
        inserted.append(
            MarketSnapshot(**operand.model_dump(), public_id=public_id, known_to=KNOWN_TO_MAX)
        )
    await session.flush()
    session.add_all(inserted)
    await session.flush()
    return tuple(
        RefreshSnapshotReceipt(
            instrument_public_id=row.instrument_public_id,
            row_id=row.id,
            public_id=row.public_id,
            session_id=row.session_id,
            sequence_id=row.sequence_id,
            timestamp=row.timestamp,
            payload_digest=snapshot_operand(row).payload_digest,
        )
        for row in sorted(inserted, key=lambda row: row.instrument_public_id)
    )


def _require_newer(snapshots: Sequence[RefreshSnapshot], active: dict[str, MarketSnapshot]) -> None:
    """Refuse equal and out-of-order receive timestamps before closing any member.

    Args:
        snapshots: The complete collected write operands to compare with current versions.
        active: Exact open-ended snapshot versions indexed by instrument identity.
    """
    if any(
        row.instrument_public_id in active
        and row.timestamp <= active[row.instrument_public_id].timestamp
        for row in snapshots
    ):
        raise ValueError("Refresh snapshots must be strictly newer than every active version")


async def verify_refresh_receipt(
    session: AsyncSession, scope: FrozenRefreshScope, receipt: RefreshCommitReceipt
) -> RefreshSnapshotBatch:
    """Prove all exact retained rows and operands, even after later ordinary updates.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        scope: Frozen requested identities, configured FX venues and complete resolved metadata.
        receipt: Acknowledged exact version identities, provenance and operand fingerprints.

    Returns:
        The original batch reconstructed from proven retained snapshot versions.
    """
    identities = tuple(sorted(row.instrument_public_id for row in receipt.snapshots))
    row_ids = [row.row_id for row in receipt.snapshots]
    if (
        identities != scope.required_instrument_ids
        or receipt.scope_digest != scope.scope_digest
        or len(set(row_ids)) != len(row_ids)
    ):
        raise ValueError("Refresh receipt must cover the exact bounded scope and distinct versions")
    result = await session.execute(select(MarketSnapshot).where(MarketSnapshot.id.in_(row_ids)))
    rows = {row.id: row for row in result.scalars().all()}
    if len(rows) != len(receipt.snapshots):
        raise ValueError("Refresh receipt is missing a retained snapshot version")
    operands = tuple(_receipt_operand(rows[item.row_id], item) for item in receipt.snapshots)
    batch = RefreshSnapshotBatch(
        scope_digest=receipt.scope_digest,
        collection_started_at=receipt.collection_started_at,
        alias_proofs=receipt.alias_proofs,
        snapshots=operands,
    )
    validate_refresh_batch(scope, batch)
    if batch.batch_digest != receipt.batch_digest:
        raise ValueError("Refresh receipt batch digest is inconsistent")
    return batch


def _receipt_operand(row: MarketSnapshot, receipt: RefreshSnapshotReceipt) -> RefreshSnapshot:
    """Require physical and logical identity plus every original collection operand.

    Args:
        row: Persisted snapshot version whose original collection operands must be preserved.
        receipt: Acknowledged exact version identities, provenance and operand fingerprints.

    Returns:
        The retained original operand after every receipt binding has been verified.
    """
    operand = snapshot_operand(row)
    if (
        row.public_id != receipt.public_id
        or row.instrument_public_id != receipt.instrument_public_id
        or row.session_id != receipt.session_id
        or row.sequence_id != receipt.sequence_id
        or row.timestamp != receipt.timestamp
        or operand.payload_digest != receipt.payload_digest
    ):
        raise ValueError("Refresh receipt retained version is inconsistent")
    return operand
