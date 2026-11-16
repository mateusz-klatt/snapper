"""Port-driven selected refresh work, without deployment or financial authority.

The caller supplies a work portion of an existing absolute deadline. Disconnect
must finish before durable intent and persistence. The producer identity belongs
to the newly created SequenceTracker, independently of the host invocation UUID.
A returned exact receipt is not whole-job success: independent container teardown
and the final freshness API observation remain the host coordinator's duties.
"""

import asyncio
import re
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Protocol
from uuid import UUID

from snapper.core.market_snapshot_freshness import STALENESS_THRESHOLD_SECONDS
from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotBatch
from snapper.core.market_snapshot_refresh_intent import RefreshIntentSnapshot
from snapper.core.market_snapshot_refresh_intent import RefreshWriteIntent
from snapper.core.market_snapshot_refresh_scope import FrozenRefreshScope
from snapper.core.market_snapshot_refresh_scope import RefreshTargetMetadata
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.core.market_snapshot_refresh_scope import normalize_refresh_fx_exchanges
from snapper.core.market_snapshot_refresh_scope import normalize_refresh_instrument_ids
from snapper.data.market_snapshot_refresh_rows import snapshot_operand
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.market_data.selected_snapshot_collector import SelectedSnapshotBatch
from snapper.infrastructure.market_data.selected_snapshot_collector import (
    collect_selected_snapshots,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker

COLLECTION_MAX_SECONDS = 10.0
DISCONNECT_RESERVE_SECONDS = 2.0
_IMPLEMENTED_VENUES = frozenset({"kraken"})


class RefreshWorkerError(ValueError):
    """A fixed refusal code without collected prices or external error payloads."""


@dataclass(frozen=True, slots=True)
class RefreshWorkerRequest:
    """Explicit host invocation and reviewed metadata scope, without a producer ID.

    Attributes:
        host_invocation_id: Host-owned UUID distinct from the fresh producer session.
        instrument_public_ids: Explicit main UUIDs, normalized before any metadata read.
        fx_exchanges: Explicit supported FX venues, without a default selection.
        expected_scope_digest: Reviewed canonical metadata digest.
    """

    host_invocation_id: str
    instrument_public_ids: tuple[str, ...]
    fx_exchanges: tuple[str, ...]
    expected_scope_digest: str

    def __post_init__(self) -> None:
        """Validate configuration before resolving metadata or constructing an adapter."""
        if self.host_invocation_id.strip() != self.host_invocation_id:
            raise RefreshWorkerError("invalid_host_invocation")
        try:
            canonical = str(UUID(self.host_invocation_id))
            requested = normalize_refresh_instrument_ids(self.instrument_public_ids)
            configured = normalize_refresh_fx_exchanges(self.fx_exchanges)
        except ValueError:
            raise RefreshWorkerError("invalid_worker_configuration") from None
        object.__setattr__(self, "host_invocation_id", canonical)
        object.__setattr__(self, "instrument_public_ids", requested)
        object.__setattr__(self, "fx_exchanges", configured)
        if re.fullmatch(r"[0-9a-f]{64}", self.expected_scope_digest) is None:
            raise RefreshWorkerError("invalid_scope_digest")


class RefreshMetadataReader(Protocol):
    """Only the explicit current metadata resolver needed by this worker."""

    async def resolve_market_snapshot_refresh_targets(
        self, instrument_public_ids: list[str]
    ) -> list[RefreshTargetMetadata]:
        """Resolve requested instruments and metadata-only FX candidates.

        Args:
            instrument_public_ids: Bounded normalized requested main identities.

        Returns:
            Complete typed metadata for required and excluded candidates.
        """
        ...


class SelectedRefreshAdapter(Protocol):
    """Prepared, initially disconnected client with exact alias evidence."""

    @property
    def alias_proofs(self) -> tuple[RefreshAliasProof, ...]:
        """Return exact current Kraken WebSocket alias admission evidence.

        Returns:
            One proof per required instrument in canonical identity order.
        """
        ...

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Connect and subscribe to only the exact admitted native symbols.

        Args:
            symbols: Explicit frozen venue-native identities.

        Returns:
            A closable asynchronous selected ticker stream.
        """
        ...

    async def disconnect(self, deadline: float) -> None:
        """Disconnect within the caller's absolute event-loop deadline.

        Args:
            deadline: Existing remaining cleanup budget in event-loop time.
        """
        ...


class RefreshAdapterFactory(Protocol):
    """Prepare alias evidence before returning any connected transport."""

    async def __call__(
        self, scope: FrozenRefreshScope, budget: RefreshBudget
    ) -> SelectedRefreshAdapter:
        """Prepare a disconnected adapter bound to the complete frozen scope.

        Args:
            scope: Validated required and excluded current metadata.
            budget: Original remaining work budget, never renewed here.

        Returns:
            A disconnected selected adapter with its admitted alias proofs.
        """
        ...


class RefreshReceiptStore(Protocol):
    """Atomic persistence and exact receipt proof, with no retry or claim lifecycle."""

    async def persist(
        self,
        run_id: str,
        scope: FrozenRefreshScope,
        batch: RefreshSnapshotBatch,
        budget: RefreshBudget,
    ) -> RefreshCommitReceipt:
        """Persist the exact collected batch and acknowledge its commit.

        Args:
            run_id: Fresh SequenceTracker producer session UUID.
            scope: Frozen resolved metadata and unchanged API scope digest.
            batch: Complete collected operands and admitted alias proofs.
            budget: Original work deadline, including commit acknowledgment.

        Returns:
            The exact acknowledged inserted-version receipt.
        """
        ...

    async def verify(
        self, scope: FrozenRefreshScope, receipt: RefreshCommitReceipt, budget: RefreshBudget
    ) -> RefreshCommitReceipt:
        """Prove retained versions and return the unchanged exact receipt.

        Args:
            scope: Originally frozen metadata scope.
            receipt: Exact acknowledged versions, never inferred from generic freshness.
            budget: Remaining original work deadline.

        Returns:
            The same receipt after exact version verification.
        """
        ...


class RefreshIntentWriter(Protocol):
    """Durably write and fsync intent, or raise before a database write begins."""

    async def __call__(self, intent: RefreshWriteIntent, budget: RefreshBudget) -> None:
        """Acknowledge durable intent without ever receiving clear price operands.

        Args:
            intent: Exact producer, scope, time, metadata and payload-hash evidence.
            budget: Remaining original work budget for the durable write.
        """
        ...


class RefreshCommitWriter(Protocol):
    """Durably retain an acknowledged commit before independent exact verification."""

    async def __call__(self, receipt: RefreshCommitReceipt, budget: RefreshBudget) -> None:
        """Record acknowledged versions without declaring final job readiness.

        Args:
            receipt: Exact intent-bound acknowledgment returned by persistence.
            budget: Remaining original work deadline, without a journal extension.
        """
        ...


def _utc_now() -> datetime:
    """Read an aware UTC clock without altering the execution deadline.

    Returns:
        The current UTC instant.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RefreshWorkerDependencies:
    """Explicit side-effect ports, all owned and configured by the outer worker host."""

    metadata_reader: RefreshMetadataReader
    adapter_factory: RefreshAdapterFactory
    store: RefreshReceiptStore
    intent_writer: RefreshIntentWriter
    commit_writer: RefreshCommitWriter
    utc_now: Callable[[], datetime] = _utc_now


def _check_cancellation(task: asyncio.Task[object], baseline: int) -> None:
    """Propagate new caller cancellation even if a cooperative port swallowed the exception.

    Args:
        task: Owning task after its local timeout context has removed its own request.
        baseline: Cancellation count already present before entering the operation.
    """
    if task.cancelling() > baseline:
        raise asyncio.CancelledError("Refresh worker cancelled")


async def _within_budget[ResultT](
    operation: Callable[[], Awaitable[ResultT]], budget: RefreshBudget, reserve: float = 0.0
) -> ResultT:
    """Await one port under the remaining original budget without resetting it.

    Args:
        operation: Factory invoked only after remaining budget is validated.
        budget: Shared absolute deadline.
        reserve: Cleanup time preserved inside the same deadline.

    Returns:
        The port result, only while budget and caller cancellation remain valid.
    """
    remaining = budget.remaining(reserve=reserve)
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Refresh worker requires an owning task")
    baseline = task.cancelling()
    try:
        async with asyncio.timeout(remaining):
            result = await operation()
    except BaseException:
        _check_cancellation(task, baseline)
        raise
    _check_cancellation(task, baseline)
    budget.remaining(reserve=reserve)
    return result


def _freeze_scope(
    request: RefreshWorkerRequest, metadata: list[RefreshTargetMetadata]
) -> FrozenRefreshScope:
    """Refuse drift, missing FX dependencies and unimplemented venues before client creation.

    Args:
        request: Validated host invocation and reviewed explicit configuration.
        metadata: Current metadata returned by the bounded resolver.

    Returns:
        The complete admitted Kraken-only required scope.
    """
    scope = freeze_refresh_scope(request.instrument_public_ids, request.fx_exchanges, metadata)
    if scope.scope_digest != request.expected_scope_digest:
        raise RefreshWorkerError("scope_digest_mismatch")
    if scope.unresolved_fx_currencies:
        raise RefreshWorkerError("unresolved_fx_currencies")
    if any(target.exchange not in _IMPLEMENTED_VENUES for target in scope.required):
        raise RefreshWorkerError("unsupported_refresh_venue")
    return scope


def _aware_clock(clock: Callable[[], datetime]) -> datetime:
    """Preserve an actual aware receipt clock without inventing a missing timezone.

    Args:
        clock: Injected UTC wall-clock port.

    Returns:
        The original instant normalized to UTC.
    """
    now = clock()
    if now.utcoffset() is None:
        raise RefreshWorkerError("invalid_receive_clock")
    return now.astimezone(UTC)


async def _disconnect(adapter: SelectedRefreshAdapter, budget: RefreshBudget) -> None:
    """Attempt bounded disconnect even when a prior operation consumed the budget.

    Args:
        adapter: Owned prepared client, whether or not subscription connected it.
        budget: The same remaining work deadline.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Refresh disconnect requires an owning task")
    baseline = task.cancelling()
    try:
        remaining = budget.timeout_seconds(DISCONNECT_RESERVE_SECONDS)
    except TimeoutError:
        remaining = 0.0
    deadline = loop.time() + remaining
    try:
        async with asyncio.timeout_at(deadline):
            await adapter.disconnect(deadline)
    except BaseException:
        _check_cancellation(task, baseline)
        raise
    _check_cancellation(task, baseline)
    if loop.time() >= deadline:
        raise TimeoutError("Refresh disconnect deadline exhausted")
    budget.remaining()


async def _collect(
    scope: FrozenRefreshScope,
    adapter: SelectedRefreshAdapter,
    budget: RefreshBudget,
    clock: Callable[[], datetime],
) -> tuple[str, RefreshSnapshotBatch]:
    """Collect one exact batch and finish disconnect before returning any write operands.

    Args:
        scope: Frozen metadata admitted before adapter preparation.
        adapter: Disconnected adapter with exact admitted WebSocket alias proofs.
        budget: Caller-owned remaining work deadline.
        clock: Original UTC observation clock.

    Returns:
        Fresh producer session identity and its complete typed batch.
    """
    try:
        proofs = adapter.alias_proofs
        if (
            tuple(sorted(proof.instrument_public_id for proof in proofs))
            != scope.required_instrument_ids
        ):
            raise RefreshWorkerError("incomplete_alias_proof")
        tracker = SequenceTracker()
        started = _aware_clock(clock)
        deadline = asyncio.get_running_loop().time() + budget.timeout_seconds(
            COLLECTION_MAX_SECONDS, reserve=DISCONNECT_RESERVE_SECONDS
        )
        targets = [MarketSnapshotRefreshTargetRow(**target.metadata()) for target in scope.required]
        collected = await collect_selected_snapshots(
            adapter,
            targets,
            deadline,
            tracker,
            received_at=lambda: _aware_clock(clock),
        )
    except BaseException:
        with suppress(Exception):
            await _disconnect(adapter, budget)
        raise
    await _disconnect(adapter, budget)
    batch = _collected_batch(scope, collected, started, proofs)
    _validate_producer_batch(tracker.session_id, batch, _aware_clock(clock))
    return tracker.session_id, batch


def _collected_batch(
    scope: FrozenRefreshScope,
    collected: SelectedSnapshotBatch,
    started: datetime,
    proofs: tuple[RefreshAliasProof, ...],
) -> RefreshSnapshotBatch:
    """Refuse partial or timed-out collection without consulting older database rows.

    Args:
        scope: Exact required identities and metadata digest.
        collected: Real selected collector result.
        started: Original collection start clock.
        proofs: Alias evidence admitted before subscription.

    Returns:
        Complete typed write operands with unchanged original timestamps.
    """
    identities = tuple(sorted(row.instrument_public_id for row in collected.snapshots))
    if (
        collected.timed_out
        or collected.missing_instrument_ids
        or identities != scope.required_instrument_ids
    ):
        raise RefreshWorkerError("incomplete_collection")
    try:
        return RefreshSnapshotBatch(
            scope_digest=scope.scope_digest,
            collection_started_at=started,
            alias_proofs=proofs,
            snapshots=tuple(snapshot_operand(row) for row in collected.snapshots),
        )
    except ValueError:
        raise RefreshWorkerError("invalid_collected_snapshot") from None


def _validate_producer_batch(run_id: str, batch: RefreshSnapshotBatch, now: datetime) -> None:
    """Check provenance and original receive interval before durable write intent.

    Args:
        run_id: Fresh SequenceTracker session UUID.
        batch: Complete immutable write operands.
        now: Fresh post-disconnect UTC observation.
    """
    sequences = [row.sequence_id for row in batch.snapshots]
    if len(set(sequences)) != len(sequences) or any(
        row.session_id != run_id or row.sequence_id <= 0 for row in batch.snapshots
    ):
        raise RefreshWorkerError("invalid_producer_provenance")
    if any(
        not batch.collection_started_at <= row.timestamp <= now
        or (now - row.timestamp).total_seconds() > STALENESS_THRESHOLD_SECONDS
        for row in batch.snapshots
    ):
        raise RefreshWorkerError("invalid_collection_interval")


def _intent(
    request: RefreshWorkerRequest,
    scope: FrozenRefreshScope,
    run_id: str,
    batch: RefreshSnapshotBatch,
) -> RefreshWriteIntent:
    """Project only identities, provenance, metadata and operand hashes into durable intent.

    Args:
        request: Host invocation identity, independent of producer session.
        scope: Frozen required and excluded metadata.
        run_id: Fresh producer UUID.
        batch: Complete write operands whose prices remain outside this projection.

    Returns:
        Immutable price-free intent for a durable fsync acknowledgment.
    """
    snapshots = tuple(
        RefreshIntentSnapshot(
            row.instrument_public_id,
            row.session_id,
            row.sequence_id,
            row.timestamp,
            row.payload_digest,
        )
        for row in batch.snapshots
    )
    return RefreshWriteIntent(
        host_invocation_id=request.host_invocation_id,
        run_id=run_id,
        scope_digest=scope.scope_digest,
        batch_digest=batch.batch_digest,
        collection_started_at=batch.collection_started_at,
        requested_instrument_public_ids=scope.requested_instrument_public_ids,
        configured_fx_exchanges=scope.configured_fx_exchanges,
        required=scope.required,
        excluded=scope.excluded,
        alias_proofs=batch.alias_proofs,
        snapshots=snapshots,
    )


def _validate_acknowledgment(
    run_id: str, batch: RefreshSnapshotBatch, receipt: RefreshCommitReceipt
) -> None:
    """Reject acknowledgments that do not bind the exact prepared write batch.

    Args:
        run_id: Actual producer session UUID.
        batch: Exact prepared operands and alias evidence.
        receipt: Store's acknowledged retained-version identities.
    """
    expected = {
        row.instrument_public_id: (
            row.payload_digest,
            row.session_id,
            row.sequence_id,
            row.timestamp,
        )
        for row in batch.snapshots
    }
    actual = {
        row.instrument_public_id: (
            row.payload_digest,
            row.session_id,
            row.sequence_id,
            row.timestamp,
        )
        for row in receipt.snapshots
    }
    if (
        receipt.run_id != run_id
        or receipt.scope_digest != batch.scope_digest
        or receipt.batch_digest != batch.batch_digest
        or receipt.collection_started_at != batch.collection_started_at
        or receipt.alias_proofs != batch.alias_proofs
        or len(actual) != len(receipt.snapshots)
        or actual != expected
    ):
        raise RefreshWorkerError("inconsistent_commit_receipt")


async def run_market_snapshot_refresh_worker(
    request: RefreshWorkerRequest, dependencies: RefreshWorkerDependencies, budget: RefreshBudget
) -> RefreshCommitReceipt:
    """Run the selected data work without declaring container or financial readiness.

    Args:
        request: Validated explicit invocation and reviewed metadata scope.
        dependencies: Read, adapter, durable-intent, store and UTC-clock ports.
        budget: Previously established work portion of one outer absolute deadline.

    Returns:
        An exact acknowledged and reverified receipt, without a whole-job success claim.
    """
    metadata = await _within_budget(
        lambda: dependencies.metadata_reader.resolve_market_snapshot_refresh_targets(
            list(request.instrument_public_ids)
        ),
        budget,
        DISCONNECT_RESERVE_SECONDS,
    )
    scope = _freeze_scope(request, metadata)
    adapter = await _within_budget(
        lambda: dependencies.adapter_factory(scope, budget), budget, DISCONNECT_RESERVE_SECONDS
    )
    run_id, batch = await _collect(scope, adapter, budget, dependencies.utc_now)
    intent = _intent(request, scope, run_id, batch)
    await _within_budget(lambda: dependencies.intent_writer(intent, budget), budget)
    receipt = await _within_budget(
        lambda: dependencies.store.persist(run_id, scope, batch, budget), budget
    )
    _validate_acknowledgment(run_id, batch, receipt)
    await _within_budget(lambda: dependencies.commit_writer(receipt, budget), budget)
    verified = await _within_budget(
        lambda: dependencies.store.verify(scope, receipt, budget), budget
    )
    if verified != receipt:
        raise RefreshWorkerError("verification_receipt_changed")
    return receipt
