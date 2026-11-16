"""Port-driven worker witnesses with the real collector and no database or venue calls."""

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import UUID

import pytest

from snapper.application.market_snapshot_refresh import worker
from snapper.application.market_snapshot_refresh.worker import RefreshWorkerDependencies
from snapper.application.market_snapshot_refresh.worker import RefreshWorkerError
from snapper.application.market_snapshot_refresh.worker import RefreshWorkerRequest
from snapper.application.market_snapshot_refresh.worker import SelectedRefreshAdapter
from snapper.application.market_snapshot_refresh.worker import run_market_snapshot_refresh_worker
from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotBatch
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotReceipt
from snapper.core.market_snapshot_refresh_intent import RefreshWriteIntent
from snapper.core.market_snapshot_refresh_scope import FrozenRefreshScope
from snapper.core.market_snapshot_refresh_scope import RefreshTargetMetadata
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.infrastructure.exchanges.contracts import TickerUpdate

_MAIN = "feed0000-1234-4567-89ab-0123456789ab"
_FX = "feed0000-1234-4567-89ab-0123456789ac"
_HOST = "feed0000-1234-4567-89ab-0123456789ad"
_NOW = datetime(2026, 11, 16, 21, tzinfo=UTC)


def _metadata() -> list[RefreshTargetMetadata]:
    """Build the two exact synthetic main and configured FX instruments.

    Returns:
        Kraken BTC-EUR and EUR-USD metadata, without database access.
    """
    return [
        RefreshTargetMetadata(
            instrument_public_id=_MAIN,
            native_symbol="BTC-EUR",
            exchange="kraken",
            base="BTC",
            quote="EUR",
            asset_type="crypto",
        ),
        RefreshTargetMetadata(
            instrument_public_id=_FX,
            native_symbol="EUR-USD",
            exchange="kraken",
            base="EUR",
            quote="USD",
            asset_type="forex",
        ),
    ]


def _proofs() -> tuple[RefreshAliasProof, ...]:
    """Build exact synthetic admitted alias versions for both required identities.

    Returns:
        Canonically ordered metadata-only WebSocket alias proofs.
    """
    return tuple(
        RefreshAliasProof(
            instrument_public_id=row["instrument_public_id"],
            symbol_public_id=row["instrument_public_id"],
            alias_public_id=row["instrument_public_id"],
            alias_row_id=index,
            exchange="kraken",
            channel="ws",
            exchange_symbol=row["native_symbol"],
        )
        for index, row in enumerate(_metadata(), 1)
    )


def _tick(symbol: str, last: float = 2.0) -> TickerUpdate:
    """Build one original synthetic venue tick.

    Args:
        symbol: Exact selected or deliberately unrelated native identity.
        last: Last-price evidence, including deliberate invalid input.

    Returns:
        Complete typed ticker input for the real collector.
    """
    return TickerUpdate(
        symbol=symbol,
        bid=1.0,
        bid_qty=2.0,
        ask=3.0,
        ask_qty=2.0,
        last=last,
        volume=4.0,
        vwap=2.0,
        low=1.0,
        high=3.0,
        change=0.1,
        change_pct=0.2,
    )


def _ticks() -> list[TickerUpdate]:
    """Provide one valid update per required identity.

    Returns:
        Main and FX ticks in deterministic order.
    """
    return [_tick("BTC-EUR"), _tick("EUR-USD")]


@dataclass
class _Ports:
    """Typed in-memory ports with explicit ordering, failure and cancellation controls."""

    metadata: list[RefreshTargetMetadata] = field(default_factory=_metadata)
    ticks: list[TickerUpdate] = field(default_factory=_ticks)
    proofs: tuple[RefreshAliasProof, ...] = field(default_factory=_proofs)
    events: list[str] = field(default_factory=list)
    subscriptions: list[list[str]] = field(default_factory=list)
    intents: list[RefreshWriteIntent] = field(default_factory=list)
    commits: list[RefreshCommitReceipt] = field(default_factory=list)
    batches: list[RefreshSnapshotBatch] = field(default_factory=list)
    producer_ids: list[str] = field(default_factory=list)
    observed_budgets: list[RefreshBudget] = field(default_factory=list)
    hook: Callable[[str], None] | None = None
    blocked_phase: str | None = None
    swallowed_cancel_phase: str | None = None
    disconnect_error: Exception | None = None
    intent_error: Exception | None = None
    commit_error: Exception | None = None
    persist_error: Exception | None = None
    verify_error: Exception | None = None
    collection_error: Exception | None = None
    receipt_transform: Callable[[RefreshCommitReceipt], RefreshCommitReceipt] | None = None
    verification_transform: Callable[[RefreshCommitReceipt], RefreshCommitReceipt] | None = None
    wait_after_ticks: bool = False
    waiting: asyncio.Event = field(default_factory=asyncio.Event)
    unblock: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def alias_proofs(self) -> tuple[RefreshAliasProof, ...]:
        """Return prepared alias admission evidence.

        Returns:
            Exactly the controlled metadata-only alias proofs.
        """
        return self.proofs

    async def _phase(self, name: str) -> None:
        """Record a phase and execute its adversarial scheduling control.

        Args:
            name: Current externally observable orchestration phase.
        """
        self.events.append(name)
        if self.hook is not None:
            self.hook(name)
        if self.blocked_phase == name:
            self.waiting.set()
            await self.unblock.wait()
        if self.swallowed_cancel_phase == name:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.sleep(0)

    async def resolve_market_snapshot_refresh_targets(
        self, instrument_public_ids: list[str]
    ) -> list[RefreshTargetMetadata]:
        """Resolve only the supplied explicit main identity.

        Args:
            instrument_public_ids: Canonical requested main identities.

        Returns:
            Controlled complete metadata without any snapshot freshness lookup.
        """
        assert instrument_public_ids == [_MAIN]
        await self._phase("metadata")
        return self.metadata

    async def prepare(
        self, scope: FrozenRefreshScope, budget: RefreshBudget
    ) -> SelectedRefreshAdapter:
        """Return a disconnected adapter after alias preparation.

        Args:
            scope: Complete admitted scope.
            budget: The original worker budget object.

        Returns:
            This in-memory selected adapter.
        """
        assert scope.required_instrument_ids == (_MAIN, _FX)
        self.observed_budgets.append(budget)
        await self._phase("prepare")
        return self

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Record the actual real collector's subscription scope.

        Args:
            symbols: Exact selected native symbols.

        Returns:
            A closable asynchronous stream of original ticks.
        """
        self.subscriptions.append(symbols)
        return self._stream()

    async def _stream(self) -> AsyncGenerator[TickerUpdate]:
        """Yield original ticks and expose stream closure without connecting a venue.

        Yields:
            Controlled original ticker inputs.
        """
        try:
            await self._phase("subscribe")
            for tick in self.ticks:
                yield tick
            if self.collection_error is not None:
                raise self.collection_error
            if self.wait_after_ticks:
                self.waiting.set()
                await self.unblock.wait()
        finally:
            self.events.append("stream_closed")

    async def disconnect(self, deadline: float) -> None:
        """Acknowledge disconnect only after its controlled phase completes.

        Args:
            deadline: Absolute event-loop cleanup deadline supplied by the worker.
        """
        assert isinstance(deadline, float)
        await self._phase("disconnect")
        if self.disconnect_error is not None:
            raise self.disconnect_error

    async def write_intent(self, intent: RefreshWriteIntent, budget: RefreshBudget) -> None:
        """Capture the price-free intent or simulate a durable fsync failure.

        Args:
            intent: Immutable provenance and operand hashes.
            budget: Original work deadline object.
        """
        self.observed_budgets.append(budget)
        self.intents.append(intent)
        await self._phase("intent")
        if self.intent_error is not None:
            raise self.intent_error

    async def persist(
        self,
        run_id: str,
        scope: FrozenRefreshScope,
        batch: RefreshSnapshotBatch,
        budget: RefreshBudget,
    ) -> RefreshCommitReceipt:
        """Simulate one acknowledged exact commit without writing a database.

        Args:
            run_id: Actual newly generated producer session.
            scope: Original frozen metadata scope.
            batch: Exact complete operands passed after durable intent.
            budget: Original work deadline object.

        Returns:
            Synthetic exact retained-version receipt or the configured mismatch.
        """
        self.observed_budgets.append(budget)
        self.batches.append(batch)
        self.producer_ids.append(run_id)
        await self._phase("persist")
        if self.persist_error is not None:
            raise self.persist_error
        receipt = RefreshCommitReceipt(
            run_id=run_id,
            scope_digest=scope.scope_digest,
            batch_digest=batch.batch_digest,
            collection_started_at=batch.collection_started_at,
            recorded_at=_NOW,
            alias_proofs=batch.alias_proofs,
            snapshots=tuple(
                RefreshSnapshotReceipt(
                    instrument_public_id=row.instrument_public_id,
                    row_id=index,
                    public_id=row.instrument_public_id,
                    payload_digest=row.payload_digest,
                    session_id=row.session_id,
                    sequence_id=row.sequence_id,
                    timestamp=row.timestamp,
                )
                for index, row in enumerate(batch.snapshots, 1)
            ),
        )
        return receipt if self.receipt_transform is None else self.receipt_transform(receipt)

    async def write_commit(self, receipt: RefreshCommitReceipt, budget: RefreshBudget) -> None:
        """Retain an acknowledged exact receipt before independent verification.

        Args:
            receipt: Original acknowledged version evidence, without clear prices.
            budget: Original work deadline object.
        """
        self.observed_budgets.append(budget)
        await self._phase("commit_receipt")
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(receipt)

    async def verify(
        self, scope: FrozenRefreshScope, receipt: RefreshCommitReceipt, budget: RefreshBudget
    ) -> RefreshCommitReceipt:
        """Simulate exact retained-version verification without generic freshness fallback.

        Args:
            scope: Frozen scope supplied to the preceding persist call.
            receipt: Exact acknowledged versions to verify.
            budget: Original work deadline object.

        Returns:
            The unchanged receipt, unless a mismatch is intentionally injected.
        """
        assert scope.scope_digest == receipt.scope_digest
        self.observed_budgets.append(budget)
        await self._phase("verify")
        if self.verify_error is not None:
            raise self.verify_error
        return (
            receipt if self.verification_transform is None else self.verification_transform(receipt)
        )


def _request(ports: _Ports) -> RefreshWorkerRequest:
    """Freeze the operator-reviewed configuration independently of later port activity.

    Args:
        ports: Metadata fixture whose expected scope is reviewed for this case.

    Returns:
        Explicit request with the exact expected metadata digest.
    """
    scope = freeze_refresh_scope((_MAIN,), ("kraken",), ports.metadata)
    return RefreshWorkerRequest(_HOST, (_MAIN,), ("kraken",), scope.scope_digest)


def _dependencies(
    ports: _Ports, clock: Callable[[], datetime] = lambda: _NOW
) -> RefreshWorkerDependencies:
    """Wire the in-memory ports without production constructors.

    Args:
        ports: Observable typed dependencies.
        clock: Original receive-time UTC clock.

    Returns:
        Frozen dependency bundle with no database or live adapter.
    """
    return RefreshWorkerDependencies(
        ports, ports.prepare, ports, ports.write_intent, ports.write_commit, clock
    )


def _budget(seconds: float = 20.0) -> RefreshBudget:
    """Create one absolute worker deadline inside the unchanged pytest timeout policy.

    Args:
        seconds: Work budget supplied by the outer test owner.

    Returns:
        One monotonic deadline shared by every port.
    """
    return RefreshBudget(time.monotonic() + seconds)


async def test_complete_worker_orders_disconnect_intent_commit_and_exact_verification() -> None:
    """Preserve the real collector and all write-admission ordering boundaries.

    Given: Exact current metadata, prepared aliases and one valid tick per identity.
    When: The worker completes its data-only orchestration.
    Then: Disconnect precedes price-free durable intent, one commit and exact verification.
    """
    ports = _Ports()
    budget = _budget()
    receipt = await run_market_snapshot_refresh_worker(
        _request(ports), _dependencies(ports), budget
    )
    assert ports.events == [
        "metadata",
        "prepare",
        "subscribe",
        "stream_closed",
        "disconnect",
        "intent",
        "persist",
        "commit_receipt",
        "verify",
    ]
    assert ports.subscriptions == [["BTC-EUR", "EUR-USD"]]
    assert all(observed is budget for observed in ports.observed_budgets)
    assert receipt.run_id != _HOST
    assert UUID(receipt.run_id).version == 7
    batch = ports.batches[0]
    assert {row.session_id for row in batch.snapshots} == {receipt.run_id}
    assert [row.sequence_id for row in batch.snapshots] == [1, 2]
    assert all(row.timestamp == _NOW for row in batch.snapshots)
    intent = ports.intents[0]
    assert intent.host_invocation_id == _HOST and intent.run_id == receipt.run_id
    assert intent.requested_instrument_public_ids == (_MAIN,)
    assert intent.configured_fx_exchanges == ("kraken",)
    assert intent.alias_proofs == ports.proofs == receipt.alias_proofs
    assert ports.commits == [receipt]
    assert intent.batch_digest == batch.batch_digest == receipt.batch_digest
    encoded = json.dumps(asdict(intent), default=str)
    assert "last_price" not in encoded and '"bid"' not in encoded
    assert {row.payload_digest for row in intent.snapshots} == {
        row.payload_digest for row in batch.snapshots
    }


async def test_invalid_and_unrelated_ticks_can_be_followed_by_valid_selected_evidence() -> None:
    """Preserve the approved collector's ignore-invalid and ignore-unrelated behavior.

    Given: Invalid and unrelated ticks arrive before valid main and FX evidence.
    When: The worker uses the existing collector without a stricter wrapper.
    Then: The complete valid batch is persisted once.
    """
    ports = _Ports(ticks=[_tick("unrelated"), _tick("BTC-EUR", last=0.0), *_ticks()])
    receipt = await run_market_snapshot_refresh_worker(
        _request(ports), _dependencies(ports), _budget()
    )
    assert len(receipt.snapshots) == 2
    assert len(ports.batches) == 1


@pytest.mark.parametrize(
    "ticks", [[], [_tick("BTC-EUR")], [_tick("BTC-EUR", last=float("nan")), _tick("EUR-USD")]]
)
async def test_missing_or_invalid_final_collection_never_uses_old_database_evidence(
    ticks: list[TickerUpdate],
) -> None:
    """Require every original selected observation rather than accepting older fresh rows.

    Given: The stream ends without a usable current tick for every required identity.
    When: The real collector returns partial evidence.
    Then: Disconnect occurs but no durable intent, persistence or verification starts.
    """
    ports = _Ports(ticks=ticks)
    with pytest.raises(RefreshWorkerError, match="^incomplete_collection$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "disconnect"
    assert not ports.intents and not ports.batches


async def test_timed_out_collection_refuses_even_after_a_partial_tick() -> None:
    """Reserve disconnect time within the original work budget.

    Given: One selected tick arrives and the stream waits for the missing FX tick.
    When: The smaller remaining collection budget expires.
    Then: The worker disconnects and refuses without any persistence attempt.
    """
    ports = _Ports(ticks=[_tick("BTC-EUR")], wait_after_ticks=True)
    with pytest.raises(RefreshWorkerError, match="^incomplete_collection$"):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports), _budget(2.1)
        )
    assert ports.events[-1] == "disconnect"
    assert not ports.batches


@pytest.mark.parametrize(
    "case,expected",
    [
        ("digest", "scope_digest_mismatch"),
        ("missing_fx", "unresolved_fx_currencies"),
        ("unsupported", "unsupported_refresh_venue"),
    ],
)
async def test_scope_refusals_precede_adapter_creation(case: str, expected: str) -> None:
    """Reject drift, unresolved FX and unavailable venue implementations before a client exists.

    Given: A reviewed configuration with one deliberately inadmissible scope condition.
    When: Current metadata is resolved.
    Then: The worker refuses before preparing any adapter or writing intent.
    """
    ports = _Ports()
    if case == "missing_fx":
        ports.metadata = ports.metadata[:1]
    elif case == "unsupported":
        ports.metadata[0]["exchange"] = "walutomat"
    request = _request(ports)
    if case == "digest":
        request = replace(request, expected_scope_digest="0" * 64)
    with pytest.raises(RefreshWorkerError, match=f"^{expected}$"):
        await run_market_snapshot_refresh_worker(request, _dependencies(ports), _budget())
    assert ports.events == ["metadata"]


def test_request_validation_normalizes_configuration_and_refuses_bad_input() -> None:
    """Validate raw configuration before a metadata reader can be called.

    Given: Equivalent UUID spellings plus malformed identity, scope and digest variants.
    When: Immutable worker requests are constructed.
    Then: Valid aliases normalize and invalid inputs fail locally.
    """
    request = _request(_Ports())
    normalized = replace(
        request, instrument_public_ids=(_MAIN.upper(), _MAIN), fx_exchanges=("kraken", "kraken")
    )
    assert normalized.instrument_public_ids == (_MAIN,)
    assert normalized.fx_exchanges == ("kraken",)
    with pytest.raises(RefreshWorkerError, match="invalid_host_invocation"):
        replace(request, host_invocation_id=f" {_HOST}")
    with pytest.raises(RefreshWorkerError, match="invalid_worker_configuration"):
        replace(request, instrument_public_ids=("bad-uuid",))
    with pytest.raises(RefreshWorkerError, match="invalid_scope_digest"):
        replace(request, expected_scope_digest="bad-digest")


async def test_incomplete_alias_proof_disconnects_without_subscribing() -> None:
    """Require exact alias admission for every selected instrument.

    Given: The prepared disconnected adapter omits one required alias proof.
    When: The worker admits the collection scope.
    Then: It disconnects without subscribing or creating write intent.
    """
    ports = _Ports(proofs=_proofs()[:1])
    with pytest.raises(RefreshWorkerError, match="^incomplete_alias_proof$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events == ["metadata", "prepare", "disconnect"]


async def test_disconnect_failure_prevents_all_write_paths() -> None:
    """Treat incomplete transport shutdown as a pre-write failure.

    Given: Complete valid evidence but a disconnect operation that fails.
    When: The worker attempts transport cleanup.
    Then: It propagates the failure and never writes durable intent or market data.
    """
    ports = _Ports(disconnect_error=OSError("disconnect_failed"))
    with pytest.raises(OSError, match="disconnect_failed"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert not ports.intents and not ports.batches


async def test_blocked_disconnect_is_bounded_and_prevents_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse a transport that fails to acknowledge disconnect within its remaining allowance.

    Given: Complete evidence and a blocked disconnect port with a tightened test allowance.
    When: The cooperative cleanup deadline expires.
    Then: The worker raises timeout without durable intent or persistence.
    """
    ports = _Ports(blocked_phase="disconnect")
    monkeypatch.setattr(worker, "DISCONNECT_RESERVE_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "disconnect"
    assert not ports.intents and not ports.batches


async def test_expired_disconnect_still_calls_adapter_with_expired_deadline() -> None:
    """Attempt cleanup without allocating fresh time after a prior budget overrun.

    Given: An owned adapter and an already exhausted work deadline.
    When: The disconnect boundary is invoked.
    Then: The adapter receives the attempt, but the worker cannot accept its result.
    """
    ports = _Ports()
    with pytest.raises(TimeoutError):
        await worker._disconnect(ports, RefreshBudget(0.0))
    assert ports.events == ["disconnect"]


async def test_durable_intent_failure_prevents_persistence() -> None:
    """Require successful durable intent acknowledgment before the sole write attempt.

    Given: Complete disconnected evidence and a failing intent fsync callback.
    When: The worker asks its journal port to make intent durable.
    Then: The error propagates without invoking persistence or verification.
    """
    ports = _Ports(intent_error=OSError("fsync_failed"))
    with pytest.raises(OSError, match="fsync_failed"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert len(ports.intents) == 1 and not ports.batches
    assert ports.events[-1] == "intent"


@pytest.mark.parametrize("phase", ["persist", "verify"])
async def test_lost_acknowledgment_or_failed_exact_verification_never_becomes_success(
    phase: str,
) -> None:
    """Leave uncertain commits unresolved instead of retrying or inferring fresh-row success.

    Given: The single persist or exact verification port raises an operational error.
    When: The worker reaches that phase after durable intent.
    Then: It propagates the error and performs no retry.
    """
    ports = _Ports()
    if phase == "persist":
        ports.persist_error = RuntimeError("lost_acknowledgment")
    else:
        ports.verify_error = RuntimeError("lost_acknowledgment")
    with pytest.raises(RuntimeError, match="lost_acknowledgment"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events.count("persist") == 1
    assert ports.events[-1] == phase
    assert len(ports.commits) == (1 if phase == "verify" else 0)


async def test_commit_journal_failure_prevents_verification_without_retry() -> None:
    """Treat failed durable acknowledgment recording as non-success after the sole commit.

    Given: Persistence acknowledges exact versions but commit receipt fsync fails.
    When: The worker tries to retain that acknowledgment before verification.
    Then: It propagates failure without another persist or any verification call.
    """
    ports = _Ports(commit_error=OSError("commit_fsync_failed"))
    with pytest.raises(OSError, match="commit_fsync_failed"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "commit_receipt"
    assert ports.events.count("persist") == 1
    assert "verify" not in ports.events
    assert len(ports.intents) == 1 and len(ports.batches) == 1
    assert not ports.commits


@pytest.mark.parametrize(
    "phase", ["metadata", "prepare", "disconnect", "intent", "persist", "commit_receipt", "verify"]
)
async def test_each_phase_consumes_the_same_deadline_without_resetting_it(phase: str) -> None:
    """Refuse a phase result when its port consumed the remaining original budget.

    Given: One port advances the shared clock beyond the inherited deadline.
    When: The worker considers proceeding after that result.
    Then: It raises timeout without allocating a replacement phase budget.
    """
    clock = [0.0]

    def expire(name: str) -> None:
        """Consume the original budget at the selected port boundary.

        Args:
            name: Current phase name.
        """
        if name == phase:
            clock[0] = 11.0

    ports = _Ports(hook=expire)
    with pytest.raises(TimeoutError):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports), RefreshBudget(10.0, clock=lambda: clock[0])
        )
    assert ports.events[-1] == phase


async def test_expired_budget_never_invokes_metadata_reader() -> None:
    """Refuse work before creating an unawaited port coroutine when the budget is exhausted.

    Given: An absolute deadline in the past.
    When: The worker attempts its first metadata observation.
    Then: It raises timeout without calling any dependency.
    """
    ports = _Ports()
    with pytest.raises(TimeoutError):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports), RefreshBudget(0.0)
        )
    assert ports.events == []


async def test_metadata_timeout_never_prepares_an_adapter() -> None:
    """Bound a blocked metadata read before any transport or intent exists.

    Given: The metadata port waits past the available work allowance.
    When: Its inherited timeout expires.
    Then: The worker propagates timeout without reaching adapter preparation.
    """
    ports = _Ports(blocked_phase="metadata")
    with pytest.raises(TimeoutError):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports), _budget(2.05)
        )
    assert ports.events == ["metadata"]


@pytest.mark.parametrize(
    "phase", ["metadata", "disconnect", "intent", "persist", "commit_receipt", "verify"]
)
async def test_swallowed_caller_cancellation_is_not_converted_to_success(phase: str) -> None:
    """Keep caller cancellation authoritative even if a cooperative port swallows its exception.

    Given: A controlled port consumes CancelledError while retaining the cancellation count.
    When: The worker receives that apparent normal result.
    Then: It propagates cancellation and does not continue to another port.
    """
    ports = _Ports(swallowed_cancel_phase=phase)
    with pytest.raises(asyncio.CancelledError):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == phase


@pytest.mark.parametrize("disconnect_fails", [False, True])
async def test_collection_cancellation_always_attempts_disconnect_and_never_writes(
    disconnect_fails: bool,
) -> None:
    """Preserve original caller cancellation while attempting bounded transport cleanup.

    Given: Collection is waiting after one tick, optionally with a failing disconnect port.
    When: The owning task is cancelled.
    Then: The stream closes, disconnect is attempted and cancellation remains the outcome.
    """
    ports = _Ports(ticks=[_tick("BTC-EUR")], wait_after_ticks=True)
    if disconnect_fails:
        ports.disconnect_error = OSError("cleanup_failed")
    task = asyncio.create_task(
        run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    )
    await ports.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ports.events[-2:] == ["stream_closed", "disconnect"]
    assert not ports.intents and not ports.batches


async def test_collection_error_is_preserved_after_disconnect() -> None:
    """Do not replace an operational collection failure with partial evidence.

    Given: A stream raises an error before producing the entire required scope.
    When: The real collector propagates the failure.
    Then: The worker disconnects and preserves the error without any write path.
    """
    ports = _Ports(ticks=[_tick("BTC-EUR")], collection_error=RuntimeError("stream_failed"))
    with pytest.raises(RuntimeError, match="stream_failed"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "disconnect"
    assert not ports.batches


async def test_nonfinite_final_operands_refuse_before_intent() -> None:
    """Validate all persisted numerical operands even when last-price evidence is usable.

    Given: A selected tick has a valid last price but a nonfinite book operand.
    When: The complete collector result becomes a typed write batch.
    Then: The worker refuses the batch before durable intent or persistence.
    """
    ticks = _ticks()
    ticks[0].bid = float("inf")
    ports = _Ports(ticks=ticks)
    with pytest.raises(RefreshWorkerError, match="^invalid_collected_snapshot$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert not ports.intents and not ports.batches


async def test_naive_receive_clock_refuses_and_disconnects() -> None:
    """Never invent timezone information for a collection timestamp.

    Given: The injected original receipt clock is naive.
    When: Collection would begin.
    Then: The prepared adapter disconnects and no write intent exists.
    """
    ports = _Ports()
    with pytest.raises(RefreshWorkerError, match="^invalid_receive_clock$"):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports, lambda: _NOW.replace(tzinfo=None)), _budget()
        )
    assert ports.events[-1] == "disconnect"
    assert not ports.intents


@pytest.mark.parametrize("final_time", [_NOW - timedelta(seconds=1), _NOW + timedelta(seconds=301)])
async def test_original_receive_interval_is_revalidated_after_disconnect(
    final_time: datetime,
) -> None:
    """Refuse receipt times outside the actual collection interval or freshness bound.

    Given: Collected timestamps become future-relative or stale after disconnect.
    When: The worker validates the complete batch against its fresh UTC clock.
    Then: It refuses before persisting intent or prices.
    """
    clocks = iter([_NOW, _NOW, _NOW, final_time])
    ports = _Ports()
    with pytest.raises(RefreshWorkerError, match="^invalid_collection_interval$"):
        await run_market_snapshot_refresh_worker(
            _request(ports), _dependencies(ports, lambda: next(clocks)), _budget()
        )
    assert not ports.intents


async def test_acknowledgment_must_match_the_actual_collected_batch() -> None:
    """Refuse an acknowledged receipt for a different producer or batch.

    Given: The persistence port returns a receipt with the host UUID as producer identity.
    When: The worker binds that acknowledgment to its fresh producer batch.
    Then: It refuses before requesting exact verification.
    """
    ports = _Ports(receipt_transform=lambda receipt: receipt.model_copy(update={"run_id": _HOST}))
    with pytest.raises(RefreshWorkerError, match="^inconsistent_commit_receipt$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "persist"


@pytest.mark.parametrize("field_name", ["session_id", "sequence_id", "timestamp"])
async def test_acknowledgment_row_provenance_must_match_exact_operands(field_name: str) -> None:
    """Refuse changed provenance even when the receipt preserves the payload hash string.

    Given: An acknowledged row changes its explicit producer session, sequence or receive time.
    When: The worker binds the receipt to the exact collected operands.
    Then: It refuses before publishing that acknowledgment or requesting verification.
    """

    def corrupt(receipt: RefreshCommitReceipt) -> RefreshCommitReceipt:
        """Change one explicit provenance value while retaining the original digest.

        Args:
            receipt: Exact synthetic store acknowledgment.

        Returns:
            A deliberately inconsistent receipt for the worker boundary.
        """
        values: dict[str, str | int | datetime] = {
            "session_id": _HOST,
            "sequence_id": 999,
            "timestamp": _NOW + timedelta(seconds=1),
        }
        first, second = receipt.snapshots
        changed = first.model_copy(update={field_name: values[field_name]})
        return receipt.model_copy(update={"snapshots": (changed, second)})

    ports = _Ports(receipt_transform=corrupt)
    with pytest.raises(RefreshWorkerError, match="^inconsistent_commit_receipt$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())
    assert ports.events[-1] == "persist"
    assert not ports.commits


async def test_verification_cannot_replace_the_acknowledged_receipt() -> None:
    """Require exact verification to return the same acknowledged version evidence.

    Given: The verification port returns an altered receipt.
    When: The worker compares the verification result with the acknowledged commit.
    Then: It refuses instead of accepting replacement evidence.
    """
    ports = _Ports(
        verification_transform=lambda receipt: receipt.model_copy(update={"run_id": _HOST})
    )
    with pytest.raises(RefreshWorkerError, match="^verification_receipt_changed$"):
        await run_market_snapshot_refresh_worker(_request(ports), _dependencies(ports), _budget())


def test_default_worker_clock_is_aware_utc() -> None:
    """Keep the default receive clock explicit and timezone-aware.

    Given: No test or host clock override.
    When: The worker reads its default clock.
    Then: The instant is aware UTC.
    """
    assert worker._utc_now().tzinfo is UTC


@pytest.mark.parametrize("case", ["session", "zero_sequence", "duplicate_sequence"])
async def test_final_batch_provenance_is_bound_to_one_new_producer(case: str) -> None:
    """Reject inconsistent provenance even if a future collector changes its operands.

    Given: A real collected batch whose session or sequence evidence is then corrupted.
    When: The pre-intent producer contract validates that batch.
    Then: It refuses the changed provenance instead of rebinding it to the host UUID.
    """
    ports = _Ports()
    receipt = await run_market_snapshot_refresh_worker(
        _request(ports), _dependencies(ports), _budget()
    )
    batch = ports.batches[0]
    first, second = batch.snapshots
    if case == "session":
        first = first.model_copy(update={"session_id": _HOST})
    elif case == "zero_sequence":
        first = first.model_copy(update={"sequence_id": 0})
    else:
        second = second.model_copy(update={"sequence_id": first.sequence_id})
    changed = batch.model_copy(update={"snapshots": (first, second)})
    with pytest.raises(RefreshWorkerError, match="^invalid_producer_provenance$"):
        worker._validate_producer_batch(receipt.run_id, changed, _NOW)


async def test_async_boundaries_refuse_without_an_owning_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep cancellation bookkeeping explicit when an invalid driver has no owning task.

    Given: A boundary reports no current asyncio task.
    When: A budgeted operation or disconnect is attempted.
    Then: It refuses before invoking the port rather than losing cancellation ownership.
    """
    ports = _Ports()
    monkeypatch.setattr(worker.asyncio, "current_task", lambda: None)
    with pytest.raises(RuntimeError, match="owning task"):
        await worker._within_budget(lambda: ports._phase("unexpected"), _budget())
    with pytest.raises(RuntimeError, match="owning task"):
        await worker._disconnect(ports, _budget())
    assert not ports.events
