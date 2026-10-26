"""Exercise current-reader boundaries without SQL, HTTP or source-issuance claims.

Strict fixture payloads exercise the reader through typed store/proof boundary
doubles. Separate integration tests cover actual persistence, guarded HTTP and
wallet-lock behavior; these unit fixtures do not replace that acceptance.
"""

import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import InvalidOperation
from typing import Literal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.current_futures_inventory as reader
from snapper.application.trade.current_futures_inventory import FuturesInventoryFreshnessPolicy
from snapper.application.trade.current_futures_inventory import get_current_futures_inventory_read
from snapper.application.trade.current_futures_inventory import read_current_futures_inventory
from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_inventory_key_binding import inventory_read_key_reference
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryAttempt
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryOutcome
from snapper.data.futures_inventory_read_store import LatestFuturesInventoryRead
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof

_NOW = datetime(2026, 10, 25, 1, tzinfo=UTC)
_AGE = timedelta(seconds=30)
_POLICY = FuturesInventoryFreshnessPolicy(
    maximum_source_age=_AGE, maximum_server_clock_skew=timedelta(seconds=5)
)
_PURPOSES: tuple[InventoryReadPurpose, ...] = (
    "kraken_futures.open_orders",
    "kraken_futures.open_positions",
)


def _uuid(number: int) -> str:
    """Allocate stable public unit-fixture identities without a source or SQL operation."""
    return f"00000000-0000-7000-8000-{number:012x}"


def _selection() -> ExecutionClientSelection:
    """Keep the reader's observational source independent of margin labels and trading access."""
    return ExecutionClientSelection(
        wallet_public_id=_uuid(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        credential_public_id=_uuid(2),
        credential_timestamp=_NOW - timedelta(days=1),
        public_api_origin="https://futures.kraken.com",
    )


@dataclass(frozen=True)
class _Times:
    """Keep original key GET, account-attempt queue and inventory receipt clocks distinct."""

    account_attempt: datetime = _NOW - timedelta(seconds=6)
    key_request: datetime = _NOW - timedelta(seconds=5)
    key_observed: datetime = _NOW - timedelta(seconds=4)
    key_recorded: datetime = _NOW - timedelta(seconds=3)
    inventory_attempt: datetime = _NOW - timedelta(seconds=2)
    inventory_request: datetime = _NOW - timedelta(seconds=2)
    observed: datetime = _NOW - timedelta(seconds=1)
    completed: datetime = _NOW
    provider: datetime = _NOW - timedelta(seconds=1)


def _proof(times: _Times) -> BoundNativeAccountIdentityProof:
    """Supply strict READ_ONLY native history shape to the isolated reader boundary double."""
    attempt = NativeAccountReadAttemptPayload(
        client_selection=_selection(), family_sequence=1, started_at=times.account_attempt
    )
    observation = NativeAccountPublicIdentity(
        client_selection=_selection(),
        account_uid=_uuid(90),
        general_access="READ_ONLY",
        transfer_access="NO_ACCESS",
        request_started_at=times.key_request,
        observed_at=times.key_observed,
    )
    return BoundNativeAccountIdentityProof(
        _uuid(21),
        NativeAccountIdentityProofPayload(
            attempt_public_id=_uuid(20),
            attempt_content_digest=native_account_read_attempt_digest(attempt),
            attempt=attempt,
            observation=observation,
            recorded_at=times.key_recorded,
            owner_root=None,
        ),
    )


def _latest(
    purpose: InventoryReadPurpose,
    times: _Times,
    kind: Literal["empty", "pending", "failed"] = "empty",
) -> tuple[LatestFuturesInventoryRead, BoundNativeAccountIdentityProof]:
    """Build strict empty or unavailable endpoint states without asserting actual persisted rows."""
    proof = _proof(times)
    attempt = FuturesInventoryReadAttempt(
        purpose=purpose,
        client_selection=_selection(),
        key_proof=inventory_read_key_reference(proof),
        family_sequence=1,
        started_at=times.inventory_attempt,
    )
    bound = BoundFuturesInventoryAttempt(_uuid(30), attempt)
    if kind == "pending":
        return LatestFuturesInventoryRead(bound, None), proof
    result: FuturesInventoryReadSource | FuturesInventoryReadFailure
    if kind == "failed":
        result = FuturesInventoryReadFailure(reason="transport_error")
    else:
        orders = purpose == "kraken_futures.open_orders"
        body = json.dumps(
            {
                "result": "success",
                "serverTime": times.provider.isoformat(),
                "openOrders" if orders else "openPositions": [],
            }
        ).encode()
        result = FuturesInventoryReadSource(
            parser_policy=(
                "kraken_futures_open_orders_v1" if orders else "kraken_futures_open_positions_v1"
            ),
            body=base64.b64encode(body).decode("ascii"),
            request_started_at=times.inventory_request,
            observed_at=times.observed,
        )
    outcome = FuturesInventoryReadOutcome(
        attempt_public_id=bound.public_id,
        attempt_content_digest=futures_inventory_attempt_digest(attempt),
        attempt=attempt,
        completed_at=times.completed,
        result=result,
    )
    return (
        LatestFuturesInventoryRead(bound, BoundFuturesInventoryOutcome(_uuid(31), outcome)),
        proof,
    )


class _SessionState:
    """Model only transaction presence, without executing or emulating SQL statements."""

    def __init__(self, active: bool = True) -> None:
        """Retain whether the composable reader's caller says its transaction is active."""
        self.active = active

    def in_transaction(self) -> bool:
        """Expose only the exact transaction-presence boundary used by the reader."""
        return self.active


def _patch_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    latest: LatestFuturesInventoryRead | None,
    proof: BoundNativeAccountIdentityProof,
    calls: list[str],
) -> None:
    """Observe reader-store delegation without granting unit fixtures actual provenance."""

    async def latest_loader(
        session: AsyncSession,
        selection: ExecutionClientSelection,
        purpose: InventoryReadPurpose,
        known_at: datetime,
    ) -> LatestFuturesInventoryRead | None:
        """Return only the selected boundary state and preserve the requested endpoint/clock."""
        assert selection == _selection() and known_at == _NOW
        if latest is not None:
            assert purpose == latest.attempt.payload.purpose
        calls.append("latest")
        return latest

    async def key_loader(
        session: AsyncSession, reference: InventoryReadKeyReference
    ) -> BoundNativeAccountIdentityProof:
        """Require the exact referenced original key rather than a latest accounts-source lookup."""
        assert reference == inventory_read_key_reference(proof)
        calls.append("original_key")
        return proof

    monkeypatch.setattr(reader, "get_latest_futures_inventory_read", latest_loader)
    monkeypatch.setattr(reader, "get_bound_inventory_read_key", key_loader)


@pytest.mark.parametrize("purpose", _PURPOSES)
@pytest.mark.parametrize("kind", ("empty", "pending", "failed", "not_read"))
async def test_unavailability_is_distinct_from_a_successful_empty_observation(
    monkeypatch: pytest.MonkeyPatch, purpose: InventoryReadPurpose, kind: str
) -> None:
    """Keep absent/pending/failed books explicit while allowing genuine parsed empty-source shape.

    Given: A reader has no request, pending/failed latest request or successful empty bytes.
    When: The composable reader evaluates that exact endpoint state using a READ_ONLY key proof.
    Then: Only the parsed successful state has an empty snapshot; every unavailable state has None.
    """
    variant = cast(Literal["empty", "pending", "failed"], "empty" if kind == "not_read" else kind)
    latest, proof = _latest(purpose, _Times(), variant)
    calls: list[str] = []
    _patch_boundaries(monkeypatch, None if kind == "not_read" else latest, proof, calls)
    result = await get_current_futures_inventory_read(
        cast(AsyncSession, _SessionState()), _selection(), purpose, _NOW, _POLICY
    )
    if kind == "empty":
        assert result.status == "available" and result.reason is None
        assert proof.payload.observation.general_access == "READ_ONLY"
        if isinstance(result.snapshot, FuturesOpenOrdersSnapshot):
            assert result.snapshot.orders == ()
        else:
            assert isinstance(result.snapshot, FuturesOpenPositionsSnapshot)
            assert result.snapshot.positions == ()
        assert calls == ["latest", "original_key"]
    else:
        assert result.status == "unavailable" and result.reason == (
            "pending" if kind == "pending" else "failed" if kind == "failed" else "not_read"
        )
        assert result.snapshot is None and calls == ["latest"]


@pytest.mark.parametrize(
    ("fault", "expected"),
    (
        ("exact_ceiling", "stale"),
        ("just_under", None),
        ("old_actual_key_get", "stale"),
        ("old_account_attempt_fresh_get", None),
        ("late_inventory_completion", "stale"),
        ("provider_skew", "clock_skew"),
    ),
)
async def test_original_http_age_is_not_renewed_by_late_observation_or_attempt_queue_time(
    monkeypatch: pytest.MonkeyPatch, fault: str, expected: str | None
) -> None:
    """Pin actual key/inventory GET ages while keeping the earlier durable account queue separate.

    Given: Strict chronological proof/source shapes and one trusted age/skew policy.
    When: HTTP reaches expiry, completes late, queued earlier, or reports excessive skew.
    Then: Expiry includes the ceiling and a fresh key GET survives an old account queue.
    """
    times = _Times()
    if fault == "exact_ceiling":
        times = replace(
            times,
            account_attempt=_NOW - _AGE - timedelta(seconds=1),
            key_request=_NOW - _AGE,
        )
    elif fault == "just_under":
        times = replace(
            times,
            account_attempt=_NOW - _AGE - timedelta(seconds=1),
            key_request=_NOW - _AGE + timedelta(microseconds=1),
        )
    elif fault == "old_actual_key_get":
        times = replace(
            times,
            account_attempt=_NOW - timedelta(minutes=6),
            key_request=_NOW - timedelta(minutes=5),
        )
    elif fault == "old_account_attempt_fresh_get":
        times = replace(times, account_attempt=_NOW - timedelta(minutes=5))
    elif fault == "late_inventory_completion":
        times = replace(
            times,
            account_attempt=_NOW - timedelta(minutes=6),
            key_request=_NOW - timedelta(minutes=5),
            key_observed=_NOW - timedelta(minutes=4),
            key_recorded=_NOW - timedelta(minutes=4),
            inventory_attempt=_NOW - timedelta(minutes=3),
            inventory_request=_NOW - timedelta(minutes=3),
        )
    else:
        times = replace(times, provider=times.observed + timedelta(seconds=6))
    latest, proof = _latest("kraken_futures.open_orders", times)
    calls: list[str] = []
    _patch_boundaries(monkeypatch, latest, proof, calls)
    result = await get_current_futures_inventory_read(
        cast(AsyncSession, _SessionState()),
        _selection(),
        "kraken_futures.open_orders",
        _NOW,
        _POLICY,
    )
    assert result.reason == expected
    assert result.status == ("available" if expected is None else "unavailable")
    assert (result.snapshot is None) == (expected is not None)
    assert calls == ["latest", "original_key"]


@pytest.mark.parametrize("fault", ("no_transaction", "naive_clock"))
async def test_missing_caller_transaction_or_aware_clock_never_reads_a_book(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Enforce composable reader prerequisites without replacing uncertainty with empty inventory.

    Given: A caller lacks its transaction or supplies a naive knowledge clock.
    When: The current reader checks its preconditions.
    Then: It returns explicit unverifiable state before touching either store boundary.
    """
    latest, proof = _latest("kraken_futures.open_orders", _Times())
    calls: list[str] = []
    _patch_boundaries(monkeypatch, latest, proof, calls)
    result = await get_current_futures_inventory_read(
        cast(AsyncSession, _SessionState(fault != "no_transaction")),
        _selection(),
        "kraken_futures.open_orders",
        _NOW.replace(tzinfo=None) if fault == "naive_clock" else _NOW,
        _POLICY,
    )
    assert result.status == "unavailable" and result.reason == "unverifiable"
    assert result.snapshot is None and not calls


class _RepositoryBoundary:
    """Observe wrapper call ordering without proving actual isolation or mutual exclusion."""

    def __init__(self, calls: list[str]) -> None:
        """Retain only the boundary call sequence and transaction-presence double."""
        self.calls = calls
        self.session_state = cast(AsyncSession, _SessionState())

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Supply the unit transaction double while recording repository scope entry."""
        self.calls.append("session")
        yield self.session_state

    async def _begin_execution_insert_transaction(self, session: AsyncSession) -> None:
        """Observe that the wrapper opens its execution transaction before acquiring its fence."""
        assert session is self.session_state
        self.calls.append("begin")

    async def _acquire_execution_fence_lock(
        self, session: AsyncSession, wallet_public_id: str
    ) -> None:
        """Observe the selected wallet before the wrapper samples its knowledge clock."""
        assert session is self.session_state and wallet_public_id == _selection().wallet_public_id
        self.calls.append("fence")


async def test_repository_wrapper_samples_age_after_its_selected_wallet_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin wrapper sequencing while leaving real SQLite/PostgreSQL locking acceptance separate.

    Given: Typed repository/store boundaries expose an available READ_ONLY empty-source shape.
    When: The public wrapper opens its transaction and selects the actual wallet fence.
    Then: It samples the trusted clock only afterward and delegates the same endpoint/selection.
    """
    calls: list[str] = []
    repository = _RepositoryBoundary(calls)
    latest, proof = _latest("kraken_futures.open_orders", _Times())
    _patch_boundaries(monkeypatch, latest, proof, calls)

    class Clock:
        """Observe the exact clock sampling boundary without changing stored source times."""

        @staticmethod
        def now(zone: object) -> datetime:
            """Require the wrapper's selected fence immediately before its knowledge sample."""
            assert zone is UTC and calls[-1] == "fence"
            calls.append("clock")
            return _NOW

    monkeypatch.setattr(reader, "datetime", Clock)
    result = await read_current_futures_inventory(
        cast(AccountingRepository, repository), _selection(), "kraken_futures.open_orders", _POLICY
    )
    assert result.status == "available"
    assert calls == ["session", "begin", "fence", "clock", "latest", "original_key"]


@pytest.mark.parametrize("boundary", ("store", "transaction"))
async def test_decimal_boundary_failure_preserves_explicit_unverifiable_inventory(
    monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    """Refuse malformed decimal evidence without allowing an arithmetic failure to escape as inventory.

    Given: An actual reader boundary cannot decode a stored decimal or establish its transaction.
    When: That boundary reports Decimal InvalidOperation through its normal exception contract.
    Then: The reader returns unverifiable with no fabricated empty snapshot or source references.
    """
    if boundary == "store":

        async def invalid_decimal(
            session: AsyncSession,
            selection: ExecutionClientSelection,
            purpose: InventoryReadPurpose,
            known_at: datetime,
        ) -> LatestFuturesInventoryRead | None:
            """Expose a decimal decoding refusal at the independently loaded evidence boundary."""
            raise InvalidOperation("Synthetic malformed stored numeric evidence")

        monkeypatch.setattr(reader, "get_latest_futures_inventory_read", invalid_decimal)
        result = await get_current_futures_inventory_read(
            cast(AsyncSession, _SessionState()),
            _selection(),
            "kraken_futures.open_orders",
            _NOW,
            _POLICY,
        )
    else:
        repository = _RepositoryBoundary([])

        async def invalid_transaction(session: AsyncSession) -> None:
            """Refuse a transaction whose adapter cannot interpret its actual stored numeric state."""
            raise InvalidOperation("Synthetic invalid transaction numeric state")

        monkeypatch.setattr(repository, "_begin_execution_insert_transaction", invalid_transaction)
        result = await read_current_futures_inventory(
            cast(AccountingRepository, repository),
            _selection(),
            "kraken_futures.open_orders",
            _POLICY,
        )
    assert result.status == "unavailable" and result.reason == "unverifiable"
    assert result.snapshot is None and result.attempt is None and result.outcome is None


@pytest.mark.parametrize(
    ("age", "skew"),
    (
        (timedelta(0), timedelta(0)),
        (timedelta(microseconds=-1), timedelta(0)),
        (timedelta(seconds=1), timedelta(microseconds=-1)),
    ),
)
def test_inventory_policy_refuses_nonpositive_age_or_negative_clock_tolerance(
    age: timedelta, skew: timedelta
) -> None:
    """Reject invalid trusted freshness configuration before it can influence any inventory read.

    Given: A policy has an expired-by-definition age window or negative provider-clock tolerance.
    When: The caller constructs the trusted reader configuration.
    Then: It refuses those bounds instead of silently changing or supplying defaults.
    """
    with pytest.raises(ValueError, match="freshness bounds"):
        FuturesInventoryFreshnessPolicy(maximum_source_age=age, maximum_server_clock_skew=skew)


@pytest.mark.parametrize("fault", ("wrong_endpoint", "future_completion"))
async def test_reader_refuses_contradictory_latest_endpoint_or_knowledge_boundary(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Keep the public reader defensive when its loader returns contradictory typed evidence.

    Given: A unit loader returns another endpoint or a strictly valid outcome not yet known locally.
    When: The requested orders reader independently checks endpoint and original knowledge clocks.
    Then: It reports unverifiable and exposes no empty snapshot or unverified source references.
    """
    latest, proof = _latest(
        (
            "kraken_futures.open_positions"
            if fault == "wrong_endpoint"
            else "kraken_futures.open_orders"
        ),
        (
            replace(_Times(), completed=_NOW + timedelta(microseconds=1))
            if fault == "future_completion"
            else _Times()
        ),
    )
    calls: list[str] = []

    async def contradictory_latest(
        session: AsyncSession,
        selection: ExecutionClientSelection,
        purpose: InventoryReadPurpose,
        known_at: datetime,
    ) -> LatestFuturesInventoryRead | None:
        """Return a contradictory boundary result without claiming actual SQL provenance."""
        assert selection == _selection() and known_at == _NOW
        assert purpose == "kraken_futures.open_orders"
        calls.append("latest")
        return latest

    async def original_key(
        session: AsyncSession, reference: InventoryReadKeyReference
    ) -> BoundNativeAccountIdentityProof:
        """Keep the exact original proof shape while allowing independent current-clock checks."""
        assert reference == inventory_read_key_reference(proof)
        calls.append("key")
        return proof

    monkeypatch.setattr(reader, "get_latest_futures_inventory_read", contradictory_latest)
    monkeypatch.setattr(reader, "get_bound_inventory_read_key", original_key)
    result = await get_current_futures_inventory_read(
        cast(AsyncSession, _SessionState()),
        _selection(),
        "kraken_futures.open_orders",
        _NOW,
        _POLICY,
    )
    assert result.status == "unavailable" and result.reason == "unverifiable"
    assert result.snapshot is None
    if fault == "wrong_endpoint":
        assert result.attempt is None and result.outcome is None and calls == ["latest"]
    else:
        assert result.attempt == latest.attempt and result.outcome == latest.outcome
        assert calls == ["latest", "key"]
