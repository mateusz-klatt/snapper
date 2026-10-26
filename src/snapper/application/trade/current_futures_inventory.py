"""Read current account-wide inventory without turning unavailability into an empty book.

Each endpoint retains its own original request and response. This reader grants
neither financial permission nor an atomic accounts/orders/positions snapshot.
The repository entry point owns the wallet fence; composable use requires that
same caller-owned transaction and fence.
"""

import base64
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_inventory_key_binding import FuturesInventoryBindingError
from snapper.data.futures_inventory_key_binding import get_bound_inventory_read_key
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryAttempt
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryOutcome
from snapper.data.futures_inventory_read_store import LatestFuturesInventoryRead
from snapper.data.futures_inventory_read_store import get_latest_futures_inventory_read

type ParsedFuturesInventory = FuturesOpenOrdersSnapshot | FuturesOpenPositionsSnapshot
type InventoryUnavailableReason = Literal[
    "not_read", "pending", "failed", "stale", "clock_skew", "unverifiable"
]


@dataclass(frozen=True, slots=True)
class FuturesInventoryFreshnessPolicy:
    """Trusted source/key age and provider-clock bounds, without caller-selected defaults."""

    maximum_source_age: timedelta
    maximum_server_clock_skew: timedelta

    def __post_init__(self) -> None:
        """Require a positive observation window and a nonnegative provider-clock tolerance."""
        if self.maximum_source_age <= timedelta(0) or self.maximum_server_clock_skew < timedelta(0):
            raise ValueError("Inventory freshness bounds must be positive and nonnegative")


@dataclass(frozen=True, slots=True)
class CurrentFuturesInventoryRead:
    """An observational result whose unavailable state never supplies a synthetic empty source.

    Public construction of this view establishes no authority. Financial and
    recovery consumers must retain original command/account evidence separately.
    """

    purpose: InventoryReadPurpose
    status: Literal["available", "unavailable"]
    reason: InventoryUnavailableReason | None
    attempt: BoundFuturesInventoryAttempt | None
    outcome: BoundFuturesInventoryOutcome | None
    snapshot: ParsedFuturesInventory | None


def _unavailable(
    purpose: InventoryReadPurpose,
    reason: InventoryUnavailableReason,
    latest: LatestFuturesInventoryRead | None = None,
) -> CurrentFuturesInventoryRead:
    """Retain explicit absence or verified historical references without fabricating inventory."""
    return CurrentFuturesInventoryRead(
        purpose=purpose,
        status="unavailable",
        reason=reason,
        attempt=None if latest is None else latest.attempt,
        outcome=None if latest is None else latest.outcome,
        snapshot=None,
    )


def _parse_source(
    purpose: InventoryReadPurpose, source: FuturesInventoryReadSource
) -> ParsedFuturesInventory:
    """Interpret the complete exact source under its original endpoint-specific parser."""
    original = base64.b64decode(source.body, validate=True)
    if purpose == "kraken_futures.open_orders":
        return parse_futures_open_orders(original)
    return parse_futures_open_positions(original)


def _source_disposition(
    latest: LatestFuturesInventoryRead,
    source: FuturesInventoryReadSource,
    snapshot: ParsedFuturesInventory,
    known_at: datetime,
    policy: FuturesInventoryFreshnessPolicy,
) -> InventoryUnavailableReason | None:
    """Verify the ordered original window after its earliest actual key GET passed the age bound."""
    attempt = latest.attempt.payload
    outcome = latest.outcome
    if outcome is None:
        return "unverifiable"
    key = attempt.key_proof
    if not (
        key.observed_at
        <= key.recorded_at
        <= attempt.started_at
        <= source.request_started_at
        <= source.observed_at
        <= outcome.payload.completed_at
        <= known_at
    ):
        return "unverifiable"
    if (
        abs(snapshot.server_time.instant - source.observed_at)
        > policy.maximum_server_clock_skew
    ):
        return "clock_skew"
    return None


async def _read_latest_inventory(
    session: AsyncSession,
    latest: LatestFuturesInventoryRead,
    purpose: InventoryReadPurpose,
    known_at: datetime,
    policy: FuturesInventoryFreshnessPolicy,
) -> CurrentFuturesInventoryRead:
    """Keep newest-attempt barriers and age the whole chain from its earliest actual signed GET.

    Actual proof loading binds key GET start before key observation/recording,
    followed by the inventory request, source observation and outcome. With one
    shared age bound, checking that earliest actual GET also bounds every later
    inventory clock without renewing the window on delayed completion.
    """
    if latest.attempt.payload.purpose != purpose:
        raise FuturesInventoryBindingError("futures_inventory_current_purpose_mismatch")
    outcome = latest.outcome
    if outcome is None:
        return _unavailable(purpose, "pending", latest)
    source = outcome.payload.result
    if not isinstance(source, FuturesInventoryReadSource):
        return _unavailable(purpose, "failed", latest)
    proof = await get_bound_inventory_read_key(session, latest.attempt.payload.key_proof)
    if known_at - proof.payload.observation.request_started_at >= policy.maximum_source_age:
        return _unavailable(purpose, "stale", latest)
    snapshot = _parse_source(purpose, source)
    disposition = _source_disposition(latest, source, snapshot, known_at, policy)
    if disposition is not None:
        return _unavailable(purpose, disposition, latest)
    return CurrentFuturesInventoryRead(
        purpose=purpose,
        status="available",
        reason=None,
        attempt=latest.attempt,
        outcome=outcome,
        snapshot=snapshot,
    )


async def get_current_futures_inventory_read(
    session: AsyncSession,
    selection: ExecutionClientSelection,
    purpose: InventoryReadPurpose,
    known_at: datetime,
    policy: FuturesInventoryFreshnessPolicy,
) -> CurrentFuturesInventoryRead:
    """Read one endpoint inside the caller's actual wallet-fenced transaction.

    Args:
        session: Caller-owned transaction already holding the actual wallet execution fence.
        selection: Current public wallet, credential, venue and environment to verify in the store.
        purpose: Independent fixed endpoint whose newest attempt supplies the current barrier.
        known_at: Trusted aware server clock sampled after acquiring the wallet fence.
        policy: Trusted source/key freshness and provider-clock limits without an order grant.

    Returns:
        A verified current observation, or explicit unavailability without an empty-book fallback.
    """
    if not session.in_transaction() or known_at.utcoffset() is None:
        return _unavailable(purpose, "unverifiable")
    try:
        latest = await get_latest_futures_inventory_read(session, selection, purpose, known_at)
        if latest is None:
            return _unavailable(purpose, "not_read")
        return await _read_latest_inventory(session, latest, purpose, known_at, policy)
    except ValueError:
        return _unavailable(purpose, "unverifiable")


async def read_current_futures_inventory(
    repository: AccountingRepository,
    selection: ExecutionClientSelection,
    purpose: InventoryReadPurpose,
    policy: FuturesInventoryFreshnessPolicy,
) -> CurrentFuturesInventoryRead:
    """Own the real wallet fence before selecting current inventory or sampling its local age.

    Args:
        repository: Actual transaction provider sharing the established wallet execution fence.
        selection: Original current public client selection, independently checked by the store.
        purpose: Fixed orders or positions endpoint without a profile or order-access prerequisite.
        policy: Trusted observation bounds, never a financial or CREATE permission.

    Returns:
        Explicit available/unavailable component state; separate calls are not an atomic snapshot.
    """
    try:
        selected = ExecutionClientSelection.model_validate(selection)
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(session, selected.wallet_public_id)
            return await get_current_futures_inventory_read(
                session, selected, purpose, datetime.now(UTC), policy
            )
    except (ValueError, SQLAlchemyError):
        return _unavailable(purpose, "unverifiable")
