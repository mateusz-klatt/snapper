"""Own native inventory transport, persistence and fresh observational consumer reads.

Startup reuses the actual prepared key/accounts owner before its socket handoff.
Later refreshes explicitly prepare a fresh owned source token and always close
it after joined transport work. No socket-private capability is recovered and
no inventory result supplies financial or original-command authority.
"""

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal
from uuid import uuid7

from kraken.futures import User

from snapper.application.trade.current_futures_inventory import FuturesInventoryFreshnessPolicy
from snapper.application.trade.current_futures_inventory import read_current_futures_inventory
from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.application.trade.native_account_owner_preflight import require_actual_native_user
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.application.trade.owned_native_source_read import read_owned_native_source
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_inventory_capture import FuturesInventoryWriteResult
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryAttempt
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryOutcome
from snapper.data.futures_inventory_read_store import _append_futures_inventory_outcome
from snapper.data.futures_inventory_read_store import begin_futures_inventory_read
from snapper.infrastructure.exchanges.futures_inventory_wire import FuturesInventoryWireError
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import read_futures_inventory_wire
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker

_FRESHNESS = FuturesInventoryFreshnessPolicy(
    maximum_source_age=timedelta(seconds=30), maximum_server_clock_skew=timedelta(seconds=5)
)
_PURPOSES: tuple[InventoryReadPurpose, ...] = (
    "kraken_futures.open_orders",
    "kraken_futures.open_positions",
)


@dataclass(frozen=True, slots=True)
class CurrentFuturesInventoryRead:
    """Expose qualified original inventory or explicit unavailability to real executor consumers."""

    snapshot: FuturesOpenOrdersSnapshot | FuturesOpenPositionsSnapshot | None
    outcome: FuturesInventoryReadOutcome | None
    reason: str

    @property
    def available(self) -> bool:
        """Distinguish a verified empty array from absence of any usable original source.

        Returns:
            Whether a qualified complete endpoint interpretation exists.
        """
        return self.snapshot is not None


@dataclass(frozen=True, slots=True)
class _InventoryOperation:
    """Keep the actually committed request and its stable producer-owned outcome identity."""

    attempt: BoundFuturesInventoryAttempt
    outcome_identity: ExecutionEvidenceIdentity


def _identity(
    tracker: SequenceTracker, selection: ExecutionClientSelection
) -> ExecutionEvidenceIdentity:
    """Allocate independent observational identities without reusing financial receipt sequences."""
    return ExecutionEvidenceIdentity(
        public_id=str(uuid7()),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(f"native.inventory.{selection.wallet_public_id}"),
    )


async def _start_operation(
    repository: AccountingRepository,
    token: PreparedKeyToken,
    purpose: InventoryReadPurpose,
    tracker: SequenceTracker,
) -> _InventoryOperation:
    """Await the actual independent request commit before allowing its fixed endpoint GET."""
    reference = InventoryReadKeyReference.model_validate(
        token.require_proof_reference().model_dump(mode="python")
    )
    attempt = await begin_futures_inventory_read(
        repository, reference, purpose, _identity(tracker, reference.client_selection)
    )
    return _InventoryOperation(attempt, _identity(tracker, reference.client_selection))


async def _record_result(
    repository: AccountingRepository,
    operation: _InventoryOperation,
    result: FuturesInventoryWriteResult,
) -> BoundFuturesInventoryOutcome:
    """Commit one original result under the wallet fence without replacing an ambiguous prior commit."""
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, operation.attempt.payload.client_selection.wallet_public_id
        )
        outcome = await _append_futures_inventory_outcome(
            session, operation.attempt.public_id, result, operation.outcome_identity
        )
        await session.commit()
        return outcome


async def _read_result(
    client: KrakenFuturesExchangeClient,
    user: User,
    token: PreparedKeyToken,
    operation: _InventoryOperation,
) -> GuardedFuturesInventoryReceipt:
    """Join actual routed inventory work before any source ownership can be released."""
    attempt = operation.attempt
    selected = attempt.payload.client_selection
    purpose = attempt.payload.purpose
    require_actual_native_user(client, selected, user)
    client._record_rest_call()
    operation_name: Literal["get_native_open_orders_source", "get_native_open_positions_source"] = (
        "get_native_open_orders_source"
        if purpose == "kraken_futures.open_orders"
        else "get_native_open_positions_source"
    )
    received = await read_owned_native_source(
        client,
        user,
        operation_name,
        lambda: read_futures_inventory_wire(user, selected, token, attempt.public_id, purpose),
    )
    require_actual_native_user(client, selected, user)
    return received


async def _complete_operation(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    user: User,
    token: PreparedKeyToken,
    operation: _InventoryOperation,
) -> BoundFuturesInventoryOutcome:
    """Retain safe failures only before captured-outcome persistence has started."""
    result: FuturesInventoryWriteResult
    try:
        result = await _read_result(client, user, token, operation)
    except asyncio.CancelledError:
        with suppress(Exception, asyncio.CancelledError):
            await _record_result(
                repository, operation, FuturesInventoryReadFailure(reason="cancelled")
            )
        raise
    except FuturesInventoryWireError as refused:
        result = FuturesInventoryReadFailure(reason=refused.reason)
    except Exception:
        result = FuturesInventoryReadFailure(reason="transport_error")
    return await _record_result(repository, operation, result)


async def _read_with_prepared_owner(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    prepared: PreparedPrivateOwnerRead,
    purpose: InventoryReadPurpose,
    tracker: SequenceTracker,
) -> BoundFuturesInventoryOutcome:
    """Use the actual still-owned prepared token without consulting a later private socket slot."""
    token = prepared.require_token()
    selected = token.require_proof_reference().client_selection
    user = require_actual_native_user(client, selected, client._user_client)
    operation = await _start_operation(repository, token, purpose, tracker)
    return await _complete_operation(repository, client, user, token, operation)


async def capture_prepared_futures_inventory(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    prepared: PreparedPrivateOwnerRead,
    tracker: SequenceTracker,
) -> None:
    """Capture independent initial inventories before the existing real socket ownership handoff.

    Args:
        repository: Actual source transaction provider sharing the wallet execution fence.
        client: Same selected Futures client whose private socket has not taken the source token.
        prepared: Actual successful key/accounts preparation, still owned by the caller.
        tracker: Original producer session and independent observation sequence allocator.

    Raises:
        BaseException: Failed commitment or cancellation propagates to the caller's owner cleanup.
    """
    for purpose in _PURPOSES:
        await _read_with_prepared_owner(repository, client, prepared, purpose, tracker)


async def _current(
    repository: AccountingRepository,
    selected: ExecutionClientSelection,
    purpose: InventoryReadPurpose,
) -> CurrentFuturesInventoryRead:
    """Project only the fenced reader's verified snapshot and exact retained outcome."""
    current = await read_current_futures_inventory(repository, selected, purpose, _FRESHNESS)
    return CurrentFuturesInventoryRead(
        snapshot=current.snapshot,
        outcome=None if current.outcome is None else current.outcome.payload,
        reason=current.reason or "available",
    )


async def refresh_current_futures_inventory(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    purpose: InventoryReadPurpose,
    tracker: SequenceTracker,
) -> CurrentFuturesInventoryRead:
    """Read a fresh qualified endpoint or explicitly own a new source preparation and refresh.

    Args:
        repository: Actual transaction provider; stored source is never a caller-supplied DTO.
        client: Same actual selected client and SDK User, independently checked across awaits.
        purpose: Fixed independent orders or positions endpoint without a trading permission check.
        tracker: Actual producer provenance allocator for new request and result identities.

    Returns:
        Qualified inventory or an explicit unavailable state, never an invented empty book.

    Raises:
        asyncio.CancelledError: Propagates after actual source workers have joined and tokens close.
    """
    if purpose not in _PURPOSES:
        return CurrentFuturesInventoryRead(None, None, "unverifiable")
    selected = client.execution_client_selection()
    user = client._user_client
    if selected is None or user is None:
        return CurrentFuturesInventoryRead(None, None, "client_unavailable")
    try:
        current = await _current(repository, selected, purpose)
        require_actual_native_user(client, selected, user)
        if current.available:
            return current
        prepared = await prepare_native_private_owner(repository, client, tracker)
        try:
            require_actual_native_user(client, selected, user)
            await _read_with_prepared_owner(repository, client, prepared, purpose, tracker)
        finally:
            prepared.close()
        current = await _current(repository, selected, purpose)
        require_actual_native_user(client, selected, user)
        return current
    except Exception:
        return CurrentFuturesInventoryRead(None, None, "unverifiable")
