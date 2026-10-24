"""Own authenticated product reads after actual successful same-key account preparation.

The initial owner preparation performs the real key and accounts reads. Product
ordering then uses its own committed attempt and terminal outcome, never a
pending key-only accounts attempt. Source failures retain fixed metadata only.
This producer does not establish order access, funds or supported-product policy.
"""

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from uuid import uuid7

from kraken.futures import User

from snapper.application.trade.native_account_owner_preflight import require_actual_native_user
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_product_capture import FuturesProductWriteResult
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_read_store import BoundFuturesProductAttempt
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import _append_futures_product_outcome
from snapper.data.futures_product_read_store import begin_futures_product_read
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    GuardedFuturesProductReceipt,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    read_futures_trading_instruments_wire,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker


@dataclass(frozen=True, slots=True)
class _ProductOperation:
    """Actual committed product attempt and producer-owned stable result identity."""

    attempt: BoundFuturesProductAttempt
    outcome_identity: ExecutionEvidenceIdentity


def _identity(
    tracker: SequenceTracker, selection: ExecutionClientSelection
) -> ExecutionEvidenceIdentity:
    """Allocate product evidence provenance independently of financial or accounts-read sequences."""
    return ExecutionEvidenceIdentity(
        public_id=str(uuid7()),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(f"native.products.{selection.wallet_public_id}"),
    )


async def _start_product_operation(
    repository: AccountingRepository, token: PreparedKeyToken, tracker: SequenceTracker
) -> _ProductOperation:
    """Commit the actual product attempt before any product endpoint request may start."""
    safe_reference = token.require_proof_reference()
    reference = ProductReadKeyReference.model_validate(safe_reference.model_dump(mode="python"))
    attempt = await begin_futures_product_read(
        repository, reference, _identity(tracker, reference.client_selection)
    )
    return _ProductOperation(attempt, _identity(tracker, reference.client_selection))


async def _record_product_result(
    repository: AccountingRepository,
    operation: _ProductOperation,
    result: FuturesProductWriteResult,
) -> BoundFuturesProductOutcome:
    """Atomically retain one original result without replacing its source clock or earlier outcome."""
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, operation.attempt.payload.client_selection.wallet_public_id
        )
        outcome = await _append_futures_product_outcome(
            session, operation.attempt.public_id, result, operation.outcome_identity
        )
        await session.commit()
        return outcome


async def _read_product_result(
    client: KrakenFuturesExchangeClient,
    user: User,
    token: PreparedKeyToken,
    operation: _ProductOperation,
) -> GuardedFuturesProductReceipt:
    """Own the same actual SDK client across route waits and final-request key validation."""
    selected = operation.attempt.payload.client_selection
    require_actual_native_user(client, selected, user)
    client._record_rest_call()
    received = await client._dispatch_routed_rest(
        operation="get_native_trading_instruments_source",
        kind="private_idempotent_read",
        target=futures_sdk_proxy_target(user),
        sync_call=lambda: read_futures_trading_instruments_wire(
            user, selected, token, operation.attempt.public_id
        ),
    )
    require_actual_native_user(client, selected, user)
    return received


async def _complete_product_operation(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    user: User,
    token: PreparedKeyToken,
    operation: _ProductOperation,
) -> BoundFuturesProductOutcome:
    """Record transport refusal without replacing a possibly already committed captured outcome."""
    result: FuturesProductWriteResult
    try:
        result = await _read_product_result(client, user, token, operation)
    except asyncio.CancelledError:
        with suppress(Exception):
            await _record_product_result(
                repository, operation, FuturesProductReadFailure(reason="cancelled")
            )
        raise
    except Exception:
        result = FuturesProductReadFailure(reason="transport_error")
    return await _record_product_result(repository, operation, result)


async def read_native_futures_products(
    repository: AccountingRepository, client: KrakenFuturesExchangeClient, tracker: SequenceTracker
) -> BoundFuturesProductOutcome:
    """Prepare the actual owner, then commit and execute a separately ordered product read.

    Args:
        repository: Actual source transaction provider sharing the wallet execution fence.
        client: Same actual selected Futures client and instantiated SDK User.
        tracker: Actual producer session and evidence sequence allocator.

    Returns:
        Committed captured or failed product result, without an order permission or freshness claim.

    Raises:
        ValueError: If owner preparation or actual product attempt commitment cannot be established.
        asyncio.CancelledError: Cancellation preserves the committed outcome or an honest pending
            attempt; a failure write is attempted only before original outcome capture starts.
    """
    selection = client.execution_client_selection()
    user = client._user_client
    if selection is None or user is None:
        raise FuturesProductBindingError("futures_product_actual_client_missing")
    prepared = await prepare_native_private_owner(repository, client, tracker)
    try:
        require_actual_native_user(client, selection, user)
        token = prepared.require_token()
        operation = await _start_product_operation(repository, token, tracker)
        return await _complete_product_operation(repository, client, user, token, operation)
    finally:
        prepared.close()
