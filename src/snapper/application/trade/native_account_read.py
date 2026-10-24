"""Own native account HTTP reads and atomically retain their original state evidence.

Only this fixed authenticated account-read operation uses the state capture
path. Ordinary execution/account-log capture continues to retain financial
holds. No caller-provided balance DTO or generic hold-bypass flag is accepted.
"""

import asyncio
import base64
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Literal
from uuid import uuid7

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_native_accounts import parse_futures_native_accounts
from snapper.application.trade.native_account_owner_preflight import PreparedNativeAccountOwner
from snapper.application.trade.native_account_owner_preflight import prove_native_account_owner
from snapper.application.trade.native_account_owner_preflight import require_actual_native_user
from snapper.application.trade.native_account_read_owner import NativeAccountOwnerPreparation
from snapper.application.trade.native_account_read_owner import NativeAccountReadOwner
from snapper.application.trade.owned_native_source_read import read_owned_native_source
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import NativeAccountReadFailure
from snapper.core.native_account_read import NativeAccountReadResult
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import _capture_under_fence
from snapper.data.contract_accounting_store import _insert_receipt
from snapper.data.contract_accounting_store import _ObservationInsert
from snapper.data.contract_accounting_store import _same_receipt
from snapper.data.models import AccountingRawReceipt
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadAttempt
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.native_account_read_store import append_native_account_read_outcome
from snapper.data.native_account_read_store import begin_native_account_read
from snapper.data.native_account_read_store import native_account_response_public_id
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    NativeAccountsBodyPrivacyError,
)
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    native_accounts_body_is_classified,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    FUTURES_PREPARED_ACCOUNTS_CHANNEL,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    FuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    GuardedFuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    read_futures_prepared_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    require_guarded_accounts_wire,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.messaging.infrastructure.publisher import SequenceTracker


@dataclass(frozen=True, slots=True)
class _ReadOperation:
    """One committed attempt and internally generated identities for its atomic source/result."""

    attempt: BoundNativeAccountReadAttempt
    outcome_identity: ExecutionEvidenceIdentity
    raw_identity: ExecutionEvidenceIdentity
    key_identity: ExecutionEvidenceIdentity


def _identity(
    tracker: SequenceTracker, selection: ExecutionClientSelection
) -> ExecutionEvidenceIdentity:
    """Allocate producer-owned identity without exposing a caller-controlled source selector."""
    return ExecutionEvidenceIdentity(
        public_id=str(uuid7()),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(f"native.accounts.{selection.wallet_public_id}"),
    )


async def _start_operation(
    repository: AccountingRepository,
    selection: ExecutionClientSelection,
    tracker: SequenceTracker,
) -> _ReadOperation:
    """Commit before any transport can run and preserve a deterministic source link to that request."""
    attempt = await begin_native_account_read(repository, selection, _identity(tracker, selection))
    raw_identity = _identity(tracker, selection)
    return _ReadOperation(
        attempt=attempt,
        outcome_identity=_identity(tracker, selection),
        key_identity=_identity(tracker, selection),
        raw_identity=ExecutionEvidenceIdentity(
            public_id=native_account_response_public_id(attempt.public_id),
            session_id=raw_identity.session_id,
            sequence_id=raw_identity.sequence_id,
        ),
    )


def _original_state(
    receipt: GuardedFuturesAccountsWireReceipt, operation: _ReadOperation
) -> _ObservationInsert:
    """Bind the original fixed-endpoint response to the exact request that performed its I/O."""
    wire, key_proof = require_guarded_accounts_wire(receipt)
    selected = operation.attempt.payload.client_selection
    if (
        key_proof.read_attempt_public_id != operation.attempt.public_id
        or key_proof.client_selection != selected
        or wire.client_selection != selected
        or wire.request_started_at < operation.attempt.payload.started_at
    ):
        raise NativeAccountReadBindingError("native_read_actual_transport_attempt_mismatch")
    if wire.status_code != 200 or not native_accounts_body_is_classified(wire.entity_bytes):
        raise NativeAccountReadBindingError("native_read_unclassified_state_body")
    raw = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
        ),
        route_revision=client_selection_digest(selected),
        credential_public_id=selected.credential_public_id,
        credential_timestamp=selected.credential_timestamp,
        kind="account_cash",
        channel=FUTURES_PREPARED_ACCOUNTS_CHANNEL,
        representation="wire_bytes_base64",
        material=base64.b64encode(wire.entity_bytes).decode("ascii"),
        observed_at=wire.observed_at,
        source_identity=operation.attempt.public_id,
        source_identity_kind="unproven",
    )
    provenance = AccountingWriteProvenance(
        **operation.raw_identity.model_dump(), timestamp=datetime.now(UTC)
    )
    return _ObservationInsert(
        raw,
        provenance,
        canonical_accounting_observation(raw),
        accounting_observation_digest(raw),
    )


async def _retain_known_state(
    session: AsyncSession, data: _ObservationInsert
) -> AccountingRawReceipt | None:
    """Retain fixed-endpoint state while routing identity conflicts through unchanged held capture."""
    existing = (
        await session.execute(
            select(AccountingRawReceipt).where(
                AccountingRawReceipt.public_id == data.provenance.public_id
            )
        )
    ).scalar_one_or_none()
    if existing is None:
        return await _insert_receipt(session, data, data.provenance.public_id)
    if _same_receipt(existing, data):
        return existing
    await _capture_under_fence(session, data)
    return None


def _interpret_retained_state(
    record: AccountingRawReceipt,
    data: _ObservationInsert,
    receipt: FuturesAccountsWireReceipt,
) -> NativeAccountReadResult:
    """Interpret only classified successful state bytes; unsupported financial shapes remain retained."""
    source = NativeAccountReadCapturedSource(
        raw_receipt_public_id=record.public_id,
        raw_content_digest=record.content_digest,
        request_started_at=receipt.request_started_at,
        source_observed_at=data.observation.observed_at,
        source_recorded_at=record.timestamp,
    )
    try:
        parse_futures_native_accounts(data.observation)
    except ValueError:
        return NativeAccountReadFailure(reason="source_invalid", source=source)
    return source


def _matches_guarded_owner(
    actual: BoundNativeAccountIdentityProof, reference: PreparedKeyProofReference
) -> bool:
    """Rebind every original actual prepared-key proof and root projection, not only its UUID."""
    proof = actual.payload
    digest = native_account_identity_digest(proof)
    root = proof.owner_root
    return (
        actual.public_id == reference.public_id
        and digest == reference.content_digest
        and proof.attempt_public_id == reference.read_attempt_public_id
        and proof.observation.client_selection == reference.client_selection
        and proof.observation.account_uid == reference.account_uid
        and proof.observation.observed_at == reference.observed_at
        and proof.recorded_at == reference.recorded_at
        and (actual.public_id if root is None else root.public_id)
        == reference.key_owner_root_public_id
        and (digest if root is None else root.content_digest)
        == reference.key_owner_root_content_digest
    )


async def _capture_response(
    repository: AccountingRepository,
    operation: _ReadOperation,
    receipt: GuardedFuturesAccountsWireReceipt,
) -> BoundNativeAccountReadOutcome:
    """Commit original source and its result atomically under the shared wallet fence."""
    wire, key_proof = require_guarded_accounts_wire(receipt)
    data = _original_state(receipt, operation)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, operation.attempt.payload.client_selection.wallet_public_id
        )
        proof = await find_native_account_identity(session, operation.attempt.public_id)
        if (
            proof is None
            or not proof.payload.matches_owner
            or not _matches_guarded_owner(proof, key_proof)
            or proof.payload.recorded_at > wire.request_started_at
        ):
            raise NativeAccountReadBindingError("native_read_identity_preflight_unproven")
        record = await _retain_known_state(session, data)
        if record is None:
            await session.commit()
            raise NativeAccountReadBindingError("native_read_raw_identity_conflict")
        result = _interpret_retained_state(record, data, wire)
        outcome = await append_native_account_read_outcome(
            session, operation.attempt.public_id, result, operation.outcome_identity
        )
        await session.commit()
        return outcome


async def _record_failure(
    repository: AccountingRepository,
    operation: _ReadOperation,
    reason: Literal["transport_error", "cancelled", "source_invalid", "http_error"],
) -> BoundNativeAccountReadOutcome:
    """Retain an honest failure without inventing a response body or exception-text evidence."""
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, operation.attempt.payload.client_selection.wallet_public_id
        )
        outcome = await append_native_account_read_outcome(
            session,
            operation.attempt.public_id,
            NativeAccountReadFailure(reason=reason),
            operation.outcome_identity,
        )
        await session.commit()
        return outcome


async def _read_native_accounts_with_preparation[OwnerT: NativeAccountReadOwner](
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    tracker: SequenceTracker,
    prepare: NativeAccountOwnerPreparation[OwnerT],
) -> tuple[BoundNativeAccountReadOutcome, OwnerT | None]:
    """Share one actual key/account read; callers either close or explicitly transfer its owner token."""
    selection = client.execution_client_selection()
    user = client._user_client
    if selection is None or user is None:
        raise NativeAccountReadBindingError("native_read_actual_client_missing")
    operation = await _start_operation(repository, selection, tracker)
    owner: OwnerT | None = None
    handed_off = False
    try:
        try:
            owner = await prepare(
                repository, client, operation.attempt, operation.key_identity, user
            )
            if not owner.proof.payload.matches_owner:
                return (
                    await _record_failure(repository, operation, "source_invalid"),
                    None,
                )
            require_actual_native_user(client, selection, user)
            client._record_rest_call()
            token = owner.require_token()
            receipt = await read_owned_native_source(
                client,
                user,
                "get_native_accounts_source",
                lambda: read_futures_prepared_accounts_wire(user, selection, token),
            )
            require_actual_native_user(client, selection, user)
        except asyncio.CancelledError:
            with suppress(Exception):
                await _record_failure(repository, operation, "cancelled")
            raise
        except NativeAccountsBodyPrivacyError as refused:
            return await _record_failure(repository, operation, refused.privacy_reason), None
        except Exception:
            return await _record_failure(repository, operation, "transport_error"), None
        outcome = await _capture_response(repository, operation, receipt)
        if outcome.payload.result.kind != "captured":
            return outcome, None
        handed_off = True
        return outcome, owner
    finally:
        if not handed_off and owner is not None:
            owner.close()


async def _read_native_futures_accounts_with_owner(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    tracker: SequenceTracker,
) -> tuple[BoundNativeAccountReadOutcome, PreparedNativeAccountOwner | None]:
    """Preserve the existing native-v1 owner preparation and runtime token handoff unchanged."""
    return await _read_native_accounts_with_preparation(
        repository, client, tracker, prove_native_account_owner
    )


async def read_native_futures_accounts(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    tracker: SequenceTracker,
) -> BoundNativeAccountReadOutcome:
    """Own the same guarded actual read without handing a runtime token to public-only callers.

    Args:
        repository: Actual transactional store sharing the account execution fence.
        client: Actual selected Futures client with its original public credential identity.
        tracker: Producer session and evidence identity allocator.

    Returns:
        Actual captured or failed read outcome; every unhanded transient key token is closed.
    """
    outcome, owner = await _read_native_futures_accounts_with_owner(repository, client, tracker)
    try:
        return outcome
    finally:
        if owner is not None:
            owner.close()
