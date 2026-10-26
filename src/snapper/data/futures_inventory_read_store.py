"""Retain independent inventory reads without profile selection or financial authority.

Standalone attempts and outcomes use the additive 0066 inventory schema,
original native key binding, guarded capture boundary and exact projections.
Newest request ordering survives pending failures, credential changes and late
completion. Original source bytes are fully interpreted before retained-body
digest verification; independent endpoints never imply an atomic account book.
"""

import base64
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import TypeAdapter
from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_inventory_capture import FuturesInventoryWriteResult
from snapper.data.futures_inventory_capture import inventory_result_for_storage
from snapper.data.futures_inventory_key_binding import FuturesInventoryBindingError
from snapper.data.futures_inventory_key_binding import get_bound_inventory_read_key
from snapper.data.futures_inventory_read_projection import inventory_read_attempt_projection
from snapper.data.futures_inventory_read_projection import inventory_read_outcome_projection
from snapper.data.models import FuturesInventoryReadAttempt as InventoryAttemptRow
from snapper.data.models import FuturesInventoryReadOutcome as InventoryOutcomeRow
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_read_store import _verify_current_selection

_PURPOSE: TypeAdapter[InventoryReadPurpose] = TypeAdapter(InventoryReadPurpose)


@dataclass(frozen=True, slots=True)
class BoundFuturesInventoryAttempt:
    """One actual immutable endpoint request with its independently rebound original key proof."""

    public_id: str
    payload: FuturesInventoryReadAttempt


@dataclass(frozen=True, slots=True)
class BoundFuturesInventoryOutcome:
    """One actual immutable result preserving its original request and source knowledge clocks."""

    public_id: str
    payload: FuturesInventoryReadOutcome


@dataclass(frozen=True, slots=True)
class LatestFuturesInventoryRead:
    """Newest endpoint request, including explicit absence of a committed terminal result."""

    attempt: BoundFuturesInventoryAttempt
    outcome: BoundFuturesInventoryOutcome | None


def _require(condition: bool, reason: str) -> None:
    """Refuse contradictions without choosing an older or differently scoped inventory source."""
    if not condition:
        raise FuturesInventoryBindingError(reason)


def _purpose(value: InventoryReadPurpose) -> InventoryReadPurpose:
    """Reject unknown endpoint purposes before constructing a family query or inserting evidence."""
    try:
        return _PURPOSE.validate_python(value, strict=True)
    except ValidationError:
        raise FuturesInventoryBindingError("futures_inventory_purpose_invalid") from None


def _family_query(
    selection: ExecutionClientSelection, purpose: InventoryReadPurpose
) -> Select[InventoryAttemptRow]:
    """Choose newest physical family before checking digest, credential, knowledge or outcome."""
    return (
        select(InventoryAttemptRow)
        .where(
            InventoryAttemptRow.wallet_public_id == selection.wallet_public_id,
            InventoryAttemptRow.exchange == selection.exchange,
            InventoryAttemptRow.mode == selection.mode,
            InventoryAttemptRow.environment == selection.environment,
            InventoryAttemptRow.purpose == purpose,
        )
        .order_by(InventoryAttemptRow.family_sequence.desc())
        .limit(1)
    )


def _verify_projection(
    row: InventoryAttemptRow | InventoryOutcomeRow, expected: dict[str, object]
) -> None:
    """Check every indexed copy, canonical payload and permanent temporal marker against evidence."""
    _require(
        is_open_ended_version(row.known_to)
        and all(getattr(row, name) == value for name, value in expected.items()),
        "futures_inventory_projection_mismatch",
    )
    try:
        ExecutionEvidenceIdentity(
            public_id=row.public_id, session_id=row.session_id, sequence_id=row.sequence_id
        )
    except ValidationError:
        raise FuturesInventoryBindingError("futures_inventory_insertion_identity_invalid") from None


def _require_original_identity(
    row: InventoryAttemptRow | InventoryOutcomeRow, identity: ExecutionEvidenceIdentity
) -> None:
    """Require original public, session and sequence identity after an uncertain acknowledgement."""
    _require(
        (row.public_id, row.session_id, row.sequence_id)
        == (identity.public_id, identity.session_id, identity.sequence_id),
        "futures_inventory_insertion_identity_changed",
    )


async def _bound_attempt(
    session: AsyncSession, row: InventoryAttemptRow
) -> BoundFuturesInventoryAttempt:
    """Rebind actual immutable key/root history before accepting any canonical attempt projection."""
    try:
        payload = FuturesInventoryReadAttempt.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise FuturesInventoryBindingError("futures_inventory_attempt_payload_invalid") from None
    await get_bound_inventory_read_key(session, payload.key_proof)
    _verify_projection(row, inventory_read_attempt_projection(payload))
    return BoundFuturesInventoryAttempt(row.public_id, payload)


async def get_bound_futures_inventory_attempt(
    session: AsyncSession, public_id: str
) -> BoundFuturesInventoryAttempt:
    """Load actual historical inventory evidence without granting freshness or current order access.

    Args:
        session: Caller-owned transaction without implicit commitment or wallet fencing.
        public_id: Exact original immutable inventory request identity.

    Returns:
        Rebound original request, including its actual endpoint purpose and safe key/root reference.
    """
    row = await session.scalar(
        select(InventoryAttemptRow).where(InventoryAttemptRow.public_id == public_id)
    )
    if row is None:
        raise FuturesInventoryBindingError("futures_inventory_attempt_missing")
    return await _bound_attempt(session, row)


def _verify_original_source(payload: FuturesInventoryReadOutcome) -> None:
    """Interpret complete original endpoint bytes before any retained-body digest is verified."""
    source = payload.result
    if not isinstance(source, FuturesInventoryReadSource):
        return
    try:
        entity = base64.b64decode(source.body, validate=True)
        if payload.attempt.purpose == "kraken_futures.open_orders":
            parse_futures_open_orders(entity)
        else:
            parse_futures_open_positions(entity)
    except (ValueError, UnicodeError, RecursionError, ArithmeticError):
        raise FuturesInventoryBindingError("futures_inventory_original_source_invalid") from None


async def _bound_outcome(
    session: AsyncSession, row: InventoryOutcomeRow
) -> BoundFuturesInventoryOutcome:
    """Rebind the actual original request/proof and semantic source before hashing retained bytes."""
    try:
        payload = FuturesInventoryReadOutcome.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise FuturesInventoryBindingError("futures_inventory_outcome_payload_invalid") from None
    attempt = await get_bound_futures_inventory_attempt(session, payload.attempt_public_id)
    _require(attempt.payload == payload.attempt, "futures_inventory_actual_attempt_mismatch")
    _verify_original_source(payload)
    _verify_projection(row, inventory_read_outcome_projection(payload))
    return BoundFuturesInventoryOutcome(row.public_id, payload)


async def get_bound_futures_inventory_outcome(
    session: AsyncSession, attempt_public_id: str
) -> BoundFuturesInventoryOutcome | None:
    """Load the original endpoint result while preserving pending absence as a distinct state.

    Args:
        session: Caller-owned transaction without an implicit freshness or financial claim.
        attempt_public_id: Actual original inventory request, independently required to exist.

    Returns:
        Rebound immutable result, or absence when that actual request is still pending.
    """
    await get_bound_futures_inventory_attempt(session, attempt_public_id)
    row = await session.scalar(
        select(InventoryOutcomeRow).where(
            InventoryOutcomeRow.attempt_public_id == attempt_public_id
        )
    )
    return None if row is None else await _bound_outcome(session, row)


async def _begin_under_fence(
    session: AsyncSession,
    reference: InventoryReadKeyReference,
    purpose: InventoryReadPurpose,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesInventoryAttempt:
    """Allocate endpoint ordering once, using original request knowledge sampled under the fence."""
    now = datetime.now(UTC)
    selected = reference.client_selection
    await _verify_current_selection(session, selected, now)
    await get_bound_inventory_read_key(session, reference)
    existing = await session.scalar(
        select(InventoryAttemptRow).where(InventoryAttemptRow.public_id == identity.public_id)
    )
    if existing is not None:
        _require_original_identity(existing, identity)
        bound = await _bound_attempt(session, existing)
        _require(
            bound.payload.key_proof == reference and bound.payload.purpose == purpose,
            "futures_inventory_attempt_identity_changed",
        )
        _require(bound.payload.started_at <= now, "futures_inventory_knowledge_clock_regressed")
        return bound
    latest = await session.scalar(_family_query(selected, purpose))
    previous = None if latest is None else (await _bound_attempt(session, latest)).payload
    _require(
        previous is None or previous.started_at <= now,
        "futures_inventory_knowledge_clock_regressed",
    )
    payload = FuturesInventoryReadAttempt(
        purpose=purpose,
        client_selection=selected,
        key_proof=reference,
        family_sequence=1 if previous is None else previous.family_sequence + 1,
        started_at=now,
    )
    row = InventoryAttemptRow(
        **identity.model_dump(mode="python"), **inventory_read_attempt_projection(payload)
    )
    session.add(row)
    await session.flush()
    return await _bound_attempt(session, row)


async def begin_futures_inventory_read(
    repository: AccountingRepository,
    reference: InventoryReadKeyReference,
    purpose: InventoryReadPurpose,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesInventoryAttempt:
    """Commit one independent endpoint request before permitting its actual inventory GET.

    Args:
        repository: Actual transaction provider sharing the existing wallet execution fence.
        reference: Original committed safe proof/root, independently rebound before insertion.
        purpose: Explicit endpoint with ordering independent of the other inventory endpoint.
        identity: Producer-owned stable insertion identity retained through uncertain commit replies.

    Returns:
        Committed original request; exact retries preserve its identity, ordinal and start clock.
    """
    proven = InventoryReadKeyReference.model_validate(reference)
    selected_purpose = _purpose(purpose)
    stamp = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, proven.client_selection.wallet_public_id
        )
        result = await _begin_under_fence(session, proven, selected_purpose, stamp)
        await session.commit()
        return result


async def _append_futures_inventory_outcome(
    session: AsyncSession,
    attempt_public_id: str,
    result: FuturesInventoryWriteResult,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesInventoryOutcome:
    """Append once inside the closed producer's wallet-fenced transaction, without committing."""
    _require(session.in_transaction(), "futures_inventory_result_transaction_required")
    attempt = await get_bound_futures_inventory_attempt(session, attempt_public_id)
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    now = datetime.now(UTC)
    _require(attempt.payload.started_at <= now, "futures_inventory_knowledge_clock_regressed")
    existing_row = await session.scalar(
        select(InventoryOutcomeRow).where(
            InventoryOutcomeRow.attempt_public_id == attempt_public_id
        )
    )
    if existing_row is not None:
        _require_original_identity(existing_row, proven)
        existing = await _bound_outcome(session, existing_row)
        original = inventory_result_for_storage(
            result, attempt.public_id, attempt.payload, allow_stored_source=True
        )
        _require(
            existing.public_id == proven.public_id and existing.payload.result == original,
            "futures_inventory_outcome_identity_changed",
        )
        _require(
            existing.payload.completed_at <= now, "futures_inventory_knowledge_clock_regressed"
        )
        return existing
    collision = await session.scalar(
        select(InventoryOutcomeRow.public_id).where(
            InventoryOutcomeRow.public_id == proven.public_id
        )
    )
    _require(collision is None, "futures_inventory_outcome_identity_reused")
    original = inventory_result_for_storage(result, attempt.public_id, attempt.payload)
    payload = FuturesInventoryReadOutcome(
        attempt_public_id=attempt.public_id,
        attempt_content_digest=futures_inventory_attempt_digest(attempt.payload),
        attempt=attempt.payload,
        completed_at=now,
        result=original,
    )
    row = InventoryOutcomeRow(
        **proven.model_dump(mode="python"), **inventory_read_outcome_projection(payload)
    )
    session.add(row)
    await session.flush()
    return await _bound_outcome(session, row)


async def get_latest_futures_inventory_read(
    session: AsyncSession,
    selection: ExecutionClientSelection,
    purpose: InventoryReadPurpose,
    known_at: datetime,
) -> LatestFuturesInventoryRead | None:
    """Choose the newest endpoint attempt before examining source age, credential or result.

    Args:
        session: Caller-owned transaction; stronger current consumers must hold the wallet fence.
        selection: Actual current public client selection independently verified against the database.
        purpose: Original fixed endpoint, with no profile or order-access permission prerequisite.
        known_at: Trusted aware local knowledge boundary, distinct from provider/arrival clocks.

    Returns:
        Latest original request and optional result, or absence for a never-read endpoint family.
        This historical ordering view grants no freshness, financial or CREATE permission.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    selected_purpose = _purpose(purpose)
    _require(known_at.utcoffset() is not None, "futures_inventory_knowledge_clock_not_aware")
    await _verify_current_selection(session, selected, known_at)
    row = await session.scalar(_family_query(selected, selected_purpose))
    if row is None:
        return None
    attempt = await _bound_attempt(session, row)
    _require(
        attempt.payload.client_selection == selected, "futures_inventory_latest_client_changed"
    )
    _require(attempt.payload.started_at <= known_at, "futures_inventory_attempt_not_yet_known")
    outcome = await get_bound_futures_inventory_outcome(session, attempt.public_id)
    _require(
        outcome is None or outcome.payload.completed_at <= known_at,
        "futures_inventory_outcome_not_yet_known",
    )
    return LatestFuturesInventoryRead(attempt, outcome)
