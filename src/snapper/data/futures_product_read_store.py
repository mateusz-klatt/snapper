"""Retain independent product attempts and outcomes without changing accounts-read ordering.

The newest committed product attempt owns current eligibility, including while
pending or failed. Original key proof and safe HTTP entity are rebound before
historical use; neither a late result nor a retry can renew their source age.
"""

import base64
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_product_source import parse_safe_futures_product_source
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_product_read import FuturesProductReadAttempt
from snapper.core.futures_product_read import FuturesProductReadOutcome
from snapper.core.futures_product_read import FuturesProductReadResult
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.core.futures_product_read import futures_product_attempt_digest
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_product_capture import FuturesProductWriteResult
from snapper.data.futures_product_capture import product_result_for_storage
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_key_binding import get_bound_product_read_key
from snapper.data.futures_product_read_projection import product_read_attempt_projection
from snapper.data.futures_product_read_projection import product_read_outcome_projection
from snapper.data.models import FuturesProductReadAttempt as ProductAttemptRow
from snapper.data.models import FuturesProductReadOutcome as ProductOutcomeRow
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_read_store import _verify_current_selection


@dataclass(frozen=True, slots=True)
class BoundFuturesProductAttempt:
    """One actual immutable product request with independently rebound safe key proof."""

    public_id: str
    payload: FuturesProductReadAttempt


@dataclass(frozen=True, slots=True)
class BoundFuturesProductOutcome:
    """One actual terminal result whose ordering and source age remain attached to its request."""

    public_id: str
    payload: FuturesProductReadOutcome


@dataclass(frozen=True, slots=True)
class LatestFuturesProductRead:
    """Newest product request with explicit absence when its result has not committed."""

    attempt: BoundFuturesProductAttempt
    outcome: BoundFuturesProductOutcome | None


def _require(condition: bool, reason: str) -> None:
    """Refuse contradictions without choosing an older or more convenient product source."""
    if not condition:
        raise FuturesProductBindingError(reason)


def _family_query(selection: ExecutionClientSelection) -> Select[ProductAttemptRow]:
    """Select direct-family latest ordering without filtering out later credentials or future clocks."""
    return (
        select(ProductAttemptRow)
        .where(
            ProductAttemptRow.wallet_public_id == selection.wallet_public_id,
            ProductAttemptRow.exchange == selection.exchange,
            ProductAttemptRow.mode == selection.mode,
            ProductAttemptRow.environment == selection.environment,
        )
        .order_by(ProductAttemptRow.family_sequence.desc())
        .limit(1)
    )


def _verify_projection(
    row: ProductAttemptRow | ProductOutcomeRow, expected: dict[str, object]
) -> None:
    """Check every indexed copy and immutable temporal marker against canonical payload material."""
    _require(
        is_open_ended_version(row.known_to)
        and all(getattr(row, name) == value for name, value in expected.items()),
        "futures_product_projection_mismatch",
    )


def _require_original_identity(
    row: ProductAttemptRow | ProductOutcomeRow, identity: ExecutionEvidenceIdentity
) -> None:
    """Require the original insertion provenance on retries without allocating replacement identity."""
    _require(
        (row.public_id, row.session_id, row.sequence_id)
        == (identity.public_id, identity.session_id, identity.sequence_id),
        "futures_product_insertion_identity_changed",
    )


async def _bound_attempt(
    session: AsyncSession, row: ProductAttemptRow
) -> BoundFuturesProductAttempt:
    """Rebind the actual proof/root as well as every stored product attempt projection."""
    try:
        payload = FuturesProductReadAttempt.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise FuturesProductBindingError("futures_product_attempt_payload_invalid") from None
    _verify_projection(row, product_read_attempt_projection(payload))
    await get_bound_product_read_key(session, payload.key_proof)
    return BoundFuturesProductAttempt(row.public_id, payload)


async def get_bound_futures_product_attempt(
    session: AsyncSession, public_id: str
) -> BoundFuturesProductAttempt:
    """Load one historical product request without claiming it is the current latest source.

    Args:
        session: Caller-owned transaction, without an implicit commit or execution fence.
        public_id: Exact immutable product attempt identity.

    Returns:
        Independently verified canonical request and original safe-key proof/root references.
    """
    row = await session.scalar(
        select(ProductAttemptRow).where(ProductAttemptRow.public_id == public_id)
    )
    if row is None:
        raise FuturesProductBindingError("futures_product_attempt_missing")
    return await _bound_attempt(session, row)


def _verify_original_result(result: FuturesProductReadResult) -> None:
    """Reparse retained successful bytes before hashing or projecting any original source body."""
    if isinstance(result, FuturesProductReadSource):
        parse_safe_futures_product_source(base64.b64decode(result.body, validate=True))


async def _bound_outcome(
    session: AsyncSession, row: ProductOutcomeRow
) -> BoundFuturesProductOutcome:
    """Rebind the actual request and safely classify original bytes before verifying their digest."""
    try:
        payload = FuturesProductReadOutcome.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise FuturesProductBindingError("futures_product_outcome_payload_invalid") from None
    _verify_original_result(payload.result)
    _verify_projection(row, product_read_outcome_projection(payload))
    attempt = await get_bound_futures_product_attempt(session, payload.attempt_public_id)
    _require(attempt.payload == payload.attempt, "futures_product_actual_attempt_mismatch")
    return BoundFuturesProductOutcome(row.public_id, payload)


async def get_bound_futures_product_outcome(
    session: AsyncSession, attempt_public_id: str
) -> BoundFuturesProductOutcome | None:
    """Load the original terminal result while keeping a pending request distinct from a failure.

    Args:
        session: Caller-owned transaction without implicit current-state or source-age claims.
        attempt_public_id: Exact actual product attempt, independently required to exist.

    Returns:
        Verified immutable outcome, or absence when the original result has not committed.
    """
    await get_bound_futures_product_attempt(session, attempt_public_id)
    row = await session.scalar(
        select(ProductOutcomeRow).where(ProductOutcomeRow.attempt_public_id == attempt_public_id)
    )
    return None if row is None else await _bound_outcome(session, row)


async def _begin_under_fence(
    session: AsyncSession, reference: ProductReadKeyReference, identity: ExecutionEvidenceIdentity
) -> BoundFuturesProductAttempt:
    """Sample actual request knowledge only after the caller owns the real wallet execution fence."""
    now = datetime.now(UTC)
    selected = reference.client_selection
    await _verify_current_selection(session, selected, now)
    await get_bound_product_read_key(session, reference)
    existing = await session.scalar(
        select(ProductAttemptRow).where(ProductAttemptRow.public_id == identity.public_id)
    )
    if existing is not None:
        _require_original_identity(existing, identity)
        bound = await _bound_attempt(session, existing)
        _require(bound.payload.key_proof == reference, "futures_product_attempt_identity_changed")
        _require(bound.payload.started_at <= now, "futures_product_knowledge_clock_regressed")
        return bound
    latest = await session.scalar(_family_query(selected))
    previous = None if latest is None else (await _bound_attempt(session, latest)).payload
    _require(
        previous is None or previous.started_at <= now,
        "futures_product_knowledge_clock_regressed",
    )
    payload = FuturesProductReadAttempt(
        client_selection=selected,
        key_proof=reference,
        family_sequence=1 if previous is None else previous.family_sequence + 1,
        started_at=now,
    )
    row = ProductAttemptRow(
        **identity.model_dump(mode="python"), **product_read_attempt_projection(payload)
    )
    session.add(row)
    await session.flush()
    return await _bound_attempt(session, row)


async def begin_futures_product_read(
    repository: AccountingRepository,
    reference: ProductReadKeyReference,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesProductAttempt:
    """Commit an independent product request before permitting the actual product GET.

    Args:
        repository: Actual transaction provider sharing the existing wallet execution fence.
        reference: Same-operation actual safe-key proof, independently rebound before insertion.
        identity: Producer-owned stable identity for retry after uncertain commit acknowledgement.

    Returns:
        Committed request; exact retries preserve the original ordering and knowledge clock.
    """
    proven = ProductReadKeyReference.model_validate(reference)
    stamp = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, proven.client_selection.wallet_public_id
        )
        result = await _begin_under_fence(session, proven, stamp)
        await session.commit()
        return result


async def _append_futures_product_outcome(
    session: AsyncSession,
    attempt_public_id: str,
    result: FuturesProductWriteResult,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesProductOutcome:
    """Append inside the actual closed producer's wallet-fenced transaction, without committing."""
    _require(session.in_transaction(), "futures_product_result_transaction_required")
    attempt = await get_bound_futures_product_attempt(session, attempt_public_id)
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    now = datetime.now(UTC)
    existing_row = await session.scalar(
        select(ProductOutcomeRow).where(ProductOutcomeRow.attempt_public_id == attempt_public_id)
    )
    if existing_row is not None:
        _require_original_identity(existing_row, proven)
        existing = await _bound_outcome(session, existing_row)
        original = product_result_for_storage(
            result, attempt.public_id, attempt.payload, allow_stored_source=True
        )
        _require(
            existing.public_id == proven.public_id and existing.payload.result == original,
            "futures_product_outcome_identity_changed",
        )
        _require(existing.payload.completed_at <= now, "futures_product_knowledge_clock_regressed")
        return existing
    collision = await session.scalar(
        select(ProductOutcomeRow.public_id).where(ProductOutcomeRow.public_id == proven.public_id)
    )
    _require(collision is None, "futures_product_outcome_identity_reused")
    original = product_result_for_storage(result, attempt.public_id, attempt.payload)
    payload = FuturesProductReadOutcome(
        attempt_public_id=attempt.public_id,
        attempt_content_digest=futures_product_attempt_digest(attempt.payload),
        attempt=attempt.payload,
        completed_at=now,
        result=original,
    )
    row = ProductOutcomeRow(
        **proven.model_dump(mode="python"), **product_read_outcome_projection(payload)
    )
    session.add(row)
    await session.flush()
    return await _bound_outcome(session, row)


async def get_latest_futures_product_read(
    session: AsyncSession, selection: ExecutionClientSelection, known_at: datetime
) -> LatestFuturesProductRead | None:
    """Choose the newest request before checking its outcome, preserving pending and failed barriers.

    Args:
        session: Caller-owned transaction; order admission must hold the existing execution fence.
        selection: Actual current public client selection, independently verified against the database.
        known_at: Trusted local knowledge boundary, distinct from provider time and completion order.

    Returns:
        Latest original request and optional result, or absence for a family never read for products.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    _require(known_at.utcoffset() is not None, "futures_product_knowledge_clock_not_aware")
    await _verify_current_selection(session, selected, known_at)
    row = await session.scalar(_family_query(selected))
    if row is None:
        return None
    attempt = await _bound_attempt(session, row)
    _require(attempt.payload.client_selection == selected, "futures_product_latest_client_changed")
    _require(attempt.payload.started_at <= known_at, "futures_product_attempt_not_yet_known")
    outcome = await get_bound_futures_product_outcome(session, attempt.public_id)
    _require(
        outcome is None or outcome.payload.completed_at <= known_at,
        "futures_product_outcome_not_yet_known",
    )
    return LatestFuturesProductRead(attempt, outcome)
