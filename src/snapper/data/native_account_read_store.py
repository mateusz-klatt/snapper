"""Order native account reads before network I/O and bind their immutable outcomes.

The newest committed attempt owns current-read eligibility even while pending
or failed. Completion order never restores an older response. These records do
not establish collateral, release financial holds or authorize order sending.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from uuid import NAMESPACE_URL
from uuid import uuid5

from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import NativeAccountReadOutcomePayload
from snapper.core.native_account_read import NativeAccountReadResult
from snapper.core.native_account_read import canonical_native_account_read_attempt
from snapper.core.native_account_read import canonical_native_account_read_outcome
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.core.native_account_read import native_account_read_family_digest
from snapper.core.native_account_read import native_account_read_outcome_digest
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none

_SOURCE_CHANNEL = "kraken_futures.rest.accounts.v1"
_ERROR_CHANNEL = "kraken_futures.rest.accounts.http_error.v1"
_SOURCE_CHANNELS = frozenset(
    {
        _SOURCE_CHANNEL,
        "kraken_futures.rest.accounts.prepared-key.v2",
    }
)
_ERROR_CHANNELS = frozenset(
    {
        _ERROR_CHANNEL,
        "kraken_futures.rest.accounts.http_error.prepared-key.v2",
    }
)


class NativeAccountReadBindingError(ValueError):
    """An immutable read identity, source or current public selection cannot be proven."""


@dataclass(frozen=True, slots=True)
class BoundNativeAccountReadAttempt:
    """One verified committed request whose ordinal is independent of response arrival."""

    public_id: str
    payload: NativeAccountReadAttemptPayload


@dataclass(frozen=True, slots=True)
class BoundNativeAccountReadOutcome:
    """One verified result and its durable public identity, without current authority."""

    public_id: str
    payload: NativeAccountReadOutcomePayload


@dataclass(frozen=True, slots=True)
class LatestNativeAccountRead:
    """The newest request, with explicit absence when its outcome remains unknown."""

    attempt: BoundNativeAccountReadAttempt
    outcome: BoundNativeAccountReadOutcome | None


def native_account_response_public_id(attempt_public_id: str) -> str:
    """Keep original local HTTP response identity inseparable from its actual committed request.

    Args:
        attempt_public_id: Validated canonical UUID of the independently retained read attempt.

    Returns:
        The deterministic raw receipt UUID shared by the closed producer and bound reader.
    """
    return str(uuid5(NAMESPACE_URL, f"snapper:native-account-response:v1:{attempt_public_id}"))


def _require(condition: bool, reason: str) -> None:
    """Refuse a contradictory stored binding without selecting an older fallback."""
    if not condition:
        raise NativeAccountReadBindingError(reason)


def _family_query(selection: ExecutionClientSelection) -> Select[NativeAccountReadAttempt]:
    """Find the latest direct account family independently of digest and temporal filters."""
    return (
        select(NativeAccountReadAttempt)
        .where(
            NativeAccountReadAttempt.wallet_public_id == selection.wallet_public_id,
            NativeAccountReadAttempt.exchange == selection.exchange,
            NativeAccountReadAttempt.mode == selection.mode,
            NativeAccountReadAttempt.environment == selection.environment,
        )
        .order_by(NativeAccountReadAttempt.family_sequence.desc())
        .limit(1)
    )


def _scope_projection(
    row: NativeAccountReadAttempt | NativeAccountReadOutcome,
    payload: NativeAccountReadAttemptPayload,
) -> None:
    """Verify every indexed family coordinate against canonical public request evidence."""
    selected = payload.client_selection
    _require(
        (
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.scope_digest,
            row.family_sequence,
            row.known_to,
        )
        == (
            selected.wallet_public_id,
            selected.exchange,
            selected.mode,
            selected.environment,
            None,
            native_account_read_family_digest(selected),
            payload.family_sequence,
            KNOWN_TO_MAX,
        ),
        "native_read_scope_projection_mismatch",
    )


def _bound_attempt(row: NativeAccountReadAttempt) -> BoundNativeAccountReadAttempt:
    """Reload canonical material and check its independently stored indexes and clock."""
    try:
        payload = NativeAccountReadAttemptPayload.model_validate_json(row.canonical_payload)
    except ValidationError as error:
        raise NativeAccountReadBindingError("native_read_attempt_payload_invalid") from error
    _scope_projection(row, payload)
    _require(
        (
            row.client_selection_digest,
            row.started_at,
            row.timestamp,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            client_selection_digest(payload.client_selection),
            payload.started_at,
            payload.started_at,
            native_account_read_attempt_digest(payload),
            canonical_native_account_read_attempt(payload),
        ),
        "native_read_attempt_projection_mismatch",
    )
    return BoundNativeAccountReadAttempt(row.public_id, payload)


async def get_bound_native_account_read_attempt(
    session: AsyncSession, public_id: str
) -> BoundNativeAccountReadAttempt:
    """Read original request evidence without claiming that it is still the latest.

    Args:
        session: Caller-owned transaction, with no new fence or implicit commit.
        public_id: Exact immutable request identity.

    Returns:
        Verified canonical request and its original durable identity.
    """
    row = (
        await session.execute(
            select(NativeAccountReadAttempt).where(NativeAccountReadAttempt.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise NativeAccountReadBindingError("native_read_attempt_missing")
    return _bound_attempt(row)


async def _verify_current_selection(
    session: AsyncSession, selected: ExecutionClientSelection, now: datetime
) -> None:
    """Use only public current wallet and credential metadata, without loading secret payloads."""
    origins = {
        "production": "https://futures.kraken.com",
        "demo": "https://demo-futures.kraken.com",
    }
    _require(
        selected.exchange == "kraken_futures"
        and selected.mode == "live"
        and selected.environment in origins
        and selected.public_api_origin == origins[selected.environment],
        "native_read_client_source_unproven",
    )
    wallet_candidates = (
        await session.execute(
            select(Wallet.is_paper, Wallet.known_to).where(
                Wallet.public_id == selected.wallet_public_id,
                Wallet.known_to > now,
                Wallet.timestamp <= now,
            )
        )
    ).all()
    wallet = one_or_none([row for row in wallet_candidates if is_open_ended_version(row.known_to)])
    _require(
        wallet is not None and wallet.is_paper is False,
        "native_read_live_wallet_missing",
    )
    credential_candidates = (
        await session.execute(
            select(
                WalletCredential.public_id,
                WalletCredential.timestamp,
                WalletCredential.credential_type,
                WalletCredential.known_to,
            ).where(
                WalletCredential.wallet_public_id == selected.wallet_public_id,
                WalletCredential.exchange == selected.exchange,
                WalletCredential.known_to > now,
                WalletCredential.timestamp <= now,
            )
        )
    ).all()
    credential = one_or_none(
        [row for row in credential_candidates if is_open_ended_version(row.known_to)]
    )
    if credential is None:
        raise NativeAccountReadBindingError("native_read_selected_credential_missing")
    _require(
        (credential.public_id, credential.timestamp, credential.credential_type)
        == (selected.credential_public_id, selected.credential_timestamp, "api_key_secret"),
        "native_read_selected_credential_changed",
    )


async def _begin_under_fence(
    session: AsyncSession,
    selection: ExecutionClientSelection,
    identity: ExecutionEvidenceIdentity,
) -> BoundNativeAccountReadAttempt:
    """Allocate the next local ordinal only after the real wallet fence is held."""
    now = datetime.now(UTC)
    await _verify_current_selection(session, selection, now)
    existing = (
        await session.execute(
            select(NativeAccountReadAttempt).where(
                NativeAccountReadAttempt.public_id == identity.public_id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        bound = _bound_attempt(existing)
        _require(
            bound.payload.client_selection == selection, "native_read_attempt_identity_changed"
        )
        _require(bound.payload.started_at <= now, "native_read_knowledge_clock_regressed")
        return bound
    latest = (await session.execute(_family_query(selection))).scalar_one_or_none()
    previous = _bound_attempt(latest).payload if latest is not None else None
    _require(
        previous is None or previous.started_at <= now, "native_read_knowledge_clock_regressed"
    )
    payload = NativeAccountReadAttemptPayload(
        client_selection=selection,
        family_sequence=1 if previous is None else previous.family_sequence + 1,
        started_at=now,
    )
    row = NativeAccountReadAttempt(
        **identity.model_dump(mode="python"),
        timestamp=now,
        wallet_public_id=selection.wallet_public_id,
        exchange=selection.exchange,
        mode=selection.mode,
        environment=selection.environment,
        margin_account=None,
        scope_digest=native_account_read_family_digest(selection),
        family_sequence=payload.family_sequence,
        client_selection_digest=client_selection_digest(selection),
        started_at=now,
        content_digest=native_account_read_attempt_digest(payload),
        canonical_payload=canonical_native_account_read_attempt(payload),
    )
    session.add(row)
    await session.flush()
    return _bound_attempt(row)


async def begin_native_account_read(
    repository: AccountingRepository,
    selection: ExecutionClientSelection,
    identity: ExecutionEvidenceIdentity,
) -> BoundNativeAccountReadAttempt:
    """Commit an actual read attempt before its producer is allowed to start network I/O.

    Args:
        repository: Real transaction provider and shared wallet execution fence.
        selection: Actual selected public client metadata, independently checked in the database.
        identity: Stable request identity for retry after an uncertain commit acknowledgement.

    Returns:
        The committed immutable attempt, retaining its ordinal on exact retry.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, selected.wallet_public_id)
        bound = await _begin_under_fence(session, selected, proven)
        await session.commit()
        return bound


async def _verify_source(session: AsyncSession, payload: NativeAccountReadOutcomePayload) -> None:
    """Bind retained response bytes to the original selected request and separate source clocks."""
    source = payload.source_reference
    if source is None:
        return
    raw = await get_bound_accounting_observation(session, source.raw_receipt_public_id)
    row = (
        await session.execute(
            select(AccountingRawReceipt).where(
                AccountingRawReceipt.public_id == source.raw_receipt_public_id
            )
        )
    ).scalar_one()
    _require(
        (source.raw_receipt_public_id, raw.source_identity, raw.source_identity_kind)
        == (
            native_account_response_public_id(payload.attempt_public_id),
            payload.attempt_public_id,
            "unproven",
        ),
        "native_read_source_attempt_mismatch",
    )
    selected = payload.attempt.client_selection
    _require(
        (
            raw.scope.wallet_public_id,
            raw.scope.exchange,
            raw.scope.mode,
            raw.scope.environment,
            raw.scope.margin_account,
            raw.credential_public_id,
            raw.credential_timestamp,
            raw.route_revision,
            raw.kind,
            raw.representation,
            raw.target_json_pointer,
            raw.observed_at,
            row.timestamp,
            row.known_to,
            accounting_observation_digest(raw),
        )
        == (
            selected.wallet_public_id,
            selected.exchange,
            selected.mode,
            selected.environment,
            None,
            selected.credential_public_id,
            selected.credential_timestamp,
            client_selection_digest(selected),
            "account_cash",
            "wire_bytes_base64",
            None,
            source.source_observed_at,
            source.source_recorded_at,
            KNOWN_TO_MAX,
            source.raw_content_digest,
        ),
        "native_read_source_binding_mismatch",
    )
    captured = isinstance(payload.result, NativeAccountReadCapturedSource)
    _require(
        raw.channel in _SOURCE_CHANNELS | _ERROR_CHANNELS
        and (not captured or raw.channel in _SOURCE_CHANNELS),
        "native_read_source_channel_mismatch",
    )


async def _bound_outcome(
    session: AsyncSession, row: NativeAccountReadOutcome
) -> BoundNativeAccountReadOutcome:
    """Verify the full result, its original request and any actual retained raw source."""
    try:
        payload = NativeAccountReadOutcomePayload.model_validate_json(row.canonical_payload)
    except ValidationError as error:
        raise NativeAccountReadBindingError("native_read_outcome_payload_invalid") from error
    attempt = await get_bound_native_account_read_attempt(session, payload.attempt_public_id)
    _require(attempt.payload == payload.attempt, "native_read_outcome_attempt_mismatch")
    _scope_projection(row, payload.attempt)
    source = payload.source_reference
    _require(
        (
            row.attempt_public_id,
            row.attempt_content_digest,
            row.completed_at,
            row.timestamp,
            row.result_kind,
            row.raw_receipt_public_id,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            attempt.public_id,
            native_account_read_attempt_digest(attempt.payload),
            payload.completed_at,
            payload.completed_at,
            payload.result.kind,
            source.raw_receipt_public_id if source else None,
            native_account_read_outcome_digest(payload),
            canonical_native_account_read_outcome(payload),
        ),
        "native_read_outcome_projection_mismatch",
    )
    await _verify_source(session, payload)
    return BoundNativeAccountReadOutcome(row.public_id, payload)


async def get_bound_native_account_read_outcome(
    session: AsyncSession, attempt_public_id: str
) -> BoundNativeAccountReadOutcome | None:
    """Read one attempt's immutable outcome while preserving a genuinely pending state.

    Args:
        session: Caller-owned transaction without an implicit completion or fence.
        attempt_public_id: Exact original request identity, independently required to exist.

    Returns:
        Verified historical outcome, or absence when no outcome has been retained.
    """
    await get_bound_native_account_read_attempt(session, attempt_public_id)
    row = (
        await session.execute(
            select(NativeAccountReadOutcome).where(
                NativeAccountReadOutcome.attempt_public_id == attempt_public_id
            )
        )
    ).scalar_one_or_none()
    return await _bound_outcome(session, row) if row is not None else None


async def append_native_account_read_outcome(
    session: AsyncSession,
    attempt_public_id: str,
    result: NativeAccountReadResult,
    identity: ExecutionEvidenceIdentity,
) -> BoundNativeAccountReadOutcome:
    """Append the actual response result in the producer's fenced raw-capture transaction.

    Args:
        session: Caller-owned wallet-fenced transaction, committed together with original raw bytes.
        attempt_public_id: Previously committed request; late outcomes remain historical.
        result: Original retained source reference or an explicit failure without invented evidence.
        identity: Stable immutable outcome identity, independent of the attempt and raw receipt.

    Returns:
        The staged verified outcome; this helper never commits or grants current authority.
    """
    attempt = await get_bound_native_account_read_attempt(session, attempt_public_id)
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    payload = NativeAccountReadOutcomePayload(
        attempt_public_id=attempt.public_id,
        attempt_content_digest=native_account_read_attempt_digest(attempt.payload),
        attempt=attempt.payload,
        completed_at=datetime.now(UTC),
        result=result,
    )
    await _verify_source(session, payload)
    existing = await get_bound_native_account_read_outcome(session, attempt.public_id)
    if existing is not None:
        _require(
            existing.public_id == proven.public_id and existing.payload.result == payload.result,
            "native_read_outcome_identity_changed",
        )
        return existing
    collision = (
        await session.execute(
            select(NativeAccountReadOutcome.public_id).where(
                NativeAccountReadOutcome.public_id == proven.public_id
            )
        )
    ).scalar_one_or_none()
    _require(collision is None, "native_read_outcome_identity_reused")
    selected = attempt.payload.client_selection
    source = payload.source_reference
    row = NativeAccountReadOutcome(
        **proven.model_dump(mode="python"),
        timestamp=payload.completed_at,
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
        margin_account=None,
        scope_digest=native_account_read_family_digest(selected),
        family_sequence=attempt.payload.family_sequence,
        attempt_public_id=attempt.public_id,
        attempt_content_digest=payload.attempt_content_digest,
        completed_at=payload.completed_at,
        result_kind=payload.result.kind,
        raw_receipt_public_id=source.raw_receipt_public_id if source else None,
        content_digest=native_account_read_outcome_digest(payload),
        canonical_payload=canonical_native_account_read_outcome(payload),
    )
    session.add(row)
    await session.flush()
    return await _bound_outcome(session, row)


async def get_latest_native_account_read(
    session: AsyncSession, selection: ExecutionClientSelection, known_at: datetime
) -> LatestNativeAccountRead | None:
    """Select the newest committed request without falling back after failure or interruption.

    Args:
        session: Caller-owned transaction; financial consumers must hold their account fence.
        selection: Actual currently selected public client revision.
        known_at: Explicit local knowledge boundary, independent of provider or response order.

    Returns:
        The latest request and optional result, or absence if the family has never been queried.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    _require(known_at.utcoffset() is not None, "native_read_knowledge_clock_not_aware")
    await _verify_current_selection(session, selected, known_at)
    row = (await session.execute(_family_query(selected))).scalar_one_or_none()
    if row is None:
        return None
    attempt = _bound_attempt(row)
    _require(attempt.payload.client_selection == selected, "native_read_latest_client_changed")
    _require(attempt.payload.started_at <= known_at, "native_read_attempt_not_yet_known")
    outcome = await get_bound_native_account_read_outcome(session, attempt.public_id)
    _require(
        outcome is None or outcome.payload.completed_at <= known_at,
        "native_read_outcome_not_yet_known",
    )
    await _require_uncontested_source(session, outcome)
    return LatestNativeAccountRead(attempt, outcome)


async def _require_uncontested_source(
    session: AsyncSession, outcome: BoundNativeAccountReadOutcome | None
) -> None:
    """Refuse current reuse after a conflicting receipt without rewriting historical outcomes."""
    source = outcome.payload.source_reference if outcome else None
    if source is None:
        return
    conflict = (
        await session.execute(
            select(AccountingRawReceipt.public_id)
            .where(
                AccountingRawReceipt.claimed_public_id == source.raw_receipt_public_id,
                AccountingRawReceipt.public_id != source.raw_receipt_public_id,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    _require(conflict is None, "native_read_source_identity_contested")
