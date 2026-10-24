"""Bind actual source graphs for native financial source evidence.

The prospective sidecar reader is a mandatory dependency, not a nominal
metadata fallback. This historical reader commits nothing and cannot substitute
for the writer's current, same-transaction owner-policy admission.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialSourceEvidence
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.financial_account_owner_policy_store import get_bound_financial_owner_evidence
from snapper.data.models import AccountingRawReceipt
from snapper.data.private_source_ownership_store import BoundPrivateSourceOwnership
from snapper.data.private_source_ownership_store import get_bound_private_source_ownership

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


class NativeFinancialBindingError(ValueError):
    """Actual native source, owner policy, sidecar or immutable financial lineage is unproven."""


@dataclass(frozen=True, slots=True)
class BoundNativeFinancialSource:
    """Actual source facts with independently rebound UID, without a current financial grant."""

    raw: RawAccountingObservation
    entry: FuturesAccountLogEntry
    authority: BoundAccountAuthority
    ownership: BoundPrivateSourceOwnership


def require_native_financial_binding(condition: bool, reason: str) -> None:
    """Refuse interpretation without discarding already committed raw evidence or pending holds.

    Args:
        condition: Actual graph invariant already evaluated by the caller.
        reason: Fixed public failure code containing no original source or authentication material.
    """
    if not condition:
        raise NativeFinancialBindingError(reason)


def verify_private_ownership_reference(
    reference: PrivateSourceOwnershipReference, bound: BoundPrivateSourceOwnership
) -> None:
    """Compare every reference field to the actual immutable sidecar row and payload.

    Args:
        reference: Exact retained public reference, never an ownership capability by construction.
        bound: Result of the mandatory actual sidecar/raw/key-proof/root reader.
    """
    source = bound.payload
    actual = (
        bound.public_id,
        bound.content_digest,
        source.raw_receipt_public_id,
        source.raw_content_digest,
        source.target_json_pointer,
        bound.recorded_at,
    )
    expected = (
        reference.public_id,
        reference.content_digest,
        reference.raw_receipt_public_id,
        reference.raw_content_digest,
        reference.target_json_pointer,
        reference.recorded_at,
    )
    require_native_financial_binding(
        actual == expected, "native_financial_private_reference_mismatch"
    )


def _verify_actual_client(
    authority: NativeAccountAuthorityPayload,
    raw: RawAccountingObservation,
    owned: BoundPrivateSourceOwnership,
) -> None:
    """Keep actual capture-wallet/environment identity while allowing proven same-UID key rotation."""
    selected = owned.payload.frame_context.client_selection
    scope = authority.scope
    actual = (
        raw.scope.wallet_public_id,
        raw.scope.exchange,
        raw.scope.mode,
        raw.scope.environment,
        raw.credential_public_id,
        raw.credential_timestamp,
        raw.route_revision,
    )
    expected = (
        selected.wallet_public_id,
        selected.exchange,
        selected.mode,
        selected.environment,
        selected.credential_public_id,
        selected.credential_timestamp,
        client_selection_digest(selected),
    )
    require_native_financial_binding(actual == expected, "native_financial_actual_client_mismatch")
    require_native_financial_binding(
        (selected.wallet_public_id, selected.exchange, selected.mode, selected.environment)
        == (scope.wallet_public_id, scope.exchange, scope.mode, scope.environment)
        and raw.scope.margin_account in {None, scope.margin_account},
        "native_financial_actual_scope_mismatch",
    )
    require_native_financial_binding(
        owned.account_uid == authority.venue_account_id,
        "native_financial_authenticated_uid_mismatch",
    )


def _verify_native_source_identity(
    raw: RawAccountingObservation, entry: FuturesAccountLogEntry
) -> None:
    """Check a declared provider ID against the actual selected child without using ID density."""
    identity = {"booking_uid": entry.booking_uid, "venue_execution_id": entry.execution}.get(
        raw.source_identity_kind or "unproven"
    )
    require_native_financial_binding(
        raw.source_identity_kind in {None, "unproven"} or raw.source_identity == identity,
        "native_financial_declared_source_identity_mismatch",
    )


async def bind_native_financial_receipt(
    session: AsyncSession,
    authority_reference: AccountAuthorityReference,
    raw_receipt_public_id: str,
    known_at: datetime,
) -> BoundNativeFinancialSource:
    """Bind actual incoming source facts before deciding whether an immutable event already exists.

    Idempotent replay must still validate a newly supplied receipt and UID. It
    need not reactivate a revoked policy or refresh expired account observations
    just to return an existing committed event. New financial writes separately
    require current authority and the locked current owner decision.

    Args:
        session: Actual read or caller-owned financial transaction.
        authority_reference: Explicit historical native family/identity/digest/recording clock.
        raw_receipt_public_id: Actual selected incoming receipt with mandatory private ownership.
        known_at: Actual current interpretation cutoff, independent of an original event's timestamp.

    Returns:
        Actual native account/log/private-source facts, without current financial admission.
    """
    clock = _INSTANT.validate_python(known_at)
    reference = AccountAuthorityReference.model_validate(authority_reference)
    require_native_financial_binding(
        reference.family == "native_accounts_v1" and reference.recorded_at <= clock,
        "native_financial_explicit_account_reference_unproven",
    )
    authority = await get_bound_account_authority_reference(session, reference)
    native = NativeAccountAuthorityPayload.model_validate(authority.payload)
    require_native_financial_binding(
        native.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2",
        "native_financial_account_source_unprepared",
    )
    owned = await get_bound_private_source_ownership(session, raw_receipt_public_id, clock)
    raw = await get_bound_accounting_observation(session, raw_receipt_public_id)
    require_native_financial_binding(
        owned.payload.raw_receipt_public_id == raw_receipt_public_id
        and accounting_observation_digest(raw) == owned.payload.raw_content_digest
        and raw.target_json_pointer == owned.payload.target_json_pointer,
        "native_financial_raw_reference_mismatch",
    )
    retained_at = await session.scalar(
        select(AccountingRawReceipt.timestamp).where(
            AccountingRawReceipt.public_id == raw_receipt_public_id
        )
    )
    require_native_financial_binding(
        retained_at is not None and retained_at <= clock and raw.observed_at <= clock,
        "native_financial_raw_knowledge_after_interpretation",
    )
    _verify_actual_client(native, raw, owned)
    entry = parse_futures_account_log(raw)
    _verify_native_source_identity(raw, entry)
    require_native_financial_binding(
        entry.margin_account == native.scope.margin_account,
        "native_financial_literal_margin_mismatch",
    )
    return BoundNativeFinancialSource(raw, entry, authority, owned)


async def bind_native_financial_source(
    session: AsyncSession, evidence: NativeFinancialSourceEvidence
) -> BoundNativeFinancialSource:
    """Bind a financial event's actual original account, owner policy and private source graph.

    The current writer separately admits the fresh current account and locked
    current owner before creating a new event. Historical replay rebinds the
    exact original closed or open policy interval and all six sidecar copies.

    Args:
        session: Actual caller-owned historical or financial transaction.
        evidence: Complete original immutable source/policy shape at the actual posting clock.

    Returns:
        The actual original source facts; no current permission follows from historical binding.
    """
    proven = NativeFinancialSourceEvidence.model_validate(evidence)
    await get_bound_financial_owner_evidence(session, proven.owner_evidence)
    source = await bind_native_financial_receipt(
        session,
        proven.authority_reference,
        proven.ownership_reference.raw_receipt_public_id,
        proven.recorded_at,
    )
    require_native_financial_binding(
        source.authority.payload == proven.authority,
        "native_financial_authority_payload_mismatch",
    )
    verify_private_ownership_reference(proven.ownership_reference, source.ownership)
    return source
