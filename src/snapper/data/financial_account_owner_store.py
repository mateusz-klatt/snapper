"""Read explicit owner configuration and rebind supported native financial history.

Actual UID-linked native-v2 cash and fully bound position books pin their
original financial owner. Legacy history remains an explicit migration
refusal. Safe read identity roots discover aliases without assigning
or transferring any financial ownership.
"""

from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import FinancialAccountOwnerKey
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import financial_account_owner_key_digest
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.financial_account_owner import require_financial_account_owner_assignment
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.financial_account_owner_policy_store import get_bound_financial_owner_evidence
from snapper.data.financial_account_owner_policy_store import load_current_financial_owner_policy
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards
from snapper.data.native_financial_owner_history_v2 import require_uid_native_financial_history_v2
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.session_context import session_no_autoflush

FINANCIAL_OWNER_LOCK_NAMESPACE = "snapper_financial_owner"
_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


def _require(condition: bool, reason: str) -> None:
    """Refuse unsupported ownership without turning absent history or source proof into permission."""
    if not condition:
        raise FinancialAccountOwnerUnavailableError(reason)


async def _owner_barrier(session: AsyncSession, key: FinancialAccountOwnerKey) -> None:
    """Serialize aliases after their wallet and shared accounting barriers, never before them."""
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:owner_key))"),
            {
                "namespace": FINANCIAL_OWNER_LOCK_NAMESPACE,
                "owner_key": financial_account_owner_key_digest(key),
            },
        )


async def _uid_linked_wallets(
    session: AsyncSession, key: FinancialAccountOwnerKey, decided_at: datetime
) -> tuple[str, ...]:
    """Rebind initial safe owner roots once per alias instead of every historical profile read."""
    rows = (
        (
            await session.execute(
                select(NativeAccountIdentityProof).where(
                    NativeAccountIdentityProof.exchange == key.exchange,
                    NativeAccountIdentityProof.mode == key.mode,
                    NativeAccountIdentityProof.environment == key.environment,
                    NativeAccountIdentityProof.account_uid == key.account_uid,
                    NativeAccountIdentityProof.owner_root_public_id.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )
    wallets: set[str] = set()
    for row in rows:
        bound = await get_bound_native_account_identity(session, row.public_id)
        proof = bound.payload
        _require(
            proof.recorded_at <= decided_at and proof.observation.account_uid == key.account_uid,
            "financial_owner_history_native_authority_mismatch",
        )
        wallets.add(proof.observation.client_selection.wallet_public_id)
    return tuple(sorted(wallets))


async def _require_supported_history(
    session: AsyncSession,
    key: FinancialAccountOwnerKey,
    decided_at: datetime,
    owner_wallet: str | None = None,
) -> None:
    """Rebind supported native-v2 owner history while retaining explicit legacy/migration refusal."""
    if owner_wallet is not None:
        await require_native_contract_accounting_guards(session)
    wallets = await _uid_linked_wallets(session, key, decided_at)
    _require(bool(wallets), "financial_owner_history_native_authority_missing")
    if owner_wallet is None:
        for model in (
            ContractAccountingEvent,
            AccountingNativeSourceClaim,
            ContractAccountingAnchor,
        ):
            found = await session.scalar(
                select(model.public_id)
                .where(
                    model.wallet_public_id.in_(wallets),
                    model.exchange == key.exchange,
                    model.mode == key.mode,
                    model.environment == key.environment,
                )
                .limit(1)
            )
            _require(found is None, "financial_owner_native_history_or_migration_unproven")
        return
    try:
        await require_uid_native_financial_history_v2(
            session, key, wallets, owner_wallet, decided_at
        )
    except NativeFinancialBindingError:
        raise FinancialAccountOwnerUnavailableError(
            "financial_owner_native_history_or_migration_unproven"
        ) from None


async def load_current_financial_owner_evidence(
    session: AsyncSession, authority_reference: AccountAuthorityReference, decided_at: datetime
) -> FinancialAccountOwnerEvidence:
    """Bind current configured ownership in the caller's transaction without enabling posting.

    The caller holds its actual wallet fence and invokes this before staging
    financial rows. Lock order is wallet, shared accounting, external owner,
    then active Setting row. A valid result proves configuration and supported
    original-owner history only; missing native-v2 or private transport proof must
    still refuse financial promotion. Indexed privileged restore damage belongs
    to the mandatory full startup audit, not an unbounded per-post global scan.

    Args:
        session: Real caller-owned transaction with its actual wallet fence already held.
        authority_reference: Exact family-qualified retained native authority and row clock.
        decided_at: Actual current decision clock, retained independently from provider economic time.

    Returns:
        Rebound configuration evidence with no implicit financial grant or account activation.
    """
    clock = _INSTANT.validate_python(decided_at)
    reference = AccountAuthorityReference.model_validate(authority_reference)
    _require(reference.family == "native_accounts_v1", "financial_owner_native_authority_required")
    _require(
        not any(
            isinstance(
                row,
                ContractAccountingEvent | AccountingNativeSourceClaim | ContractAccountingAnchor,
            )
            for row in session.new
        ),
        "financial_owner_staged_financial_rows_unsupported",
    )
    with session_no_autoflush(session):
        await require_native_contract_accounting_guards(session)
        bound = await get_bound_account_authority_reference(session, reference)
        native = NativeAccountAuthorityPayload.model_validate(bound.payload)
        key = financial_account_owner_key_from_authority(native)
        _require(bound.recorded_at <= clock, "financial_owner_authority_not_known_at_decision")
        await _owner_barrier(session, key)
        policy = await load_current_financial_owner_policy(session, clock)
        assignment = require_financial_account_owner_assignment(
            policy.policy, key, native.scope.wallet_public_id
        )
        await _require_supported_history(session, key, clock, assignment.owner_wallet_public_id)
        evidence = FinancialAccountOwnerEvidence(
            policy_reference=policy.reference,
            policy=policy.policy,
            assignment=assignment,
            authority_reference=reference,
            decided_at=clock,
        )
        return await get_bound_financial_owner_evidence(session, evidence)
