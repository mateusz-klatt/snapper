"""Rebind exact Settings revisions and native authorities without issuing a financial permit.

This narrow historical reader deliberately imports no accounting claim or
financial event reader. A future native claim reader can verify retained policy
evidence here without creating a cycle through current financial admission.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.financial_account_owner import financial_account_owner_policy_digest
from snapper.core.financial_account_owner import parse_financial_account_owner_policy
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.models import Setting
from snapper.data.models import is_open_ended_version

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)
_SETTING_CATEGORY = "financial_owner"


@dataclass(frozen=True, slots=True)
class BoundFinancialOwnerPolicy:
    """Actual versioned configuration, with no source-transport or financial admission verdict."""

    reference: FinancialAccountOwnerPolicyReference
    policy: FinancialAccountOwnerPolicy


def _require(condition: bool, reason: str) -> None:
    """Keep policy failure reasons fixed without including original configured JSON values."""
    if not condition:
        raise FinancialAccountOwnerUnavailableError(reason)


def _bound_setting(row: Setting, decided_at: datetime) -> BoundFinancialOwnerPolicy:
    """Validate actual plaintext policy metadata and the original SCD2 decision interval."""
    _require(
        row.key == FINANCIAL_OWNER_SETTING_KEY
        and row.category == _SETTING_CATEGORY
        and not row.is_encrypted,
        "financial_owner_setting_metadata_unsupported",
    )
    _require(
        row.timestamp <= decided_at < row.known_to,
        "financial_owner_setting_not_known_at_decision",
    )
    try:
        policy = parse_financial_account_owner_policy(row.value)
    except ValueError:
        raise FinancialAccountOwnerUnavailableError(
            "financial_owner_setting_payload_invalid"
        ) from None
    return BoundFinancialOwnerPolicy(
        FinancialAccountOwnerPolicyReference(
            public_id=row.public_id,
            timestamp=row.timestamp,
            session_id=row.session_id,
            sequence_id=row.sequence_id,
            policy_content_digest=financial_account_owner_policy_digest(policy),
        ),
        policy,
    )


async def load_current_financial_owner_policy(
    session: AsyncSession, decided_at: datetime
) -> BoundFinancialOwnerPolicy:
    """Lock the actual active Setting revision inside the caller's financial transaction.

    The caller first acquires the wallet, shared accounting and external-owner
    barriers. The PostgreSQL row share lock blocks an ordinary SCD2 close until
    financial commit; SQLite callers already hold the actual write reservation.

    Args:
        session: Caller-owned real transaction, never a cached Settings service or separate session.
        decided_at: Actual retained decision knowledge clock after its wallet fence.

    Returns:
        The exact active policy and provenance, without any financial permission.
    """
    clock = _INSTANT.validate_python(decided_at)
    rows = [
        row
        for row, known_to in (
            await session.execute(
                select(Setting, Setting.known_to)
                .where(Setting.key == FINANCIAL_OWNER_SETTING_KEY, Setting.known_to > clock)
                .with_for_update(read=True)
            )
        ).all()
        if is_open_ended_version(known_to)
    ]
    _require(len(rows) == 1, "financial_owner_current_setting_missing_or_ambiguous")
    return _bound_setting(rows[0], clock)


async def get_bound_financial_owner_evidence(
    session: AsyncSession, evidence: FinancialAccountOwnerEvidence
) -> FinancialAccountOwnerEvidence:
    """Rebind an exact historical policy decision without requiring that its revision stay active.

    Args:
        session: Actual interpretation transaction whose caller owns rollback and commit.
        evidence: Original complete evidence, including the exact closed or open Setting revision.

    Returns:
        The same fully rebound configuration evidence; replay grants no current financial permit.
    """
    proven = FinancialAccountOwnerEvidence.model_validate(evidence)
    reference = proven.policy_reference
    rows = (
        (
            await session.execute(
                select(Setting).where(
                    Setting.key == reference.setting_key,
                    Setting.public_id == reference.public_id,
                    Setting.timestamp == reference.timestamp,
                    Setting.session_id == reference.session_id,
                    Setting.sequence_id == reference.sequence_id,
                )
            )
        )
        .scalars()
        .all()
    )
    _require(len(rows) == 1, "financial_owner_historical_setting_missing_or_ambiguous")
    bound = _bound_setting(rows[0], proven.decided_at)
    _require(
        bound.reference == reference and bound.policy == proven.policy,
        "financial_owner_historical_setting_changed",
    )
    authority = await get_bound_account_authority_reference(session, proven.authority_reference)
    native = NativeAccountAuthorityPayload.model_validate(authority.payload)
    _require(
        financial_account_owner_key_from_authority(native) == proven.assignment.owner_key
        and native.scope.wallet_public_id == proven.assignment.owner_wallet_public_id,
        "financial_owner_assignment_authority_mismatch",
    )
    return proven
