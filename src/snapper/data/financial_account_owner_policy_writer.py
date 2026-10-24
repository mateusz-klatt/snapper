"""Write explicit financial owner configuration using the actual post-lock revision clock.

Configuration changes supply no private transport ownership or financial grant.
Unsupported financial history requires source-backed migration before assignment;
an explicit empty policy records revocation without deleting its prior history.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import canonical_financial_account_owner_policy
from snapper.core.financial_account_owner import financial_account_owner_policy_digest
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_account_owner_policy_store import load_current_financial_owner_policy
from snapper.data.financial_account_owner_store import _require_supported_history
from snapper.data.financial_owner_policy_clock import require_financial_owner_policy_close_clock
from snapper.data.models import Setting
from snapper.data.models import is_open_ended_version
from snapper.data.repository import close_and_insert
from snapper.messaging.infrastructure.publisher import SequenceTracker

FINANCIAL_OWNER_POLICY_WRITER_NAMESPACE = "snapper_financial_owner_policy"


class FinancialOwnerPolicyChange(BaseModel):
    """Explicit administrator input without a pre-sampled timestamp or financial permission flag."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )
    policy: FinancialAccountOwnerPolicy
    updated_by: EvidenceText = Field(max_length=64)


@dataclass(frozen=True, slots=True)
class FinancialOwnerPolicyWriteResult:
    """The actual retained revision; changed indicates configuration mutation, never trade permission."""

    reference: FinancialAccountOwnerPolicyReference
    changed: bool


async def _lock_reserved_policy(session: AsyncSession) -> Setting | None:
    """Lock the actual current policy before sampling its authoritative revision clock.

    Database time only narrows selection; the separately projected exact marker
    proves membership even when the ORM entity is cached. The revision clock is
    still sampled once by the caller after this row-lock operation returns.
    """
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:setting_key))"),
            {
                "namespace": FINANCIAL_OWNER_POLICY_WRITER_NAMESPACE,
                "setting_key": FINANCIAL_OWNER_SETTING_KEY,
            },
        )
    rows = [
        row
        for row, known_to in (
            await session.execute(
                select(Setting, Setting.known_to)
                .where(
                    Setting.key == FINANCIAL_OWNER_SETTING_KEY,
                    Setting.known_to > func.current_timestamp(),
                )
                .with_for_update()
            )
        ).all()
        if is_open_ended_version(known_to)
    ]
    if len(rows) > 1:
        raise FinancialAccountOwnerUnavailableError("financial_owner_current_setting_ambiguous")
    return rows[0] if rows else None


async def _write_locked_revision(
    session: AsyncSession,
    change: FinancialOwnerPolicyChange,
    tracker: SequenceTracker,
    clock: datetime,
) -> FinancialAccountOwnerPolicyReference:
    """Reuse generic SCD2 only after the real row lock and actual post-wait clock were acquired."""
    row = await close_and_insert(
        session,
        Setting,
        [Setting.key == FINANCIAL_OWNER_SETTING_KEY],
        {
            "key": FINANCIAL_OWNER_SETTING_KEY,
            "value": canonical_financial_account_owner_policy(change.policy),
            "category": "financial_owner",
            "description": "Explicit owner of authenticated native account financial history",
            "is_encrypted": False,
            "updated_by": change.updated_by,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence("settings"),
        },
        bus_time=clock,
    )
    await session.flush()
    return FinancialAccountOwnerPolicyReference(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        policy_content_digest=financial_account_owner_policy_digest(change.policy),
    )


async def write_financial_owner_policy(
    repository: AccountingRepository,
    change: FinancialOwnerPolicyChange,
    tracker: SequenceTracker,
) -> FinancialOwnerPolicyWriteResult:
    """Commit an explicitly authorized policy after the actual row lock, without financial activation.

    Permission belongs to the existing authenticated CONFIGURE_SYSTEM/CSRF
    application boundary. This internal producer must not be exposed as an
    unauthenticated endpoint. No wallet or external-owner lock is acquired after
    the Setting lock; existing financial FOR SHARE owners must commit first.

    Args:
        repository: Real isolated or application transaction provider with its actual backend barrier.
        change: Explicit strict policy and authenticated updater supplied by trusted application code.
        tracker: Actual provenance allocated for an attempted new revision before commit.
            Transaction rollback may leave gaps in this producer sequence.

    Returns:
        The actual new or unchanged version reference; no current trading or posting capability.
    """
    proven = FinancialOwnerPolicyChange.model_validate(change)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await require_contract_accounting_guards(session)
        previous = await _lock_reserved_policy(session)
        clock = datetime.now(UTC)
        if previous is not None:
            existing = await load_current_financial_owner_policy(session, clock)
            if existing.policy == proven.policy:
                await session.commit()
                return FinancialOwnerPolicyWriteResult(existing.reference, False)
            if previous.timestamp >= clock:
                raise FinancialAccountOwnerUnavailableError(
                    "financial_owner_policy_clock_not_advanced"
                )
            await require_financial_owner_policy_close_clock(session, existing.reference, clock)
        for assignment in proven.policy.assignments:
            await _require_supported_history(
                session, assignment.owner_key, clock, assignment.owner_wallet_public_id
            )
        reference = await _write_locked_revision(session, proven, tracker, clock)
        await session.commit()
        return FinancialOwnerPolicyWriteResult(reference, True)
