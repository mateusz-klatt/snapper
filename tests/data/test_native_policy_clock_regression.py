"""Unrun actual reserved-writer interval witnesses under an explicitly controlled post-lock UTC clock.

These cases do not assert or simulate a PostgreSQL row-lock wait. They target
historical interval preservation if the actual sampled clock is no later than
an already committed native financial owner decision.
"""

from datetime import UTC
from datetime import datetime
from datetime import tzinfo
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update

import snapper.data.financial_account_owner_policy_writer as policy_writer
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Setting
from snapper.data.native_cash_history_v2 import get_bound_native_cash_event_v2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from tests.helpers.native_cash_accounting_v2 import NativeCashScenarioV2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_scenario_v2

pytestmark = pytest.mark.asyncio


class _SampledClock:
    """Control only the reserved writer's actual post-lock clock, without replacing its database path."""

    current = datetime(2026, 10, 24, tzinfo=UTC)
    samples: list[datetime] = []

    @staticmethod
    def now(timezone: tzinfo | None = None) -> datetime:
        """Return the explicitly selected earlier or equal revision sample."""
        _SampledClock.samples.append(_SampledClock.current)
        return (
            _SampledClock.current
            if timezone is None
            else _SampledClock.current.astimezone(timezone)
        )


@pytest.mark.parametrize("boundary", ["before", "equal"])
async def test_reserved_revocation_preserves_committed_owner_decision_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    """Do not close a valid original policy before an actual committed financial decision.

    Given: Genuine SDK/private ownership, native authority and cash posting retain one original owner decision.
    When: The real reserved revocation writer samples a controlled time before or equal to that decision.
    Then: It must refuse or preserve the original interval and complete cash replay; no backdated close is valid.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(2800),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        decision = original.payload.source.owner_evidence.decided_at
        reference = original.payload.source.owner_evidence.policy_reference
        assert reference.timestamp < decision
        _SampledClock.current = (
            reference.timestamp + (decision - reference.timestamp) / 2
            if boundary == "before"
            else decision
        )
        assert reference.timestamp < _SampledClock.current <= decision
        if boundary == "before":
            assert _SampledClock.current < decision
        _SampledClock.samples = []
        monkeypatch.setattr(policy_writer, "datetime", _SampledClock)
        refusal: str | None = None
        try:
            await policy_writer.write_financial_owner_policy(
                scenario.source.repository,
                FinancialOwnerPolicyChange(
                    policy=FinancialAccountOwnerPolicy(assignments=()),
                    updated_by="isolated-clock-boundary-revocation",
                ),
                scenario.source.tracker,
            )
        except FinancialAccountOwnerUnavailableError as error:
            refusal = str(error)
        assert _SampledClock.samples == [_SampledClock.current]
        async with scenario.source.repository.session() as session:
            retained = (
                await session.scalars(
                    select(Setting).where(
                        Setting.public_id == reference.public_id,
                        Setting.timestamp == reference.timestamp,
                        Setting.session_id == reference.session_id,
                        Setting.sequence_id == reference.sequence_id,
                    )
                )
            ).one()
            assert retained.known_to > decision
            assert retained.known_to == KNOWN_TO_MAX
            assert refusal == "financial_owner_policy_clock_not_advanced"
            rebound = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        assert rebound.payload == original.payload
        assert rebound.row.content_digest == original.row.content_digest


async def test_ordinary_later_revocation_preserves_real_cash_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Allow ordinary explicit revocation after the actual last financial decision.

    Given: Genuine private ownership and cash source facts committed under the current owner policy.
    When: The real reserved writer samples its normal later wall clock and records an empty policy.
    Then: The prior exact interval still contains its original decision and the unchanged cash graph replays.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(2800),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        changed = await policy_writer.write_financial_owner_policy(
            scenario.source.repository,
            FinancialOwnerPolicyChange(
                policy=FinancialAccountOwnerPolicy(assignments=()),
                updated_by="isolated-later-revocation",
            ),
            scenario.source.tracker,
        )
        assert changed.changed and changed.reference.timestamp > original.row.timestamp
        async with scenario.source.repository.session() as session:
            rebound = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
            old = (
                await session.scalars(
                    select(Setting).where(
                        Setting.public_id
                        == original.payload.source.owner_evidence.policy_reference.public_id,
                        Setting.timestamp
                        == original.payload.source.owner_evidence.policy_reference.timestamp,
                    )
                )
            ).one()
        assert old.known_to == changed.reference.timestamp
        assert old.known_to > original.payload.source.owner_evidence.decided_at
        assert rebound.payload == original.payload


async def test_other_historical_policy_decision_does_not_pin_the_active_empty_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compare complete original policy references instead of attributing every prior fact to the active row.

    Given: A genuine cash fact retains policy one, followed by a correctly later empty policy two.
    When: An explicit later revision restores the original owner while closing policy two.
    Then: Policy one's original closed interval and cash fact survive without being relabelled as policy two.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(2800),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        revoked = await policy_writer.write_financial_owner_policy(
            scenario.source.repository,
            FinancialOwnerPolicyChange(
                policy=FinancialAccountOwnerPolicy(assignments=()),
                updated_by="isolated-first-revocation",
            ),
            scenario.source.tracker,
        )
        restored = await policy_writer.write_financial_owner_policy(
            scenario.source.repository,
            FinancialOwnerPolicyChange(
                policy=scenario.policy, updated_by="isolated-owner-restoration"
            ),
            scenario.source.tracker,
        )
        assert restored.changed and restored.reference != revoked.reference
        assert restored.reference.timestamp > revoked.reference.timestamp > original.row.timestamp
        async with scenario.source.repository.session() as session:
            rebound = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        assert rebound.payload.source.owner_evidence.policy_reference != revoked.reference
        assert rebound.payload.source.owner_evidence.policy_reference != restored.reference
        assert rebound.payload == original.payload


async def _restore_event_timestamp(
    scenario: NativeCashScenarioV2, public_id: str, timestamp: datetime
) -> None:
    """Simulate an isolated privileged restore and reinstall the genuine immutable UPDATE guard."""
    async with scenario.source.repository.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await connection.execute(
            update(ContractAccountingEvent)
            .where(ContractAccountingEvent.public_id == public_id)
            .values(timestamp=timestamp)
        )
        await connection.run_sync(
            install_contract_accounting_table_triggers, "contract_accounting_events"
        )


async def test_changed_indexed_clock_cannot_hide_a_policy_decision_from_close_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rebind every actual row before trusting timestamp projections when closing a policy.

    Given: A genuine cash fact's indexed timestamp is changed by a privileged isolated restore with guards reinstalled.
    When: Reserved revocation samples the original decision clock while the indexed row appears older.
    Then: Complete row binding refuses the close, and restoring the original projection preserves cash replay.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(2800),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        decision = original.payload.source.owner_evidence.decided_at
        reference = original.payload.source.owner_evidence.policy_reference
        await _restore_event_timestamp(scenario, first.public_id, reference.timestamp)
        try:
            _SampledClock.current = decision
            _SampledClock.samples = []
            monkeypatch.setattr(policy_writer, "datetime", _SampledClock)
            with pytest.raises(
                FinancialAccountOwnerUnavailableError, match="retained_history_unproven"
            ):
                await policy_writer.write_financial_owner_policy(
                    scenario.source.repository,
                    FinancialOwnerPolicyChange(
                        policy=FinancialAccountOwnerPolicy(assignments=()),
                        updated_by="isolated-damaged-projection-revocation",
                    ),
                    scenario.source.tracker,
                )
            assert _SampledClock.samples == [decision]
            async with scenario.source.repository.session() as session:
                setting = (
                    await session.scalars(
                        select(Setting).where(
                            Setting.public_id == reference.public_id,
                            Setting.timestamp == reference.timestamp,
                        )
                    )
                ).one()
            assert setting.known_to == KNOWN_TO_MAX
        finally:
            await _restore_event_timestamp(scenario, first.public_id, decision)
        async with scenario.source.repository.session() as session:
            rebound = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        assert rebound.payload == original.payload
