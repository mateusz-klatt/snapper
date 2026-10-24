"""Rebind a real owner decision across a post-lock reserved-policy update without a DB-wait claim."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.services.settings as settings_module
import snapper.data.financial_account_owner_policy_writer as writer_module
from snapper.application.services.settings import SettingsService
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.data.financial_account_owner_policy_store import get_bound_financial_owner_evidence
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.models import Setting
from snapper.data.repository import SQLAlchemyRepository
from tests.data.test_financial_account_owner_store import _authority
from tests.data.test_financial_account_owner_store import _policy
from tests.data.test_financial_account_owner_store import scenario as _scenario_fixture
from tests.helpers.native_account_authority import NativeAuthorityScenario

scenario = _scenario_fixture


def _fresh_settings_service(monkeypatch: pytest.MonkeyPatch, url: str) -> SettingsService:
    """Own an isolated generic service instance without opening its unused network publisher."""
    monkeypatch.setattr(SettingsService, "_instance", None)
    monkeypatch.setattr(SettingsService, "_init_params", None)
    monkeypatch.setattr(SettingsService, "_initialized", False)
    return SettingsService(url, "inproc://unused-explicit-owner-policy-clock")


async def _require_original_decision_replay(
    scenario: NativeAuthorityScenario,
    evidence: FinancialAccountOwnerEvidence,
    revocation_at: datetime,
) -> None:
    """Rebind the original exact revision after the actual generic writer commits."""
    async with scenario.repository.session() as session:
        old = await session.scalar(
            select(Setting).where(
                Setting.public_id == evidence.policy_reference.public_id,
                Setting.timestamp == evidence.policy_reference.timestamp,
            )
        )
        assert old is not None
        assert old.known_to > evidence.decided_at
        assert await get_bound_financial_owner_evidence(session, evidence) == evidence
        current = await session.scalar(
            select(Setting)
            .where(Setting.key == FINANCIAL_OWNER_SETTING_KEY)
            .order_by(Setting.timestamp.desc())
        )
        assert current is not None
        assert current.timestamp == revocation_at
        assert current.category == "financial_owner"
        assert current.is_encrypted is False


async def test_delayed_reserved_policy_revocation_preserves_an_intervening_owner_decision(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve original decision knowledge after an actual delayed administrative revocation.

    Given: A reserved admin revocation is scheduled before its actual write-reservation boundary.
    When: Scheduling resumes after a genuine owner decision commits and the clock has advanced.
    Then: The committed revision uses the later clock and preserves the original interval and replay.
    """
    reference, native = await _authority(scenario)
    await _policy(scenario, native)
    t0 = scenario.clock.current + timedelta(seconds=1)
    t1 = t0 + timedelta(seconds=1)
    sampled = asyncio.Event()
    permit_lock_query = asyncio.Event()
    original_begin = scenario.repository._begin_execution_insert_transaction
    delayed_first_begin = False
    url = str(scenario.repository.engine.url)

    class _Clock:
        """Keep the actual generic writer's original timestamp observable across its real waits."""

        current = t0
        samples: list[datetime] = []

        @staticmethod
        def now(zone: object) -> datetime:
            """Observe the real dedicated producer's clock after its reserved row lock."""
            assert zone is UTC
            _Clock.samples.append(_Clock.current)
            return _Clock.current

    class _ForbiddenGenericClock:
        """Refuse a reserved update that samples the generic service clock before dispatch."""

        @staticmethod
        def now(zone: object) -> datetime:
            """Make a pre-dispatch timestamp regression visible before any policy commit."""
            raise AssertionError("The reserved policy path must use its real post-lock clock")

    def _owned_repository(actual_url: str) -> SQLAlchemyRepository:
        """Keep the real generic service on this actual isolated SQLite database."""
        assert actual_url == url
        return scenario.repository

    async def _delayed_begin(session: AsyncSession) -> None:
        """Delay only the first caller before its real reservation so a valid reader can proceed."""
        nonlocal delayed_first_begin
        if not delayed_first_begin:
            delayed_first_begin = True
            sampled.set()
            await permit_lock_query.wait()
        await original_begin(session)

    monkeypatch.setattr(settings_module, "datetime", _ForbiddenGenericClock)
    monkeypatch.setattr(settings_module, "get_repository", _owned_repository)
    monkeypatch.setattr(writer_module, "datetime", _Clock)
    monkeypatch.setattr(scenario.repository, "_begin_execution_insert_transaction", _delayed_begin)
    service = _fresh_settings_service(monkeypatch, url)
    admin = asyncio.create_task(
        service.update_setting(
            key=FINANCIAL_OWNER_SETTING_KEY,
            value='{"schema_version":1,"assignments":[]}',
            category="financial_owner",
            updated_by="isolated-admin-clock-witness",
        )
    )
    try:
        await sampled.wait()
        assert _Clock.samples == []
        scenario.clock.current = t1
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, native.scope.wallet_public_id
            )
            evidence = await load_current_financial_owner_evidence(session, reference, t1)
            _Clock.current = t1 + timedelta(seconds=1)
            await session.commit()
        permit_lock_query.set()
        await admin
        await _require_original_decision_replay(scenario, evidence, t1 + timedelta(seconds=1))
        assert _Clock.samples == [t1 + timedelta(seconds=1)]
    finally:
        permit_lock_query.set()
        if not admin.done():
            admin.cancel()
        await asyncio.gather(admin, return_exceptions=True)
        await service.shutdown()
