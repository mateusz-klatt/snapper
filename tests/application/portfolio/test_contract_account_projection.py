"""Exercise the actual financial callback and restart hydration against real immutable journals."""

import asyncio
from datetime import datetime
from datetime import tzinfo
from decimal import Decimal
from typing import Literal

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.portfolio.contract_account_projection as projection_module
from snapper.application.portfolio.contract_account_projection import ContractAccountProjection
from snapper.application.trade.financial_publication import publish_pending_financial_notifications
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.data.contract_account_projection_store import ContractAccountProjectionError
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import FinancialPublicationDelivery
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_cash
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.unit_admission import admission_id

projection_scenario = shared_projection_scenario


async def test_real_pending_publication_callback_hydrates_and_restart_ignores_delivery_marker(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Use the actual v1 publisher seam and recover from durable facts after local delivery.

    Given: Actual source, execution, unit, position, claim and notification committed atomically.
    When: The real pending publisher calls the projection and a fresh observer restarts afterward.
    Then: Both hydrate the position although only the first call sends the notification.
    """
    publication = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = projection_scenario.fill.admission.authority.scope
    observer = ContractAccountProjection(repository, scope)
    assert observer.snapshot is None
    assert (
        await publish_pending_financial_notifications(
            repository, scope, observer, SequenceTracker()
        )
        == 1
    )
    snapshot = observer.snapshot
    assert snapshot is not None
    assert (
        snapshot.positions[0].latest_event_public_id == publication.payload.position_event_public_id
    )
    assert snapshot.readiness == "held"
    restarted = ContractAccountProjection(repository, scope)
    assert (
        await publish_pending_financial_notifications(
            repository, scope, restarted, SequenceTracker()
        )
        == 0
    )
    assert restarted.snapshot is None
    recovered = await restarted.refresh()
    assert recovered.positions == snapshot.positions
    async with repository.session() as session:
        assert (
            await session.scalar(select(func.count()).select_from(FinancialPublicationDelivery))
            == 1
        )


async def test_old_notification_redelivery_refreshes_new_actual_cash_without_duplicate_facts(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Refresh current durable facts rather than apply a stale notification's financial delta.

    Given: A position notification was consumed before a distinct real cash event arrived.
    When: The same old notification is redelivered after the cash event commits.
    Then: The cache shows the actual latest balance and retains only two native financial facts.
    """
    publication = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    observer = ContractAccountProjection(repository, publication.payload.scope)
    await observer(publication.topic, publication.public_id, publication.payload)
    before = observer.snapshot
    assert before is not None
    assert before.native_cash == ()
    await record_projection_cash(projection_scenario)
    await observer(publication.topic, publication.public_id, publication.payload)
    after = observer.snapshot
    assert after is not None
    assert after.native_cash[0].balance == Decimal("99.75")
    assert after.positions == before.positions
    async with repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 2


@pytest.mark.parametrize("change", ["topic", "payload", "missing_id", "scope"])
async def test_unbound_notification_preserves_the_previously_hydrated_cache(
    projection_scenario: ContractPositionScenario, change: str
) -> None:
    """Require actual publication identity, exact payload and configured scope before cache mutation.

    Given: A real observer already holds one source-bound partial snapshot.
    When: A callback carries a changed topic, payload, absent UUID or different declared margin.
    Then: Durable binding refuses and the prior cache object remains untouched.
    """
    publication = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = publication.payload.scope
    if change == "scope":
        scope = ExecutionAccountScope.model_validate(
            {**scope.model_dump(), "margin_account": "other-margin-profile"}
        )
    observer = ContractAccountProjection(repository, scope)
    previous = await observer.refresh()
    topic = publication.topic if change != "topic" else publication.topic + ".foreign"
    public_id = publication.public_id if change != "missing_id" else admission_id(999)
    payload = publication.payload
    if change == "payload":
        payload = type(payload).model_validate(
            {**payload.model_dump(), "source_execution_id": "contradictory-provider-fill"}
        )
    with pytest.raises((ContractAccountProjectionError, FinancialPublicationError)):
        await observer(topic, public_id, payload)
    assert observer.snapshot is previous


async def test_failed_read_transaction_commit_never_replaces_the_cache(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep cache replacement after successful transaction completion.

    Given: The observer has a genuine position snapshot and a new actual cash event exists.
    When: The real refresh session raises before its read transaction can commit.
    Then: The exception propagates and the old position-only cache remains intact.
    """
    publication = await record_projection_position(projection_scenario)
    observer = ContractAccountProjection(
        projection_scenario.fill.admission.repository, publication.payload.scope
    )
    previous = await observer.refresh()
    await record_projection_cash(projection_scenario)

    async def fail_commit(self: AsyncSession) -> None:
        """Fail the actual session boundary before any new cache can become visible."""
        raise RuntimeError("projection_read_commit_failed")

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    with pytest.raises(RuntimeError, match="projection_read_commit_failed"):
        await observer.refresh()
    assert observer.snapshot is previous
    assert previous.native_cash == ()


async def test_publication_newer_than_local_read_clock_cannot_change_the_cache(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require the actual notification to exist by the sampled local read knowledge clock.

    Given: An actual committed publication is newer than an explicitly earlier test clock.
    When: The real callback verifies it under the wallet fence.
    Then: It refuses instead of exposing future retained knowledge.
    """
    publication = await record_projection_position(projection_scenario)

    class EarlierClock(datetime):
        """Supply an aware local read clock before actual publication retention."""

        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            """Return the actual source-fill knowledge time before publication retention."""
            return _KNOWN

    monkeypatch.setattr(projection_module, "datetime", EarlierClock)
    observer = ContractAccountProjection(
        projection_scenario.fill.admission.repository, publication.payload.scope
    )
    with pytest.raises(ContractAccountProjectionError, match="publication_knowledge"):
        await observer(publication.topic, publication.public_id, publication.payload)
    assert observer.snapshot is None


async def test_concurrent_refreshes_serialize_verification_hydration_and_cache_replacement(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve refresh serialization after the actual database fence has been released.

    Given: The first read committed but pauses before replacing its position-only cache.
    When: A real cash fact commits and another refresh attempts to acquire the observer lock.
    Then: The second waits, reads the later cash fact and becomes the final cache without reversion.
    """
    publication = await record_projection_position(projection_scenario)
    observer = ContractAccountProjection(
        projection_scenario.fill.admission.repository, publication.payload.scope
    )
    committed = asyncio.Event()
    release = asyncio.Event()
    second_attempted = asyncio.Event()
    second_acquired = asyncio.Event()
    original_commit = AsyncSession.commit
    commits = 0
    acquisitions = 0

    class ObservedLock(asyncio.Lock):
        """Expose actual lock attempts independently of the SQLite transaction fence."""

        async def acquire(self) -> Literal[True]:
            """Report the second attempt before waiting on the real observer mutex.

            Returns:
                The actual successful lock acquisition result.
            """
            nonlocal acquisitions
            acquisitions += 1
            number = acquisitions
            if number == 2:
                second_attempted.set()
            acquired = await super().acquire()
            if number == 2:
                second_acquired.set()
            return acquired

    async def pause_after_real_commit(session: AsyncSession) -> None:
        """Release the actual DB fence before delaying the first cache replacement."""
        nonlocal commits
        await original_commit(session)
        commits += 1
        if commits == 1:
            committed.set()
            await release.wait()

    observer._lock = ObservedLock()
    monkeypatch.setattr(AsyncSession, "commit", pause_after_real_commit)
    tasks = [asyncio.create_task(observer.refresh())]
    try:
        await committed.wait()
        await record_projection_cash(projection_scenario)
        tasks.append(asyncio.create_task(observer.refresh()))
        await second_attempted.wait()
        assert not second_acquired.is_set()
        assert observer.snapshot is None
    finally:
        release.set()
        snapshots = await asyncio.gather(*tasks)
    assert len(snapshots) == 2
    assert snapshots[0].native_cash == ()
    assert snapshots[1].native_cash[0].balance == Decimal("99.75")
    assert observer.snapshot is snapshots[1]
