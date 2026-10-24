"""Exercise actual reserved policy versions and public mutation boundaries without activating accounts."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.services.settings as settings_module
import snapper.config.settings_routes as routes_module
import snapper.data.financial_account_owner_policy_writer as writer_module
from snapper.api.schemas.settings import RemoveSettingBody
from snapper.api.schemas.settings import RemoveSettingRequest
from snapper.application.services.settings import SettingsService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import canonical_financial_account_owner_policy
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.data.financial_account_owner_policy_store import get_bound_financial_owner_evidence
from snapper.data.financial_account_owner_policy_store import load_current_financial_owner_policy
from snapper.data.financial_account_owner_policy_writer import (
    FINANCIAL_OWNER_POLICY_WRITER_NAMESPACE,
)
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import _lock_reserved_policy
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Setting
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.data.test_financial_account_owner_store import _alias
from tests.data.test_financial_account_owner_store import _authority
from tests.data.test_financial_account_owner_store import _policy
from tests.data.test_financial_account_owner_store import scenario as _scenario_fixture
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_uuid as _id

scenario = _scenario_fixture


def _clock(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch, seconds: int = 1
) -> datetime:
    """Make only the dedicated producer clock deterministic, keeping actual source clocks intact."""
    instant = scenario.clock.current + timedelta(seconds=seconds)

    class _Clock:
        """Expose the exact instant sampled by the real post-lock writer."""

        @staticmethod
        def now(zone: object) -> datetime:
            """Return the declared producer instant with its actual UTC contract."""
            assert zone is UTC
            return instant

    monkeypatch.setattr(writer_module, "datetime", _Clock)
    return instant


def _change(policy: FinancialAccountOwnerPolicy) -> FinancialOwnerPolicyChange:
    """Declare an explicit isolated administrative policy change without a caller clock."""
    return FinancialOwnerPolicyChange(policy=policy, updated_by="isolated-policy-administrator")


def _service(scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch) -> SettingsService:
    """Bind the actual generic setter to the isolated repository without opening ZMQ sockets."""
    monkeypatch.setattr(SettingsService, "_instance", None)
    monkeypatch.setattr(SettingsService, "_init_params", None)
    monkeypatch.setattr(SettingsService, "_initialized", False)
    url = str(scenario.repository.engine.url)

    def _repository(actual_url: str) -> SQLAlchemyRepository:
        """Return the actual owned repository for the service's declared database."""
        assert actual_url == url
        return scenario.repository

    monkeypatch.setattr(settings_module, "get_repository", _repository)
    return SettingsService(url, "inproc://unused-policy-writer-fixture")


async def _rows(scenario: NativeAuthorityScenario) -> tuple[Setting, ...]:
    """Read every real policy revision in knowledge order after the owned transaction completes."""
    async with scenario.repository.session() as session:
        return tuple(
            (
                await session.scalars(
                    select(Setting)
                    .where(Setting.key == FINANCIAL_OWNER_SETTING_KEY)
                    .order_by(Setting.timestamp)
                )
            ).all()
        )


async def test_create_policy_returns_actual_retained_revision(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Create explicit configuration from actual native roots without creating financial facts.

    Given: A real signed key/account read proves the initial owner root and no policy exists.
    When: The dedicated administrator producer commits one explicit assignment.
    Then: Its exact reference rebinds the actual plaintext row and post-lock revision clock.
    """
    _reference, native = await _authority(scenario)
    policy = FinancialAccountOwnerPolicy(
        assignments=(
            FinancialAccountOwnerAssignment(
                owner_key=financial_account_owner_key_from_authority(native),
                owner_wallet_public_id=native.scope.wallet_public_id,
            ),
        )
    )
    clock = _clock(scenario, monkeypatch)
    tracker = SequenceTracker()
    result = await write_financial_owner_policy(scenario.repository, _change(policy), tracker)

    assert result.changed is True
    assert result.reference.timestamp == clock
    assert result.reference.session_id == tracker.session_id
    async with scenario.repository.session() as session:
        bound = await load_current_financial_owner_policy(session, clock)
        assert bound.reference == result.reference
        assert bound.policy == policy
    actual = await _rows(scenario)
    assert len(actual) == 1
    assert actual[0].category == "financial_owner"
    assert actual[0].is_encrypted is False
    assert actual[0].updated_by == "isolated-policy-administrator"


async def test_identical_policy_returns_original_revision_without_new_version(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain the original row when the explicit policy is economically and canonically unchanged.

    Given: The actual policy already assigns the same wallet under a retained configuration version.
    When: An administrator requests that same strict policy at a later producer instant.
    Then: No close or insert occurs and the returned reference is the actual original revision.
    """
    _reference, native = await _authority(scenario)
    policy = await _policy(scenario, native)
    clock = _clock(scenario, monkeypatch)
    before = await _rows(scenario)
    result = await write_financial_owner_policy(
        scenario.repository, _change(policy), SequenceTracker()
    )

    assert result.changed is False
    assert result.reference.timestamp == before[0].timestamp < clock
    actual = await _rows(scenario)
    assert len(actual) == 1
    assert actual[0].known_to == KNOWN_TO_MAX
    assert actual[0].sequence_id == before[0].sequence_id


async def test_changed_policy_requires_a_strictly_later_locked_clock(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse a temporal overwrite when the actual post-lock clock has not advanced.

    Given: A retained original policy starts at the producer's current instant.
    When: A distinct empty policy requests a close at exactly that same instant.
    Then: The writer refuses and the original active interval remains unchanged.
    """
    _reference, native = await _authority(scenario)
    await _policy(scenario, native)
    _clock(scenario, monkeypatch, 0)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="clock_not_advanced"):
        await write_financial_owner_policy(
            scenario.repository,
            _change(FinancialAccountOwnerPolicy(assignments=())),
            SequenceTracker(),
        )
    actual = await _rows(scenario)
    assert len(actual) == 1
    assert actual[0].known_to == KNOWN_TO_MAX


@pytest.mark.parametrize("changes", [{"category": "system"}, {"is_encrypted": True}])
async def test_damaged_current_policy_metadata_cannot_be_replaced_as_if_it_were_proven(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch, changes: dict[str, object]
) -> None:
    """Keep existing metadata validation before admitting a reserved policy revision.

    Given: An actual retained policy row has invalid category or encryption metadata.
    When: The reserved writer attempts an explicit revocation.
    Then: The original damaged row remains retained and no replacement is silently certified.
    """
    _reference, native = await _authority(scenario)
    await _policy(scenario, native, changes=changes)
    _clock(scenario, monkeypatch)
    with pytest.raises(FinancialAccountOwnerUnavailableError):
        await write_financial_owner_policy(
            scenario.repository,
            _change(FinancialAccountOwnerPolicy(assignments=())),
            SequenceTracker(),
        )
    assert len(await _rows(scenario)) == 1


async def test_ambiguous_current_rows_are_refused_after_deliberate_unique_index_damage(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an imported duplicate rather than picking an arbitrary active owner policy.

    Given: Privileged fixture SQL removes the normal key uniqueness guard and imports a duplicate.
    When: The dedicated writer locks current policy rows.
    Then: It reports ambiguity and leaves both retained source versions untouched.
    """
    _reference, native = await _authority(scenario)
    policy = await _policy(scenario, native)
    async with scenario.repository.session() as session:
        await session.execute(text("DROP INDEX uq_settings_key"))
        session.add(
            Setting(
                public_id=_id(601),
                session_id=_id(9),
                sequence_id=601,
                timestamp=scenario.clock.current,
                key=FINANCIAL_OWNER_SETTING_KEY,
                value=canonical_financial_account_owner_policy(policy),
                category="financial_owner",
                is_encrypted=False,
                updated_by="deliberate-corrupt-import-fixture",
            )
        )
        await session.commit()
    _clock(scenario, monkeypatch)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="current_setting_ambiguous"):
        await write_financial_owner_policy(
            scenario.repository,
            _change(FinancialAccountOwnerPolicy(assignments=())),
            SequenceTracker(),
        )
    assert len(await _rows(scenario)) == 2


async def test_new_assignment_requires_actual_uid_roots_instead_of_a_pure_configured_uid(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse an unobserved UID even if its pure policy assignment is otherwise well formed.

    Given: No actual signed account read or initial identity root exists for the configured UID.
    When: The reserved writer requests its first assignment.
    Then: Configuration creation refuses instead of treating a caller-supplied UID as source proof.
    """
    from_authority = FinancialAccountOwnerAssignment.model_validate(
        {
            "owner_key": {"environment": "production", "account_uid": _id(80)},
            "owner_wallet_public_id": scenario.selection.wallet_public_id,
        }
    )
    policy = FinancialAccountOwnerPolicy(assignments=(from_authority,))
    _clock(scenario, monkeypatch)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="native_authority_missing"):
        await write_financial_owner_policy(scenario.repository, _change(policy), SequenceTracker())
    assert await _rows(scenario) == ()


async def test_uid_linked_legacy_book_blocks_reassignment_through_the_actual_writer(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the explicit unsupported-history barrier across a real administrative write.

    Given: An actual paired legacy position and shared source claim belong to a UID-linked alias.
    When: The dedicated producer attempts to assign that financial book to the current wallet.
    Then: Existing unsupported history refuses and no owner setting revision is created.
    """
    _reference, native = await _authority(scenario)
    legacy = await seed_contract_position_scenario(scenario.repository, monkeypatch)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await stage_contract_position(session, legacy)
        await session.commit()
    alias = await _alias(scenario, 3)
    await _authority(alias, 501)
    policy = FinancialAccountOwnerPolicy(
        assignments=(
            FinancialAccountOwnerAssignment(
                owner_key=financial_account_owner_key_from_authority(native),
                owner_wallet_public_id=native.scope.wallet_public_id,
            ),
        )
    )
    _clock(scenario, monkeypatch)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="native_history_or_migration"):
        await write_financial_owner_policy(scenario.repository, _change(policy), SequenceTracker())
    assert await _rows(scenario) == ()


async def test_failed_real_insert_rolls_back_the_policy_close_and_preserves_service_cache(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rollback a genuine close-plus-insert failure before publishing configuration state.

    Given: A valid policy decision exists and a real test trigger refuses its next Setting insert.
    When: The public reserved-key setter attempts an explicit empty-policy revocation.
    Then: The original interval and replay survive and neither cache nor broadcast changes.
    """
    reference, native = await _authority(scenario)
    policy = await _policy(scenario, native)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, native.scope.wallet_public_id
        )
        evidence = await load_current_financial_owner_evidence(
            session, reference, scenario.clock.current
        )
        await session.commit()
    async with scenario.repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER test_refuse_policy_insert BEFORE INSERT ON settings "
                "WHEN NEW.key = 'financial_account_owner_policy_v1' "
                "BEGIN SELECT RAISE(ABORT, 'isolated policy insert refused'); END"
            )
        )
        await session.commit()
    service = _service(scenario, monkeypatch)
    service._cache[FINANCIAL_OWNER_SETTING_KEY] = policy.model_dump(mode="json")
    before_cache = dict(service._cache)
    broadcast = AsyncMock()
    monkeypatch.setattr(service, "_broadcast_change", broadcast)
    _clock(scenario, monkeypatch)
    try:
        with pytest.raises(IntegrityError, match="isolated policy insert refused"):
            await service.update_setting(
                FINANCIAL_OWNER_SETTING_KEY,
                {"schema_version": 1, "assignments": []},
                updated_by="isolated-administrator",
            )
        assert service._cache == before_cache
        broadcast.assert_not_awaited()
        actual = await _rows(scenario)
        assert len(actual) == 1
        assert actual[0].known_to == KNOWN_TO_MAX
        async with scenario.repository.session() as session:
            assert await get_bound_financial_owner_evidence(session, evidence) == evidence
    finally:
        await service.shutdown()


async def test_generic_setter_routes_reserved_metadata_and_noop_publication_to_the_real_writer(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route the public setter before generic encryption and use the committed revision clock.

    Given: The public setting caller supplies generic metadata and the normal default encryption flag.
    When: It writes then repeats one explicit empty policy through SettingsService.
    Then: Actual reserved plaintext metadata is retained and only the changed revision is broadcast.
    """
    service = _service(scenario, monkeypatch)
    broadcast = AsyncMock()
    monkeypatch.setattr(service, "_broadcast_change", broadcast)
    clock = _clock(scenario, monkeypatch)
    try:
        for _attempt in range(2):
            await service.update_setting(
                FINANCIAL_OWNER_SETTING_KEY,
                {"schema_version": 1, "assignments": []},
                category="system",
                description="A caller cannot replace the reserved metadata",
                updated_by="isolated-administrator",
            )
        actual = await _rows(scenario)
        assert len(actual) == 1
        assert actual[0].timestamp == clock
        assert actual[0].is_encrypted is False
        assert actual[0].category == "financial_owner"
        assert service._cache[FINANCIAL_OWNER_SETTING_KEY] == {
            "schema_version": 1,
            "assignments": [],
        }
        broadcast.assert_awaited_once_with(
            FINANCIAL_OWNER_SETTING_KEY,
            canonical_financial_account_owner_policy(FinancialAccountOwnerPolicy(assignments=())),
            "financial_owner",
            "isolated-administrator",
            now=clock,
        )
    finally:
        await service.shutdown()


@pytest.mark.parametrize(
    "value,updater,expected",
    [
        ('{"schema_version":1,"assignments":[]}', None, FinancialAccountOwnerUnavailableError),
        ('{"schema_version":1,"assignments":[]}', " ", ValidationError),
        ('{"schema_version":1,"schema_version":1,"assignments":[]}', "admin", ValueError),
        ('{"schema_version":true,"assignments":[]}', "admin", ValidationError),
    ],
)
async def test_invalid_reserved_input_never_enters_generic_mutation_or_changes_cache(
    scenario: NativeAuthorityScenario,
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    updater: str | None,
    expected: type[Exception],
) -> None:
    """Validate reserved input before it can follow the generic setting path.

    Given: A caller omits the updater or supplies blank, ambiguous or non-strict policy material.
    When: The real public setter dispatches the reserved key.
    Then: Validation fails with no retained policy, cache update or broadcast.
    """
    service = _service(scenario, monkeypatch)
    generic = AsyncMock()
    broadcast = AsyncMock()
    monkeypatch.setattr(service, "_close_and_insert_setting", generic)
    monkeypatch.setattr(service, "_broadcast_change", broadcast)
    try:
        with pytest.raises(expected):
            await service.update_setting(FINANCIAL_OWNER_SETTING_KEY, value, updated_by=updater)
        generic.assert_not_awaited()
        broadcast.assert_not_awaited()
        assert service._cache == {}
        assert await _rows(scenario) == ()
    finally:
        await service.shutdown()


async def test_reserved_service_refuses_an_unproven_repository_backend(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require the actual backend transaction implementation rather than a nominal generic facade.

    Given: A generic repository facade has no established native write-reservation contract.
    When: The public setting service dispatches an otherwise valid explicit empty owner policy.
    Then: It refuses before mutation, cache update or publication.
    """
    service = _service(scenario, monkeypatch)
    facade = MagicMock(spec=Repository)

    def _unproven_repository(url: str) -> Repository:
        """Return the deliberately unsupported boundary without opening a real transaction."""
        return cast(Repository, facade)

    monkeypatch.setattr(settings_module, "get_repository", _unproven_repository)
    try:
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="sqlalchemy_writer_required"
        ):
            await service.update_setting(
                FINANCIAL_OWNER_SETTING_KEY,
                {"schema_version": 1, "assignments": []},
                updated_by="isolated-administrator",
            )
        facade.session.assert_not_called()
        assert service._cache == {}
    finally:
        await service.shutdown()


async def test_private_generic_helper_cannot_bypass_the_reserved_policy_producer(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse direct generic-helper writes to the reserved owner policy.

    Given: Internal generic setting code has a valid repository and a reserved policy key.
    When: It calls the old private helper directly with an arbitrary pre-sampled clock.
    Then: The helper refuses before creating any policy version.
    """
    service = _service(scenario, monkeypatch)
    try:
        with pytest.raises(FinancialAccountOwnerUnavailableError, match="reserved_writer_required"):
            await service._close_and_insert_setting(
                key=FINANCIAL_OWNER_SETTING_KEY,
                value='{"schema_version":1,"assignments":[]}',
                category="financial_owner",
                description=None,
                is_encrypted=False,
                updated_by="isolated-administrator",
                now=scenario.clock.current,
            )
        assert await _rows(scenario) == ()
    finally:
        await service.shutdown()


async def test_reserved_remove_route_refuses_before_opening_a_database_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require a versioned empty policy for revocation rather than a direct temporal deletion.

    Given: An authenticated configuration administrator requests removal of the reserved key.
    When: The actual route receives its strict removal envelope.
    Then: It reports the explicit revocation contract without touching settings or repository state.
    """
    settings = MagicMock(
        side_effect=AssertionError("Reserved removal must not access the database")
    )
    monkeypatch.setattr(routes_module, "get_settings", settings)
    body = RemoveSettingRequest(
        session_id=_id(900),
        sequence_id=1,
        public_id=_id(901),
        timestamp=datetime(2026, 10, 23, 15, tzinfo=UTC),
        payload=RemoveSettingBody(),
    )
    with pytest.raises(HTTPException) as error:
        await routes_module.remove_setting(
            cast(Request, MagicMock(spec=Request)),
            FINANCIAL_OWNER_SETTING_KEY,
            AuthPrincipal(username="isolated-administrator", role=UserRole.ADMIN),
            None,
            body,
        )
    assert error.value.status_code == 409
    assert "empty financial owner policy" in error.value.detail
    settings.assert_not_called()


async def test_postgresql_boundary_uses_reserved_key_advisory_before_real_row_lock() -> None:
    """Verify PG SQL operands without claiming an actual server wait or concurrency witness.

    Given: A typed SQLAlchemy boundary reports PostgreSQL and no current setting row.
    When: The actual reserved-row helper constructs its two lock statements.
    Then: The setting-wide namespace precedes a FOR UPDATE query on the reserved natural key.
    """
    boundary = MagicMock(spec=AsyncSession)
    boundary.get_bind.return_value.dialect.name = "postgresql"
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    boundary.execute = AsyncMock(return_value=result)
    assert await _lock_reserved_policy(cast(AsyncSession, boundary)) is None
    first, second = boundary.execute.await_args_list
    assert "pg_advisory_xact_lock" in str(first.args[0])
    assert first.args[1] == {
        "namespace": FINANCIAL_OWNER_POLICY_WRITER_NAMESPACE,
        "setting_key": FINANCIAL_OWNER_SETTING_KEY,
    }
    assert "FOR UPDATE" in str(second.args[0])
    assert FINANCIAL_OWNER_POLICY_WRITER_NAMESPACE not in {
        "snapper_financial_owner",
        "snapper_startup_integrity",
        "execution_fence",
    }
