"""Real legacy/native account routing with explicit collision and historical boundaries."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update

import snapper.data.execution_account_authority_store as dispatch
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import canonical_route_authority_payload
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.data.execution_admission_triggers import install_execution_admission_table_triggers
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_reference
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.unit_admission import seed_admission_scenario


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativeAuthorityScenario]:
    """Own the real source producer and both immutable authority schemas in isolated SQLite."""
    async with native_authority_scenario(tmp_path, monkeypatch) as value:
        yield value


async def _native(scenario: NativeAuthorityScenario, number: int = 500) -> ExecutionRouteReference:
    """Retain genuine source-bound native authority before handing its UUID to common routing."""
    outcome = await capture_native_account_read(scenario)
    payload = await build_native_authority_payload(scenario, outcome)
    result = await insert_native_authority(scenario, payload, number)
    return native_authority_reference(result, payload)


async def _route(scenario: NativeAuthorityScenario, native: bool) -> ExecutionRouteReference:
    """Create the selected real source family without mixing its source representation."""
    if native:
        return await _native(scenario)
    legacy = await seed_admission_scenario(scenario.repository, scenario.clock.current)
    return legacy.route


async def _current(
    scenario: NativeAuthorityScenario,
    reference: ExecutionRouteReference,
    as_of: datetime,
    policy: NativeAuthorityFreshnessPolicy | None,
) -> dispatch.BoundAccountAuthority:
    """Hold the actual intended wallet fence for the entire common current-source validation."""
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, reference.scope.wallet_public_id
        )
        return await dispatch.require_current_account_authority(session, reference, as_of, policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("native", (False, True))
async def test_common_dispatch_preserves_the_actual_family_and_knowledge_clock(
    scenario: NativeAuthorityScenario, native: bool
) -> None:
    """Route either real family without inventing shared payload material or knowledge timestamps.

    Given: A real legacy observation or a signed native accounts source with immutable authority,
    When: Historical and current common readers resolve the same untyped UUID,
    Then: They agree on family, digest, complete payload and the actual stored recording clock.
    """
    route = await _route(scenario, native)
    async with scenario.repository.session() as session:
        historical = await dispatch.get_bound_account_authority(session, route.authority_public_id)
    current = await _current(scenario, route, scenario.clock.current, scenario.policy)
    assert current == historical
    assert current.family == ("native_accounts_v1" if native else "legacy_route_v1")
    assert current.content_digest == route.authority_content_digest
    assert current.recorded_at == scenario.clock.current
    reference = AccountAuthorityReference(
        family=current.family,
        public_id=current.public_id,
        content_digest=current.content_digest,
        recorded_at=current.recorded_at,
    )
    assert AccountAuthorityReference.model_validate_json(reference.model_dump_json()) == reference
    async with scenario.repository.session() as session:
        assert (
            await dispatch.get_bound_account_authority_reference(session, reference) == historical
        )
    if isinstance(current.payload, ExecutionRouteAuthorityPayload):
        legacy = current.payload
        assert route_authority_content_digest(legacy) == route.authority_content_digest
        async with scenario.repository.session() as session:
            row = await session.scalar(
                select(ExecutionRouteAuthority).where(
                    ExecutionRouteAuthority.public_id == route.authority_public_id
                )
            )
            assert row is not None
            assert row.canonical_payload == canonical_route_authority_payload(legacy)


@pytest.mark.asyncio
async def test_unknown_common_identity_is_not_invented(
    scenario: NativeAuthorityScenario,
) -> None:
    """Refuse an absent identifier instead of selecting any currently valid account authority.

    Given: An empty authority inventory and a well-shaped unrelated public UUID,
    When: The common historical reader resolves that UUID,
    Then: Missing or ambiguous identity is reported without a family fallback.
    """
    async with scenario.repository.session() as session:
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="missing_or_ambiguous"):
            await dispatch.get_bound_account_authority(session, native_authority_uuid(901))


@pytest.mark.asyncio
async def test_later_legacy_collision_refuses_untyped_current_but_preserves_native_history(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep explicit native history stable while refusing a later ambiguous routing handle.

    Given: A genuine native row followed by a genuine legacy row sharing its public UUID,
    When: Untyped history and current routing are compared with explicit native history,
    Then: Both untyped operations refuse while the independently bound native graph is unchanged.
    """
    route = await _native(scenario, 93)
    async with scenario.repository.session() as session:
        original = await get_bound_native_account_authority(session, route.authority_public_id)
    legacy = await seed_admission_scenario(scenario.repository, scenario.clock.current)
    assert legacy.route.authority_public_id == route.authority_public_id
    async with scenario.repository.session() as session:
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="missing_or_ambiguous"):
            await dispatch.get_bound_account_authority(session, route.authority_public_id)
        assert (
            await get_bound_native_account_authority(session, route.authority_public_id) == original
        )
    with pytest.raises(dispatch.AccountAuthorityBindingError, match="missing_or_ambiguous"):
        await _current(scenario, route, scenario.clock.current, scenario.policy)
    native_reference = AccountAuthorityReference(
        family="native_accounts_v1",
        public_id=original.public_id,
        content_digest=original.content_digest,
        recorded_at=original.recorded_at,
    )
    legacy_reference = AccountAuthorityReference(
        family="legacy_route_v1",
        public_id=legacy.route.authority_public_id,
        content_digest=legacy.route.authority_content_digest,
        recorded_at=legacy.now,
    )
    async with scenario.repository.session() as session:
        native_bound = await dispatch.get_bound_account_authority_reference(
            session, native_reference
        )
        legacy_bound = await dispatch.get_bound_account_authority_reference(
            session, legacy_reference
        )
    assert native_bound.payload == original.payload
    assert legacy_bound.payload == legacy.authority


@pytest.mark.asyncio
@pytest.mark.parametrize("native", (False, True))
async def test_current_common_read_cannot_use_a_row_before_it_was_known(
    scenario: NativeAuthorityScenario, native: bool
) -> None:
    """Respect actual row knowledge separately from original observation time for both families.

    Given: A real authority row retained at the controlled local recording clock,
    When: Current interpretation supplies an earlier clock,
    Then: The common boundary refuses without treating the source observation as row knowledge.
    """
    route = await _route(scenario, native)
    with pytest.raises(dispatch.AccountAuthorityBindingError, match="not_yet_known"):
        await _current(
            scenario, route, scenario.clock.current - timedelta(microseconds=1), scenario.policy
        )


@pytest.mark.asyncio
async def test_current_common_read_requires_an_aware_clock(
    scenario: NativeAuthorityScenario,
) -> None:
    """Refuse ambiguous local time before selecting a current source family.

    Given: A real native authority and its exact runtime reference,
    When: Current interpretation omits timezone information,
    Then: The common boundary rejects the clock before granting a current result.
    """
    route = await _native(scenario)
    with pytest.raises(dispatch.AccountAuthorityBindingError, match="clock_not_aware"):
        await _current(
            scenario, route, scenario.clock.current.replace(tzinfo=None), scenario.policy
        )


@pytest.mark.asyncio
async def test_native_current_dispatch_cannot_supply_its_own_default_freshness_policy(
    scenario: NativeAuthorityScenario,
) -> None:
    """Require explicit trusted native freshness configuration at current admission.

    Given: A genuine native row and exact route reference,
    When: A legacy caller reaches common current lookup without native policy,
    Then: Native admission refuses while its immutable historical graph remains readable.
    """
    route = await _native(scenario)
    with pytest.raises(dispatch.AccountAuthorityBindingError, match="freshness_policy_missing"):
        await _current(scenario, route, scenario.clock.current, None)
    async with scenario.repository.session() as session:
        assert await dispatch.get_bound_account_authority(session, route.authority_public_id)


@pytest.mark.asyncio
async def test_restored_legacy_row_with_changed_lifetime_is_not_immutable_authority(
    scenario: NativeAuthorityScenario,
) -> None:
    """Detect a restored lifetime contradiction even after authentic mutation guards return.

    Given: A privileged restore changes immutable legacy known_to and reinstalls genuine guards,
    When: The common historical boundary inspects the actual retained row,
    Then: It refuses the invalid immutable clock instead of accepting a retired SCD2 version.
    """
    route = await _route(scenario, False)
    async with scenario.repository.engine.begin() as connection:
        await connection.execute(text("PRAGMA ignore_check_constraints=ON"))
        await connection.execute(text("DROP TRIGGER execution_route_authorities_reject_update"))
        await connection.execute(
            update(ExecutionRouteAuthority)
            .where(ExecutionRouteAuthority.public_id == route.authority_public_id)
            .values(known_to=scenario.clock.current)
        )
        await connection.execute(text("PRAGMA ignore_check_constraints=OFF"))
        await connection.run_sync(
            lambda actual: install_execution_admission_table_triggers(
                actual, "execution_route_authorities"
            )
        )
    async with scenario.repository.session() as session:
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="invalid_immutable_clock"):
            await dispatch.get_bound_account_authority(session, route.authority_public_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("native", (False, True))
async def test_family_delegate_cannot_return_material_different_from_discovered_row(
    scenario: NativeAuthorityScenario, native: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a source-reader contract violation against actual discovered immutable metadata.

    Given: Genuine stored authority but a faulty delegate returning changed knowledge or material,
    When: Common historical routing compares the returned graph with its database discovery,
    Then: It refuses instead of combining metadata from one record with another payload.
    """
    route = await _route(scenario, native)
    async with scenario.repository.session() as session:
        if native:
            original = await get_bound_native_account_authority(session, route.authority_public_id)
            changed = replace(
                original, recorded_at=original.recorded_at + timedelta(microseconds=1)
            )
            monkeypatch.setattr(
                dispatch, "get_bound_native_account_authority", AsyncMock(return_value=changed)
            )
        else:
            original_legacy = await dispatch.get_bound_route_authority(
                session, route.authority_public_id
            )
            changed_legacy = ExecutionRouteAuthorityPayload.model_validate(
                original_legacy.model_dump()
                | {"valid_until": original_legacy.valid_until + timedelta(seconds=1)}
            )
            monkeypatch.setattr(
                dispatch, "get_bound_route_authority", AsyncMock(return_value=changed_legacy)
            )
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="discovery_changed"):
            await dispatch.get_bound_account_authority(session, route.authority_public_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("changed_clock", (False, True))
async def test_typed_historical_reference_rebinds_digest_and_actual_knowledge(
    scenario: NativeAuthorityScenario, native: bool, changed_clock: bool
) -> None:
    """Refuse forged reference metadata even when the family and public UUID name a genuine row.

    Given: A real legacy or native authority with its original immutable knowledge clock,
    When: A retained typed reference claims another digest or recording time,
    Then: Explicit historical lookup refuses instead of treating the supplied metadata as evidence.
    """
    route = await _route(scenario, native)
    reference = AccountAuthorityReference(
        family="native_accounts_v1" if native else "legacy_route_v1",
        public_id=route.authority_public_id,
        content_digest=route.authority_content_digest if changed_clock else "f" * 64,
        recorded_at=(
            scenario.clock.current + timedelta(seconds=1)
            if changed_clock
            else scenario.clock.current
        ),
    )
    async with scenario.repository.session() as session:
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="reference_mismatch"):
            await dispatch.get_bound_account_authority_reference(session, reference)


@pytest.mark.asyncio
async def test_typed_legacy_reference_does_not_search_another_family_for_a_missing_row(
    scenario: NativeAuthorityScenario,
) -> None:
    """Preserve the explicit family boundary when only another family has the supplied UUID.

    Given: One genuine native row but a typed legacy reference claiming its identity,
    When: The historical reference reader queries the declared family,
    Then: It refuses the absent legacy row rather than falling back to the native payload.
    """
    route = await _native(scenario)
    reference = AccountAuthorityReference(
        family="legacy_route_v1",
        public_id=route.authority_public_id,
        content_digest=route.authority_content_digest,
        recorded_at=scenario.clock.current,
    )
    async with scenario.repository.session() as session:
        with pytest.raises(dispatch.AccountAuthorityBindingError, match="reference_missing"):
            await dispatch.get_bound_account_authority_reference(session, reference)
