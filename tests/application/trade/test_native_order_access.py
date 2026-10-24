"""Real closed-HTTP native order prerequisites without any venue order call."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.native_order_access as access
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_identity import NativeKeyAccess
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import begin_native_account_read
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import bound_native_authority_sources
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_identity
from tests.helpers.native_account_authority import native_authority_reference
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.unit_admission import refresh_admission_account
from tests.helpers.unit_admission import seed_admission_scenario
from tests.helpers.unit_admission_payload import unit_admission_payload

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativeAuthorityScenario]:
    """Own actual retained HTTP identity/accounts evidence and controlled producer clocks."""
    async with native_authority_scenario(tmp_path, monkeypatch) as value:
        monkeypatch.setattr(access, "datetime", value.clock)
        yield value


async def _capture(
    scenario: NativeAuthorityScenario,
    permission: NativeKeyAccess = "FULL_ACCESS",
    number: int = 500,
) -> tuple[ExecutionRouteReference, BoundNativeAccountReadOutcome]:
    """Vary the actual closed check-v3 response and retain the genuine bound native source graph."""
    scenario.adapter.general_access = permission
    outcome = await capture_native_account_read(scenario)
    payload = await build_native_authority_payload(scenario, outcome)
    result = await insert_native_authority(scenario, payload, number)
    return native_authority_reference(result, payload), outcome


async def _require(
    scenario: NativeAuthorityScenario, route: ExecutionRouteReference
) -> access.NativeFuturesOrderAccessPrerequisite:
    """Acquire the actual wallet fence before evaluating the read-only prerequisite."""
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, route.scope.wallet_public_id
        )
        return await access.require_current_native_futures_order_access(
            session, route, scenario.policy
        )


async def test_read_only_still_retains_native_observation_but_refuses_new_order_prerequisite(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep read identity/account observation independent of the new order-access requirement.

    Given: Actual retained check-v3 READ_ONLY permission and a successful signed local accounts response.
    When: Accounts are rebound and the new order-only prerequisite is evaluated under the wallet fence.
    Then: Account observation remains valid while the order prerequisite refuses without a network call.
    """
    route, outcome = await _capture(scenario, "READ_ONLY")
    proof, accounts = await bound_native_authority_sources(scenario, outcome)
    assert proof.payload.observation.general_access == "READ_ONLY"
    assert accounts.source_recorded_at <= scenario.clock.current
    requests = len(scenario.adapter.calls)
    with pytest.raises(
        access.NativeFuturesOrderAccessUnavailableError, match="requires_full_access"
    ):
        await _require(scenario, route)
    assert len(scenario.adapter.calls) == requests
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, route.authority_public_id)
        ).payload.owner_proof.public_id == proof.public_id


async def test_full_access_returns_actual_source_prerequisite_without_order_or_transfer_permission(
    scenario: NativeAuthorityScenario,
) -> None:
    """Return bound evidence rather than a cached CREATE capability or a transfer-access requirement.

    Given: Actual check-v3 FULL_ACCESS general permission with the original NO_ACCESS transfer permission.
    When: The actual current authority and same-attempt proof are rebound inside the wallet fence.
    Then: Evidence retains the exact source and checked clock without another HTTP request or order call.
    """
    route, outcome = await _capture(scenario)
    proof, _ = await bound_native_authority_sources(scenario, outcome)
    requests = len(scenario.adapter.calls)
    evidence = await _require(scenario, route)
    assert evidence.authority.family == "native_accounts_v1"
    assert evidence.authority.public_id == route.authority_public_id
    assert evidence.key_proof == proof
    assert evidence.key_proof.payload.observation.general_access == "FULL_ACCESS"
    assert evidence.key_proof.payload.observation.transfer_access == "NO_ACCESS"
    assert evidence.checked_at == scenario.clock.current
    assert len(scenario.adapter.calls) == requests


async def test_later_read_only_source_invalidates_old_full_access_and_refuses_new_read_only_route(
    scenario: NativeAuthorityScenario,
) -> None:
    """Use latest actual observed key access instead of a last-known trading-capable key proof.

    Given: A genuine FULL_ACCESS source followed by a successful later READ_ONLY key/accounts read.
    When: Both the old and newly issued route are offered to the new order prerequisite.
    Then: Old current eligibility is gone, new READ_ONLY refuses and immutable old permission stays intact.
    """
    old_route, old_outcome = await _capture(scenario)
    await _require(scenario, old_route)
    old_proof, _ = await bound_native_authority_sources(scenario, old_outcome)
    scenario.clock.current += timedelta(seconds=1)
    new_route, _ = await _capture(scenario, "READ_ONLY", 501)
    requests = len(scenario.adapter.calls)
    with pytest.raises(ValueError, match="newest_attempt_changed_or_pending"):
        await _require(scenario, old_route)
    with pytest.raises(
        access.NativeFuturesOrderAccessUnavailableError, match="requires_full_access"
    ):
        await _require(scenario, new_route)
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_identity(session, old_proof.public_id) == old_proof
    assert len(scenario.adapter.calls) == requests


async def test_later_full_access_uses_current_proof_even_when_immutable_owner_root_was_read_only(
    scenario: NativeAuthorityScenario,
) -> None:
    """Read permission from the actual same-attempt proof rather than its historical account-owner root.

    Given: The immutable initial owner root observed READ_ONLY and a later same-account proof observes FULL_ACCESS.
    When: A new current native route is independently constructed and evaluated.
    Then: The prerequisite uses that current proof while the original READ_ONLY root remains unchanged.
    """
    _, first_outcome = await _capture(scenario, "READ_ONLY")
    root, _ = await bound_native_authority_sources(scenario, first_outcome)
    scenario.clock.current += timedelta(seconds=1)
    route, _ = await _capture(scenario, "FULL_ACCESS", 501)
    evidence = await _require(scenario, route)
    assert evidence.key_proof.public_id != root.public_id
    assert evidence.key_proof.payload.owner_root is not None
    assert evidence.key_proof.payload.owner_root.public_id == root.public_id
    assert evidence.key_proof.payload.observation.general_access == "FULL_ACCESS"
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_identity(session, root.public_id)
        ).payload.observation.general_access == "READ_ONLY"


async def test_later_failed_read_prevents_old_full_access_from_becoming_current_again(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep a failed latest account source from falling back to an older successful access proof.

    Given: A valid FULL_ACCESS route and a later genuine failed local accounts request.
    When: The old route is evaluated for new order access.
    Then: Current-source binding refuses before accepting its historical FULL_ACCESS field.
    """
    route, _ = await _capture(scenario)
    scenario.clock.current += timedelta(seconds=1)
    scenario.adapter.fail_accounts = True
    await capture_native_account_read(scenario)
    requests = len(scenario.adapter.calls)
    with pytest.raises(ValueError, match="newest_attempt_changed_or_pending"):
        await _require(scenario, route)
    assert len(scenario.adapter.calls) == requests


async def test_latest_pending_attempt_invalidates_old_full_access_before_any_new_http_response(
    scenario: NativeAuthorityScenario,
) -> None:
    """Respect durable attempt ordering before response arrival or permission refresh.

    Given: A genuine FULL_ACCESS route followed by a later committed native read attempt with no outcome.
    When: The previous route is evaluated under the actual wallet fence.
    Then: The prerequisite refuses the pending latest state without polling or choosing an old proof.
    """
    route, _ = await _capture(scenario)
    scenario.clock.current += timedelta(seconds=1)
    await begin_native_account_read(
        scenario.repository, scenario.selection, native_authority_identity(601)
    )
    requests = len(scenario.adapter.calls)
    with pytest.raises(ValueError, match="newest_attempt_changed_or_pending"):
        await _require(scenario, route)
    assert len(scenario.adapter.calls) == requests


async def test_full_access_permission_does_not_extend_an_expired_original_native_source(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep observed key permission separate from the trusted source-age window.

    Given: A retained FULL_ACCESS proof whose original native source later expires.
    When: The actual evaluation clock advances beyond the unchanged source window.
    Then: Current binding refuses instead of treating FULL_ACCESS as perpetual validity.
    """
    route, _ = await _capture(scenario)
    scenario.clock.current += timedelta(seconds=61)
    with pytest.raises(ValueError, match="native_authority_not_current"):
        await _require(scenario, route)


async def test_legacy_futures_route_is_not_a_native_key_permission_source(
    scenario: NativeAuthorityScenario,
) -> None:
    """Require the actual native source family rather than interpreting a generic balance snapshot as access.

    Given: An actual retained legacy Futures route from the existing repository snapshot path.
    When: The new order prerequisite receives that route.
    Then: It refuses the legacy family while the unchanged legacy current loader still works.
    """
    legacy = await seed_admission_scenario(
        scenario.repository, scenario.clock.current, source="kraken_futures"
    )
    with pytest.raises(
        access.NativeFuturesOrderAccessUnavailableError, match="requires_native_authority"
    ):
        await _require(scenario, legacy.route)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, legacy.route.scope.wallet_public_id
        )
        assert (
            await require_current_route_authority(session, legacy.route, scenario.clock.current)
            == legacy.authority
        )


async def test_current_cross_family_uuid_collision_refuses_even_with_a_genuine_full_access_proof(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep current untyped authority discovery unique before evaluating order-specific access.

    Given: A valid native FULL_ACCESS authority and an actual later legacy authority with its same UUID.
    When: New order access tries to discover that current route.
    Then: It refuses ambiguity rather than prioritizing the native proof.
    """
    route, _ = await _capture(scenario)
    legacy = await seed_admission_scenario(
        scenario.repository, scenario.clock.current, source="kraken_futures"
    )
    scenario.clock.current += timedelta(seconds=1)
    collision = await refresh_admission_account(legacy, {}, scenario.clock.current, 500)
    assert collision.authority_public_id == route.authority_public_id
    assert collision.authority_content_digest != legacy.route.authority_content_digest
    async with scenario.repository.session() as session:
        actual = await get_bound_route_authority(session, collision.authority_public_id)
        assert route_authority_content_digest(actual) == collision.authority_content_digest
    with pytest.raises(ValueError, match="missing_or_ambiguous"):
        await _require(scenario, route)


@pytest.mark.parametrize("paper", [False, True])
async def test_existing_spot_or_paper_route_loading_is_unchanged_and_not_routed_through_native_access(
    scenario: NativeAuthorityScenario, paper: bool
) -> None:
    """Keep the new Futures-only prerequisite from changing existing Spot route behavior.

    Given: A genuine existing Spot or Paper route selected through its unchanged snapshot loader.
    When: Legacy loading runs and the separate Futures-only prerequisite is called directly.
    Then: Existing loading still succeeds and the new prerequisite reports unsupported scope.
    """
    legacy = await seed_admission_scenario(
        scenario.repository, scenario.clock.current, source="kraken", paper=paper
    )
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, legacy.route.scope.wallet_public_id
        )
        assert (
            await require_current_route_authority(session, legacy.route, scenario.clock.current)
            == legacy.authority
        )
        with pytest.raises(
            access.NativeFuturesOrderAccessUnavailableError, match="requires_live_futures"
        ):
            await access.require_current_native_futures_order_access(
                session, legacy.route, scenario.policy
            )


async def test_caller_transaction_is_required_before_any_native_source_lookup() -> None:
    """Reject an absent transaction using only a public shape and an unbound session.

    Given: No database transaction or wallet fence exists.
    When: The new prerequisite is invoked with a shape-only route and trusted freshness configuration.
    Then: It refuses before any source query without constructing an unrelated native HTTP fixture.
    """
    from_route = unit_admission_payload().route
    async with AsyncSession() as session:
        with pytest.raises(RuntimeError, match="existing wallet-fenced transaction"):
            await access.require_current_native_futures_order_access(
                session,
                from_route,
                NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=1)),
            )


async def test_futures_paper_mode_copy_is_rejected_by_original_route_validation() -> None:
    """Preserve the core live-Futures mode contract before any actual current-source query.

    Given: An unchecked shape copy assigns Paper mode to a Futures production route.
    When: The order prerequisite revalidates it inside an otherwise active unbound transaction.
    Then: Original route validation refuses the inconsistent mode before any database lookup.
    """
    route = unit_admission_payload().route
    invalid = route.model_copy(update={"scope": route.scope.model_copy(update={"mode": "paper"})})
    policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=1))
    async with AsyncSession() as session, session.begin():
        with pytest.raises(ValidationError, match="Paper exchange, mode and environment"):
            await access.require_current_native_futures_order_access(session, invalid, policy)
