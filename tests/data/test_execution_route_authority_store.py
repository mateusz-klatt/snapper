"""Real account-route binding, freshness, credential selection and immutable history."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import route_selection_digest
from snapper.data.execution_route_authority_store import RouteAuthorityBindingError
from snapper.data.execution_route_authority_store import RouteAuthorityConflictError
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueAccountAttemptRow
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)


@pytest.fixture
async def scenario(tmp_path: Path) -> AsyncIterator[AdmissionScenario]:
    """Create actual isolated wallet, credential and successful account observations."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW)
    finally:
        await repository.engine.dispose()


async def test_current_route_and_exact_retry_preserve_one_actual_authority(
    scenario: AdmissionScenario,
) -> None:
    """Freshness is independently checked before content-idempotent identity reuse.

    Given: A genuinely bound selected route and successful current account snapshot,
    When: It is independently required and inserted again with new provenance,
    Then: The original immutable identity and complete payload remain unchanged.
    """
    async with scenario.repository.session() as session:
        proven = await require_current_route_authority(session, scenario.route, _NOW)
    retry = await insert_execution_route_authority(
        scenario.repository, scenario.authority, admission_stamp(194, _NOW)
    )
    assert retry.created is False
    assert retry.public_id == scenario.route.authority_public_id
    assert proven == scenario.authority


@pytest.mark.parametrize("field", ("authority_content_digest", "route_revision", "scope"))
async def test_current_reference_must_match_the_actual_selected_public_route(
    scenario: AdmissionScenario, field: str
) -> None:
    """A well-formed reference cannot substitute a different client/account selection.

    Given: One changed content digest, route revision or actual account scope,
    When: Current route authority is required,
    Then: The crossed reference is refused before financial use.
    """
    data = scenario.route.model_dump(mode="python")
    if field == "scope":
        data["scope"]["margin_account"] = "different-account"
    else:
        data[field] = "f" * 64
    reference = type(scenario.route).model_validate(data)
    async with scenario.repository.session() as session:
        with pytest.raises(RouteAuthorityBindingError, match="actual_execution_route_mismatch"):
            await require_current_route_authority(session, reference, _NOW)


async def test_expired_observation_is_historical_truth_without_current_permission(
    scenario: AdmissionScenario,
) -> None:
    """An immutable account record does not acquire unlimited freshness.

    Given: A retained actual route whose source window expired,
    When: It is read historically and required for a new financial operation,
    Then: Only the historical read succeeds.
    """
    async with scenario.repository.session() as session:
        original = await get_bound_route_authority(session, scenario.route.authority_public_id)
        with pytest.raises(RouteAuthorityBindingError, match="not_current"):
            await require_current_route_authority(
                session, scenario.route, _NOW + timedelta(minutes=3)
            )
    assert original == scenario.authority


async def test_current_authority_requires_an_aware_actual_clock(
    scenario: AdmissionScenario,
) -> None:
    """A naive wall-clock cannot establish an authenticated response's freshness.

    Given: An explicit actual account route,
    When: A caller supplies an ambiguous naive interpretation clock,
    Then: The current-authority assertion refuses it explicitly.
    """
    async with scenario.repository.session() as session:
        with pytest.raises(RouteAuthorityBindingError, match="clock_not_aware"):
            await require_current_route_authority(
                session, scenario.route, _NOW.replace(tzinfo=None)
            )


@pytest.mark.parametrize("is_paper", (None, True))
async def test_missing_or_reclassified_actual_wallet_refuses_current_route(
    scenario: AdmissionScenario, is_paper: bool | None
) -> None:
    """Credential-shaped metadata cannot override the actual wallet's live/Paper identity.

    Given: A closed wallet or a contradictory current Paper flag,
    When: Current authenticated authority is checked,
    Then: The missing/crossed wallet scope is refused.
    """
    async with scenario.repository.session() as session:
        wallet = (await session.execute(select(Wallet))).scalar_one()
        if is_paper is None:
            wallet.known_to = _NOW - timedelta(seconds=1)
        else:
            wallet.is_paper = is_paper
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(RouteAuthorityBindingError, match="route_wallet"):
            await require_current_route_authority(session, scenario.route, _NOW)


@pytest.mark.parametrize("change", ("missing", "revision", "mode"))
async def test_selected_public_credential_revision_is_required(
    scenario: AdmissionScenario, change: str
) -> None:
    """A credential lookup must prove the exact currently selected revision and execution mode.

    Given: Missing, changed or incorrectly typed current credential metadata,
    When: The prior actual route is required,
    Then: The public selection contradiction is held before financial use.
    """
    async with scenario.repository.session() as session:
        credential = (await session.execute(select(WalletCredential))).scalar_one()
        if change == "missing":
            credential.known_to = _NOW - timedelta(seconds=1)
        elif change == "revision":
            credential.timestamp = _NOW - timedelta(seconds=1)
        else:
            credential.credential_type = "paper"
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(RouteAuthorityBindingError, match="selected_credential"):
            await require_current_route_authority(session, scenario.route, _NOW)


@pytest.mark.parametrize("change", ("observation", "window", "state"))
async def test_real_source_observation_and_current_bounded_state_are_required(
    scenario: AdmissionScenario, change: str
) -> None:
    """Fresh labels cannot substitute an actual durable source and its current authority window.

    Given: An unknown observation, an excessive window or no current account state,
    When: A route authority is inserted,
    Then: Binding fails independently of its syntactically valid payload.
    """
    data = scenario.authority.model_dump(mode="python")
    if change == "observation":
        data["account_observation_public_id"] = admission_id(77)
    elif change == "window":
        data["valid_until"] = scenario.authority.valid_until + timedelta(seconds=1)
    else:
        async with scenario.repository.session() as session:
            state = (await session.execute(select(VenueAccountState))).scalar_one()
            state.known_to = _NOW - timedelta(seconds=1)
            await session.commit()
    payload = ExecutionRouteAuthorityPayload.model_validate(data)
    with pytest.raises(RouteAuthorityBindingError):
        await insert_execution_route_authority(
            scenario.repository, payload, admission_stamp(194, _NOW)
        )


async def test_source_error_retention_does_not_reauthorize_the_prior_fresh_content(
    scenario: AdmissionScenario,
) -> None:
    """Exact content retries still honor an independently degraded current source state.

    Given: An account's current state becomes an error while retaining old balances,
    When: The old immutable authority is inserted again,
    Then: Current proof refuses it while historical evidence remains available.
    """
    async with scenario.repository.session() as session:
        state = (await session.execute(select(VenueAccountState))).scalar_one()
        state.sync_status = "error"
        state.balance_status = "error"
        await session.commit()
    with pytest.raises(RouteAuthorityBindingError, match="current_account_evidence_changed"):
        await insert_execution_route_authority(
            scenario.repository, scenario.authority, admission_stamp(194, _NOW)
        )
    async with scenario.repository.session() as session:
        retained = await get_bound_route_authority(session, scenario.route.authority_public_id)
    assert retained == scenario.authority


async def test_public_identity_cannot_be_reused_for_different_fresh_evidence(
    scenario: AdmissionScenario,
) -> None:
    """Immutable authority identity is independent from its content digest.

    Given: Another valid observation envelope under a previously used public UUID,
    When: It is inserted under the same current account fence,
    Then: Identity conflict is explicit and the original remains unchanged.
    """
    data = scenario.authority.model_dump(mode="python")
    data["observed_at"] = _NOW + timedelta(seconds=1)
    changed = ExecutionRouteAuthorityPayload.model_validate(data)
    with pytest.raises(RouteAuthorityConflictError, match="public_identity_changed"):
        await insert_execution_route_authority(
            scenario.repository, changed, admission_stamp(93, _NOW + timedelta(seconds=1))
        )
    async with scenario.repository.session() as session:
        retained = await get_bound_route_authority(session, scenario.route.authority_public_id)
    assert retained == scenario.authority


async def test_a_declared_demo_environment_cannot_use_the_production_origin(
    scenario: AdmissionScenario,
) -> None:
    """Environment is the actual supported route, not a caller-controlled safety label.

    Given: A production Spot client relabeled as an unsupported demo route,
    When: Its selected-route hash is coherently recomputed,
    Then: The actual environment/origin guard still refuses it.
    """
    data = scenario.authority.model_dump(mode="python")
    scope = scenario.authority.scope.model_dump(mode="python")
    scope["environment"] = "demo"
    updated_scope = type(scenario.authority.scope).model_validate(scope)
    data["scope"] = updated_scope
    data["route_revision"] = route_selection_digest(
        updated_scope,
        scenario.authority.credential_public_id,
        scenario.authority.credential_timestamp,
        scenario.authority.public_api_origin,
    )
    changed = ExecutionRouteAuthorityPayload.model_validate(data)
    with pytest.raises(RouteAuthorityBindingError, match="api_environment_unproven"):
        await insert_execution_route_authority(
            scenario.repository, changed, admission_stamp(194, _NOW)
        )


async def test_missing_authority_is_not_invented_from_a_reference(
    scenario: AdmissionScenario,
) -> None:
    """A well-shaped UUID cannot manufacture retained account evidence.

    Given: No immutable authority exists for a supplied identity,
    When: Its historical payload is requested,
    Then: Absence is explicit without granting permission or fabricating balances.
    """
    async with scenario.repository.session() as session:
        with pytest.raises(RouteAuthorityBindingError, match="route_authority_missing"):
            await get_bound_route_authority(session, admission_id(77))
        assert (
            await session.execute(select(ExecutionRouteAuthority))
        ).scalar_one().public_id == scenario.route.authority_public_id


@pytest.mark.parametrize("current_credential", (True, False))
async def test_explicit_paper_credential_absence_matches_the_actual_selected_row(
    current_credential: bool,
    tmp_path: Path,
) -> None:
    """An uncredentialed Paper route is explicit and cannot hide an actually selected credential.

    Given: A new simulated account response after the current route selection,
    When: Credential absence is declared with or without a real current credential,
    Then: Only the genuinely uncredentialed local route receives authority.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    try:
        await repository.create_all()
        scenario = await seed_admission_scenario(repository, _NOW, paper=True)
        if not current_credential:
            async with repository.session() as session:
                credential = (await session.execute(select(WalletCredential))).scalar_one()
                credential.known_to = _NOW - timedelta(seconds=1)
                await session.commit()
        attempt: VenueAccountAttemptRow = {
            "wallet_public_id": admission_id(3),
            "exchange": "paper",
            "mode": "paper",
            "balance_status": "simulated",
            "position_status": "not_applicable",
            "valuation_status": "native_only",
            "balances_json": scenario.authority.balance_receipt_json,
            "open_positions_json": None,
            "balance_observed_at": _NOW,
            "position_observed_at": None,
            "authoritative_until": _NOW + timedelta(minutes=2),
            "error": None,
            "session_id": admission_id(99),
            "sequence_id": 194,
            "bus_time": _NOW,
        }
        state_id = await repository.record_venue_account_snapshot(attempt)
        async with repository.session() as session:
            state = (
                await session.execute(
                    select(VenueAccountState).where(VenueAccountState.id == state_id)
                )
            ).scalar_one()
            observation = (
                await session.execute(
                    select(VenueAccountObservation).where(
                        VenueAccountObservation.id == state.current_attempt_observation_id
                    )
                )
            ).scalar_one()
        data = scenario.authority.model_dump(mode="python")
        data.update(
            credential_public_id=None,
            credential_timestamp=None,
            account_observation_public_id=observation.public_id,
            account_observation_timestamp=observation.timestamp,
            balance_observed_at=_NOW,
            route_revision=route_selection_digest(scenario.authority.scope, None, None, None),
        )
        payload = ExecutionRouteAuthorityPayload.model_validate(data)
        if current_credential:
            with pytest.raises(RouteAuthorityBindingError, match="credential_identity_omitted"):
                await insert_execution_route_authority(
                    repository, payload, admission_stamp(194, _NOW)
                )
        else:
            result = await insert_execution_route_authority(
                repository, payload, admission_stamp(194, _NOW)
            )
            assert result.created is True
    finally:
        await repository.engine.dispose()
